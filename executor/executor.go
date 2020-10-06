package executor

import (
	"fmt"
	"github.com/Percona-Lab/go-tpcc/databases"
	"github.com/Percona-Lab/go-tpcc/tpcc/models"
	"time"
)

type Executor struct {
	batchSize int
	data map[string][]interface{}
	db *databases.Database
	retries int
	transaction bool
}

const DefaultRetries = 10

func NewExecutor(db *databases.Database, batchSize int) (*Executor, error) {
	return &Executor {
		batchSize: 512,
		data:      make(map[string][]interface{}),
		db:        db,
		retries:   DefaultRetries,
		transaction: false,
	}, nil
}

func (e *Executor) ChangeBatchSize(batchSize int) {
	e.batchSize = batchSize
}

func (e *Executor) ChangeRetries(r int) {
	e.retries = r
}

// @TODO@
// Error handling

func (e *Executor) SaveBatch(collectionName string, d interface{}) {
	e.data[collectionName] = append(e.data[collectionName], d)

	if len(e.data[collectionName]) % e.batchSize == 0 {
		e.db.InsertBatch(collectionName,e.data[collectionName])
		delete(e.data, collectionName)

	}
}

func (e *Executor) Flush(collectionName string) {
	e.db.InsertBatch(collectionName,e.data[collectionName])
	delete(e.data, collectionName)
}
//@TODO@
//error handling
func (e *Executor) Save(collectionName string, d interface{}) {
	e.db.InsertOne(collectionName, d)
}


func (e *Executor) DoTrxRetries(fn func() error) error {
	var err error

	retries := e.retries

	if ! e.transaction {
		retries = 1
	}


	for i := 0; i < retries; i++ {
		err = nil
		if e.transaction {
			err = e.db.StartTrx()
			if err != nil {
				return err
			}
		}

		err = fn()

		if err != nil {
			if e.transaction {
				e := e.db.RollbackTrx()
				if e != nil {
					return e
				}
			}
			break
		}

		if e.transaction {
			err := e.db.CommitTrx()
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (e *Executor) DoStockLevelTrx(warehouseId int, districtId int, threshold int) error {
	// Do Stock Level never requires a transactions

	noid, err := e.db.GetNextOrderId(warehouseId, districtId)
	if err != nil {
		return err
	}

	_, err = e.db.GetStockCount(noid, noid-20, threshold, warehouseId, districtId)

	if err != nil {
		return err
	}

	return nil
}


func (e *Executor) DoDeliveryTrx(wId int, oCarrierId int, olDeliveryD time.Time, dId int) error {
	return e.DoTrxRetries(func() error {
		return e.DoDelivery(wId, oCarrierId, olDeliveryD, dId)
	})
}

//todo the order of arguments here is weird
// also the dId passed from the worker is probably utterly wrong
func (e *Executor) DoDelivery(wId int, oCarrierId int, olDeliveryD time.Time, dId int) error {
	no, err := e.db.GetNewOrder(wId, dId)
	if err != nil {
		return err
	}

	err = e.db.DeleteNewOrder(no.NO_O_ID, wId, dId)
	if err != nil {
		return err
	}

	_, err = e.db.GetCustomerIdOrder(no.NO_O_ID, wId, dId)
	if err != nil {
		return err
	}

	err = e.db.UpdateOrders(no.NO_O_ID, wId, dId, oCarrierId, olDeliveryD)
	if err != nil {
		return err
	}

	return nil
}

func (e *Executor) DoOrderStatusTrx(warehouseId, districtId, cId int, cLast string) error {
	return e.DoTrxRetries(func() error {
		return e.DoOrderStatus(warehouseId, districtId, cId, cLast)
	})
}

func (e *Executor) DoOrderStatus(warehouseId, districtId, cId int, cLast string) error {

	var err error

	if cId > 0 {
		_, err = e.db.GetCustomerById(cId, warehouseId, districtId)
	} else {
		var customer *models.Customer
		customer, err = e.db.GetCustomerByName(cLast, warehouseId, districtId)
		if err != nil {
			return err
		}
		cId =  customer.C_ID
	}


	if err != nil {
		return err
	}

	order, err := e.db.GetLastOrder(cId, warehouseId, districtId)

	if err != nil {
		return err
	}

	_, err = e.db.GetOrderLines(order.O_ID, warehouseId, districtId)

	if err != nil {
		return err
	}

	return nil
}

func (e *Executor) DoPaymentTrx(warehouseId, districtId int,
	amount float64,
	cWId, cDId, cId int,
	cLast string,
	hDate time.Time,
	badCredit string,
	cdatalen int) error {
	return e.DoTrxRetries(func() error {
		return e.DoPayment(warehouseId, districtId,
			amount,
			cWId, cDId, cId,
			cLast,
			hDate,
			badCredit ,
			cdatalen)
	})
}

func (e *Executor) DoPayment(
	warehouseId, districtId int,
	amount float64,
	cWId, cDId, cId int,
	cLast string,
	hDate time.Time,
	badCredit string,
	cdatalen int,
) error {
	warehouse, err := e.db.GetWarehouse(warehouseId)

	if err != nil {
		return err
	}

	err = e.db.UpdateWarehouseBalance(warehouseId, amount)

	if err != nil {
		return err
	}

	district, err := e.db.GetDistrict(warehouseId, districtId)

	if err != nil {
		return err
	}

	err = e.db.UpdateDistrictBalance(warehouseId, districtId, amount)

	if err != nil {
		return err
	}
	var customer *models.Customer
	if cId > 0 {
		customer, err = e.db.GetCustomerById(cId, warehouseId, districtId)
	} else {
		customer, err = e.db.GetCustomerByName(cLast, warehouseId, districtId)
	}

	if err != nil {
		return err
	}

	if customer.C_CREDIT == badCredit {
		var buf string

		buf = fmt.Sprintf("%v %v %v %v %v %v|%v", cId, cDId, cWId, districtId, warehouseId, amount, customer.C_DATA)
		err = e.db.UpdateCredit(cId, warehouseId, districtId, amount, buf[:cdatalen])

		if err != nil {
			return err
		}

	} else {
		err = e.db.UpdateCredit(cId, warehouseId, districtId, amount, "")

		if err != nil {
			return err
		}
	}

	hData := fmt.Sprintf("%v    %v", warehouse.W_NAME, district.D_NAME)

	err = e.db.InsertHistory(warehouseId, districtId, time.Now(), amount, hData)

	if err != nil {
		return err
	}

	return nil
}

func (e *Executor) DoNewOrderTrx(wId, dId, cId int, oEntryD time.Time, iIds []int, iWids []int, iQtys []int) error {
	return e.DoTrxRetries(func() error {
		return e.DoNewOrder(wId, dId, cId, oEntryD, iIds, iWids, iQtys)
	})
}

func (e *Executor) DoNewOrder(wId, dId, cId int, oEntryD time.Time, iIds []int, iWids []int, iQtys []int) error {
	var err error

	_, err = e.db.GetWarehouse(wId)
	if err != nil {
		return err
	}

	district, err := e.db.GetDistrict(wId, dId)
	if err != nil {
		return err
	}

	err = e.db.IncrementDistrictOrderId(wId, dId)
	if err != nil {
		return err
	}

	_, err = e.db.GetCustomer(cId, wId, dId)
	if err != nil {
		return err
	}


	allLocal := 1
	for _, item := range iWids {
		if item != wId {
			allLocal = 0
			break
		}
	}

	items, err := e.db.GetItems(iIds)
	if err != nil {
		return err
	}

	if len(*items) != len(iIds) {
		return fmt.Errorf("TPCC defines 1%% of neworder gives a wrong itemid, causing rollback. This happens on purpose")
	}


	stocks, err := e.db.GetStockInfo(dId, iIds, iWids, allLocal)
	if err != nil {
		return err
	}

	if len(*stocks) != len(iIds) {
		return fmt.Errorf("len(stocks) != len(i_ids)")
	}

	var orderLines []models.OrderLine

	for i:=0; i < len(iIds); i++ {
		sQuantity := (*stocks)[i].S_QUANTITY


		if sQuantity >= 10 + iQtys[i] {
			sQuantity -=  iQtys[0]
		} else {
			sQuantity += 91 - iQtys[0]
		}

		if iWids[i] != wId {
			err = e.db.UpdateStock(
				(*stocks)[i].S_I_ID,
				iWids[1],
				sQuantity,
				(*stocks)[i].S_YTD + iQtys[i],
				(*stocks)[i].S_ORDER_CNT + 1,
				(*stocks)[i].S_REMOTE_CNT + 1,
			)

			if err != nil {
				return err
			}
		}

		orderLines = append(orderLines, models.OrderLine{
			OL_O_ID:        district.D_NEXT_O_ID,
			OL_NUMBER:      i+1,
			OL_I_ID:        iIds[i],
			OL_SUPPLY_W_ID: iWids[i],
			OL_DELIVERY_D:  oEntryD,
			OL_QUANTITY:    iQtys[i],
			OL_AMOUNT:      (*items)[i].I_PRICE*float64(iQtys[i]),
			OL_DIST_INFO:   distCol(dId, &(*stocks)[i]),
		})
	}

	err = e.db.CreateOrder(district.D_NEXT_O_ID, cId, wId, dId, 0, len(iIds), allLocal, oEntryD, orderLines)
	if err != nil {
		return err
	}

	return nil
}

func (e *Executor) CreateIndexes() error {
	return e.db.CreateIndexes()
}

func distCol(dId int, stock *models.Stock) string {
	switch dId {
	case 1:
		return stock.S_DIST_01
	case 2:
		return stock.S_DIST_02
	case 3:
		return stock.S_DIST_03
	case 4:
		return stock.S_DIST_04
	case 5:
		return stock.S_DIST_05
	case 6:
		return stock.S_DIST_06
	case 7:
		return stock.S_DIST_07
	case 8:
		return stock.S_DIST_08
	case 9:
		return stock.S_DIST_09
	default:
		return stock.S_DIST_10
	}
}
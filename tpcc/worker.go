package tpcc

import (
	"context"
	"github.com/Percona-Lab/go-tpcc/databases"
	"github.com/Percona-Lab/go-tpcc/executor"
	"github.com/Percona-Lab/go-tpcc/helpers"
	"sync"
	"fmt"
	"time"
)

type Configuration struct {
	DBDriver string
	URI string
	Transactions bool
	DBName string
	Threads int
	WriteConcern int
	ReadConcern int
	ReportInterval int
	WareHouses int
	ScaleFactor float64
	PercentFail int
}


type Worker struct {
	cfg *Configuration
	sc *ScaleParameters
	threadId  int
	ex *executor.Executor
	ctx context.Context
	wg *sync.WaitGroup
	c chan Transaction
	denormalized bool
}

func NewWorker(ctx context.Context, configuration *Configuration, wg *sync.WaitGroup, c chan Transaction, threadId int) (*Worker, error) {

	sc,_ := NewScaleParameters(
		configuration.ScaleFactor,
		NUM_ITEMS,
		configuration.WareHouses,
		DISTRICTS_PER_WAREHOUSE,
		CUSTOMERS_PER_DISTRICT,
		INITIAL_NEW_ORDERS_PER_DISTRICT,
	)

	den := false
	if configuration.DBDriver == "mongodb" {
		den = true
	}

	d, err := databases.NewDatabase(configuration.DBDriver, configuration.URI, configuration.DBName, "a", "b", configuration.Transactions, false)
	if err != nil {
		return nil, err
	}
	ex, err := executor.NewExecutor(d,256)
	if err != nil {
		return nil, err
	}

	w := &Worker {
		threadId:	threadId,
		cfg: configuration,
		sc: sc,
		ex: ex,
		ctx: ctx,
		wg: wg,
		c: c,
		denormalized: den,
	}

	return w, nil
}

type ScaleParameters struct {
	Items int
	Warehouses int
	DistrictsPerWarehouse int
	CustomersPerDistrict int
	NewOrdersPerDistrict int
}

func NewScaleParameters(
	scaleFactor float64,
	items int,
	warehouses int,
	districtsPerWarehouse int,
	customersPerDistrict int,
	newOrdersPerDistrict int,
) (*ScaleParameters, error) {
	s := &ScaleParameters{
		Items:                 int(float64(items) / scaleFactor),
		DistrictsPerWarehouse: districtsPerWarehouse,
		CustomersPerDistrict:  int(float64(customersPerDistrict)/scaleFactor),
		NewOrdersPerDistrict:  int(float64(newOrdersPerDistrict)/scaleFactor),
		Warehouses: warehouses,
	}

	return s, nil
}
type TransactionType int

const (
	StockLevelTrx = iota
	DeliveryTrx
	OrderStatusTrx
	PaymentTrx
	NewOrderTrx
)

type Transaction struct {
	ThreadId int
	Type TransactionType
	Failed bool
	Time float64
}

func (w *Worker) Execute() {
	defer w.wg.Done()
	for {
		select {
		case <- w.ctx.Done():
			return
		default:
			t := time.Now()
			var status error
			trx := Transaction{
				ThreadId: w.threadId,
			}
			switch r := helpers.RandInt(1, 100); {
			case r <= 4:
				trx.Type = StockLevelTrx
				status = w.DoStockLevelTrx()
			case r <= 8:
				trx.Type = DeliveryTrx
				status = w.DoDelivery()
			case r <= 12:
				trx.Type = OrderStatusTrx
				status = w.DoOrderStatus()
			case r <= 55:
				trx.Type = PaymentTrx
				status = w.DoPayment()
			default:
				trx.Type = NewOrderTrx
				status = w.DoNewOrder()
			}


			trx.Time = float64(time.Now().Sub(t).Nanoseconds())/1e6

			trx.Failed = false
			if status != nil {
				trx.Failed = true
			}

			w.c <- trx
		}
	}
}

func (w *Worker) DoStockLevelTrx() error {
	warehouseId := helpers.RandInt(1, w.sc.Warehouses)
	districtId := helpers.RandInt(1, w.sc.DistrictsPerWarehouse)
	threshold := helpers.RandInt(MIN_STOCK_LEVEL_THRESHOLD, MAX_STOCK_LEVEL_THRESHOLD)

	return w.ex.DoStockLevelTrx(warehouseId, districtId, threshold)
}

func (w *Worker) DoDelivery() error {
	warehouseId := helpers.RandInt(1, w.sc.Warehouses)
	OCarrierId := helpers.RandInt(MIN_CARRIER_ID, MAX_CARRIER_ID)
	OlDeliveryD := time.Now()

	return w.ex.DoDelivery(warehouseId, OCarrierId, OlDeliveryD, w.sc.DistrictsPerWarehouse)
}

func (w *Worker) DoOrderStatus() error {
	wId := helpers.RandInt(1, w.sc.Warehouses)
	dId := helpers.RandInt(1, w.sc.DistrictsPerWarehouse)
	cId := 0
	cLast := ""

	if helpers.RandInt(1,100) <= 60 {
		sylN := (helpers.RandInt(0, 256)|helpers.RandInt(0,1000)+helpers.RandInt(0,256))%1000

		cLast = SYLLABLES[sylN/100] +
			SYLLABLES[(sylN/10)%10] +
			SYLLABLES[sylN%10]

	} else {
		cId = helpers.RandInt(1, w.sc.CustomersPerDistrict)
	}

	return w.ex.DoOrderStatus(wId, dId, cId, cLast)
}

func (w *Worker) DoPayment() error {
	wId := helpers.RandInt(1, w.sc.Warehouses)
	dId := helpers.RandInt(1, w.sc.DistrictsPerWarehouse)
	cWId := 0
	cDId := 0
	cId := 0
	cLast := ""
	hAmount := helpers.RandFloat(MIN_PAYMENT, MAX_PAYMENT, MONEY_DECIMALS)
	hDate := time.Now()

	if w.sc.Warehouses == 1 || helpers.RandInt(1, 100) <= 85 {
		cWId = wId
		cDId = dId
	} else {
		cWId = helpers.RandIntExcluding(1, w.sc.Warehouses, wId)
		cDId = helpers.RandInt(1, w.sc.DistrictsPerWarehouse)
	}

	if helpers.RandInt(1, 100) <= 60 {
		sylN := (helpers.RandInt(0, 256)|helpers.RandInt(0,1000)+helpers.RandInt(0,256))%1000

		cLast = SYLLABLES[sylN/100] +
			SYLLABLES[(sylN/10)%10] +
			SYLLABLES[sylN%10]

	} else {
		cId = helpers.RandInt(1, w.sc.CustomersPerDistrict)
	}

	return w.ex.DoPayment(wId, dId, hAmount, cWId, cDId, cId, cLast, hDate, BAD_CREDIT, MAX_C_DATA)
}

func (w *Worker) DoNewOrder() error {
	wId := helpers.RandInt(1, w.sc.Warehouses)
	dId := helpers.RandInt(1, w.sc.DistrictsPerWarehouse)
	cId := helpers.RandInt(1, w.sc.CustomersPerDistrict)
	oEntryD := time.Now()
	olCnt := helpers.RandInt(MIN_OL_CNT, MAX_OL_CNT)

	rollback := false

	if helpers.RandInt(1,100) < w.cfg.PercentFail  {
		rollback = true
	}

	var iIds []int
	var iWIds []int
	var iQtys []int

	for i := 0; i < olCnt; i++ {
		if rollback && i+1 == olCnt {
			iIds = append(iIds, w.sc.Items + 1)
		} else {
			//todo
			//it should be generated by the non-uniform thing
			iIds = append(iIds, helpers.RandInt(1, w.sc.Items))
		}

		if w.sc.Warehouses > 1 && helpers.RandInt(1, 100) == 42  {
			iWIds = append(iWIds, helpers.RandIntExcluding(1, w.sc.Warehouses, wId))
		} else {
			iWIds = append(iWIds, wId)
		}

		iQtys = append(iQtys, helpers.RandInt(1, MAX_OL_QUANTITY))
	}

	return w.ex.DoNewOrder(wId, dId, cId, oEntryD, iIds, iWIds, iQtys)
}

func (w *Worker) CreateIndexes() error {
	return w.ex.CreateIndexes()
}

func (w *Worker) CreateSchema() error {
	return w.ex.CreateSchema()
}

package executor

import (
	"context"
	"github.com/Percona-Lab/go-tpcc/databases"
	"time"
)

type Executor struct {
	batchSize int
	data map[string][]interface{}
	db *databases.Database
	retries int
}

const DefaultRetries = 10

func NewExecutor(db *databases.Database, batchSize int) (*Executor, error) {
	return &Executor {
		batchSize: 512,
		data:      make(map[string][]interface{}),
		db:        db,
		retries:   DefaultRetries,
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


func (e *Executor) DoStockLevelTrx(wId int, dId int, threshold int) error {
	// Do Stock Level never requires a transactions

	return e.db.DoStockLevelTrx(context.Background(), wId, dId, threshold)
}

func (e *Executor) DoDelivery(wId int, oCarrierId int, olDeliveryD time.Time, dId int) error {
	var err error
	return e.db.DoTrxRetries( func(ctx context.Context) error {
		for d:=1; d < dId+ 1; d++ {
			err = e.db.DoDelivery(ctx, wId, oCarrierId, olDeliveryD, d)
			if err != nil {
				return err
			}
		}

		return nil
	}, e.retries)

}

func (e *Executor) DoOrderStatus(wId, dId, cId int, cLast string) error {
	return e.db.DoTrxRetries( func (ctx context.Context) error {
		return e.db.DoOrderStatus(ctx, wId, dId, cId, cLast)
	}, e.retries)
}

func (e *Executor) DoPayment(wId, dId int, hAmount float64, cWId, cDId, cId int, cLast string, hDate time.Time, badCredit string, cdatalen int) error {
	return e.db.DoTrxRetries( func (ctx context.Context) error {
		return e.db.DoPayment(ctx, wId, dId, hAmount, cWId, cDId, cId, cLast, hDate, badCredit, cdatalen)
	}, e.retries)

}

func (e *Executor) DoNewOrder(wId, dId, cId int, oEntryD time.Time, iIds []int, iWids []int, iQtys []int) error {
	return e.db.DoTrxRetries(func (ctx context.Context) error {
		return e.db.DoNewOrder(ctx, wId, dId, cId, oEntryD, iIds, iWids, iQtys)
	}, e.retries)
}

func (e *Executor) CreateIndexes() error {
	return e.db.CreateIndexes()
}


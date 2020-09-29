package tpcc

import (
	"mongo-tpcc/helpers"
	"time"
)

type Order struct {
	O_ID         int `bson:"O_ID"`
	O_C_ID       int `bson:"O_C_ID"`
	O_D_ID       int `bson:"O_D_ID"`
	O_W_ID       int `bson:"O_W_ID"`
	O_ENTRY_D    time.Time `bson:"O_ENTRY_D"`
	O_CARRIED_ID int `bson:"O_CARRIED_ID"`
	O_OL_CNT     int `bson:"O_OL_CNT"`
	O_ALL_LOCAL  int `bson:"O_ALL_LOCAL"`
	ORDER_LINE []OrderLine `bson:"ORDER_LINE,omitempty"`
}

type NewOrder struct {
	NO_O_ID int `bson:"NO_O_ID"`
	NO_D_ID int `bson:"NO_D_ID"`
	NO_W_ID int `bson:"NO_W_ID"`
}

type OrderLine struct {
	OL_O_ID        int `bson:"OL_O_ID"`
	OL_D_ID        int `bson:"OL_D_ID"`
	OL_W_ID        int `bson:"OL_W_ID"`
	OL_NUMBER      int `bson:"OL_NUMBER"`
	OL_I_ID        int `bson:"OL_I_ID"`
	OL_SUPPLY_W_ID int `bson:"OL_SUPPLY_W_ID"`
	OL_DELIVERY_D  time.Time `bson:"OL_DELIVERY_D"`
	OL_QUANTITY     int `bson:"OL_QUANTITY"`
	OL_AMOUNT      float64 `bson:"OL_AMOUNT"`
	OL_DIST_INFO   string `bson:"OL_DIST_INFO"`
}

func (w* Worker) generateOrder(oWId int, oDId int, oId int, oCId int, oOlCnt int, isNewOrder bool) Order {

	carrierId := NULL_CARRIER_ID
	if ! isNewOrder {
		carrierId = helpers.RandInt(MIN_CARRIER_ID, MAX_CARRIER_ID)
	}

	return Order{
		O_ID:         oId,
		O_C_ID:       oCId,
		O_D_ID:       oDId,
		O_W_ID:       oWId,
		O_ENTRY_D:    time.Now(),
		O_CARRIED_ID: carrierId,
		O_OL_CNT:     oOlCnt,
		O_ALL_LOCAL:  INITIAL_ALL_LOCAL,
	}
}

func (w* Worker) generateNewOrder(oWId int, oDId int, oId int) NewOrder {
	return NewOrder{
		NO_O_ID: oId,
		NO_D_ID: oDId,
		NO_W_ID: oWId,
	}
}

func (w* Worker) generateOrderLine(olWId int, olDId int, olOId int, olNumber int, maxItems int, isNewOrder bool) OrderLine {
	supplyId := olWId

	if w.sc.Warehouses > 1 && helpers.RandInt(1,100) == 1 {
		supplyId = helpers.RandIntExcluding(1,w.sc.Warehouses, supplyId)
	}

	t := time.Time{}
	if ! isNewOrder {
		t = time.Now()
	}

	return OrderLine{
		OL_O_ID:        olOId,
		OL_D_ID:        olDId,
		OL_W_ID:        olWId,
		OL_NUMBER:      olNumber,
		OL_I_ID:        helpers.RandInt(1, maxItems),
		OL_SUPPLY_W_ID: supplyId,
		OL_DELIVERY_D:  t,
		OL_QUANTITY:    INITIAL_QUANTITY,
		OL_AMOUNT:      helpers.RandFloat(MIN_AMOUNT,MAX_PRICE * MAX_OL_QUANTITY, MONEY_DECIMALS),
		OL_DIST_INFO:   helpers.RandString(DIST),
	}
}
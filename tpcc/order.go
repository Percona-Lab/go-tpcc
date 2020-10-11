package tpcc

import (
	"github.com/Percona-Lab/go-tpcc/helpers"
	"github.com/Percona-Lab/go-tpcc/tpcc/models"
	"time"
)


func (w* Worker) generateOrder(oWId int, oDId int, oId int, oCId int, oOlCnt int, isNewOrder bool) models.Order {

	carrierId := NULL_CARRIER_ID
	if ! isNewOrder {
		carrierId = helpers.RandInt(MIN_CARRIER_ID, MAX_CARRIER_ID)
	}

	return models.Order{
		O_ID:         oId,
		O_C_ID:       oCId,
		O_D_ID:       oDId,
		O_W_ID:       oWId,
		O_ENTRY_D:    time.Now(),
		O_CARRIER_ID: carrierId,
		O_OL_CNT:     oOlCnt,
		O_ALL_LOCAL:  INITIAL_ALL_LOCAL,
	}
}

func (w* Worker) generateNewOrder(oWId int, oDId int, oId int) models.NewOrder {
	return models.NewOrder{
		NO_O_ID: oId,
		NO_D_ID: oDId,
		NO_W_ID: oWId,
	}
}

func (w* Worker) generateOrderLine(olWId int, olDId int, olOId int, olNumber int, maxItems int, isNewOrder bool) models.OrderLine {
	supplyId := olWId

	if w.sc.Warehouses > 1 && helpers.RandInt(1,100) == 1 {
		supplyId = helpers.RandIntExcluding(1,w.sc.Warehouses, supplyId)
	}

	t := time.Time{}
	if ! isNewOrder {
		t = time.Now()
	}

	return models.OrderLine{
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
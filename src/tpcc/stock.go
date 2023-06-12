package tpcc

import (
	"github.com/slocke716/go-tpcc/helpers"
	"github.com/slocke716/go-tpcc/tpcc/models"
)

func (w *Worker) generateStock(sWId int, sIId int, isOriginal bool) models.Stock {

	data := helpers.RandString(helpers.RandInt(MIN_I_DATA, MAX_I_DATA))

	if isOriginal {
		data = helpers.RandOriginal(data, ORIGINAL_STRING)
	}

	return models.Stock{
		S_I_ID:       sIId,
		S_W_ID:       sWId,
		S_QUANTITY:   helpers.RandInt(MIN_QUANTITY, MAX_QUANTITY),
		S_DIST_01:    helpers.RandString(DIST),
		S_DIST_02:    helpers.RandString(DIST),
		S_DIST_03:    helpers.RandString(DIST),
		S_DIST_04:    helpers.RandString(DIST),
		S_DIST_05:    helpers.RandString(DIST),
		S_DIST_06:    helpers.RandString(DIST),
		S_DIST_07:    helpers.RandString(DIST),
		S_DIST_08:    helpers.RandString(DIST),
		S_DIST_09:    helpers.RandString(DIST),
		S_DIST_10:    helpers.RandString(DIST),
		S_YTD:        0,
		S_ORDER_CNT:  0,
		S_REMOTE_CNT: 0,
		S_DATA:       data,
	}
}

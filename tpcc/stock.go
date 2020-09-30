package tpcc

import "github.com/Percona-Lab/go-tpcc/helpers"

type Stock struct {
	S_I_ID int `bson:"S_I_ID"`
	S_W_ID int `bson:"S_W_ID"`
	S_QUANTITY int `bson:"S_QUANTITY"`
	S_DIST_01 string `bson:"S_DIST_01"`
	S_DIST_02 string `bson:"S_DIST_02"`
	S_DIST_03 string `bson:"S_DIST_03"`
	S_DIST_04 string `bson:"S_DIST_04"`
	S_DIST_05 string `bson:"S_DIST_05"`
	S_DIST_06 string `bson:"S_DIST_06"`
	S_DIST_07 string `bson:"S_DIST_07"`
	S_DIST_08 string `bson:"S_DIST_08"`
	S_DIST_09 string `bson:"S_DIST_09"`
	S_DIST_10 string `bson:"S_DIST_10"`
	S_YTD int `bson:"S_YTD"`
	S_ORDER_CNT int `bson:"S_ORDER_CNT"`
	S_REMOTE_CNT int `bson:"S_REMOTE_CNT"`
	S_DATA string `bson:"S_DATA"`
}

func (w *Worker) generateStock(sWId int, sIId int, isOriginal bool) Stock {

	data := helpers.RandString(helpers.RandInt(MIN_I_DATA, MAX_I_DATA))

	if isOriginal {
		data = helpers.RandOriginal(data, ORIGINAL_STRING)
	}

	return Stock{
		S_I_ID:     sIId,
		S_W_ID:     sWId,
		S_QUANTITY: helpers.RandInt(MIN_QUANTITY, MAX_QUANTITY),
		S_DIST_01:  helpers.RandString(DIST),
		S_DIST_02:  helpers.RandString(DIST),
		S_DIST_03:  helpers.RandString(DIST),
		S_DIST_04:  helpers.RandString(DIST),
		S_DIST_05:  helpers.RandString(DIST),
		S_DIST_06:  helpers.RandString(DIST),
		S_DIST_07:  helpers.RandString(DIST),
		S_DIST_08:  helpers.RandString(DIST),
		S_DIST_09:  helpers.RandString(DIST),
		S_DIST_10:    helpers.RandString(DIST),
		S_YTD:        0,
		S_ORDER_CNT:  0,
		S_REMOTE_CNT: 0,
		S_DATA:       data,
	}
}
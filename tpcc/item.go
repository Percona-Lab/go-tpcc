package tpcc

import (
	"github.com/Percona-Lab/go-tpcc/helpers"
	"github.com/Percona-Lab/go-tpcc/tpcc/models"
)


func (w *Worker) LoadItems() {
	originalRows := helpers.SelectUniqueIds(int(w.sc.Items/10), 1, w.sc.Items)

	for i:=1; i < w.sc.Items+1; i++ {
		isOriginalRow := false
		for _, item := range originalRows {
			if item == i {
				isOriginalRow = true
				break
			}
		}
		w.ex.SaveBatch(TABLENAME_ITEM, w.GenerateItem(i, isOriginalRow))
	}
	w.ex.Flush(TABLENAME_ITEM)
}
func (w *Worker) GenerateItem(id int, isOriginalRow bool) models.Item {

	var iData = helpers.RandString(helpers.RandInt(MIN_I_DATA, MAX_I_DATA))
	if isOriginalRow {
		iData = helpers.RandOriginal(
			iData,
			ORIGINAL_STRING,
		)
	}
	return models.Item{
		I_ID:    id,
		I_IM_ID: helpers.RandInt(MIN_IM, MAX_IM),
		I_NAME:  helpers.RandString(helpers.RandInt(MIN_I_NAME, MAX_I_NAME)),
		I_PRICE: helpers.RandFloat(MIN_PRICE, MAX_PRICE, MONEY_DECIMALS),
		I_DATA:  iData,
	}
}

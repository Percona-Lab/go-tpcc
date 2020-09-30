package tpcc

import(
	"github.com/Percona-Lab/go-tpcc/helpers"
)

type District struct {
	D_ID        int `bson:"D_ID"`
	D_W_ID      int  `bson:"D_W_ID"`
	D_NAME      string `bson:"D_NAME"`
	D_STREET_1  string `bson:"D_STREET_1"`
	D_STREET_2  string `bson:"D_STREET_2"`
	D_CITY      string `bson:"D_CITY"`
	D_STATE     string `bson:"D_STATE"`
	D_ZIP       string `bson:"D_ZIP"`
	D_TAX       float64 `bson:"D_TAX"`
	D_YTD       float64 `bson:"D_YTD"`
	D_NEXT_O_ID int `bson:"D_NEXT_O_ID"`
}

func (w *Worker) generateDistrict(dId int, dWId int, dNextOId int) District {

	address_ := w.generateRandomAddress()

	return District{
		D_ID:        dId,
		D_W_ID:      dWId,
		D_NAME:      helpers.RandString(helpers.RandInt(MIN_NAME, MAX_NAME)),
		D_STREET_1:  address_.street_1,
		D_STREET_2:  address_.street_2,
		D_CITY:      address_.city,
		D_STATE:     address_.state,
		D_ZIP:       address_.zip,
		D_TAX:       helpers.RandFloat(MIN_TAX, MAX_TAX, TAX_DECIMALS),
		D_YTD:       INITIAL_W_YTD,
		D_NEXT_O_ID: dNextOId,
	}
}
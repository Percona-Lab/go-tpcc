package tpcc

import (
	"github.com/slocke716/go-tpcc/helpers"
	"github.com/slocke716/go-tpcc/tpcc/models"
)

func (w *Worker) generateDistrict(dId int, dWId int, dNextOId int) models.District {

	address_ := w.generateRandomAddress()

	return models.District{
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

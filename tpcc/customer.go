package tpcc

import (
	"github.com/Percona-Lab/go-tpcc/helpers"
	"github.com/Percona-Lab/go-tpcc/tpcc/models"
	"time"
)

func (w* Worker) generateCustomer(cId int, cWId int, cDId int, isBadCredit bool) models.Customer {

	var lastName string

	sylN := 0
	if cId < 1000 {
		sylN = cId - 1
	} else {

		//
		// @TODO@
		//
		// the last +rand.Intn(256) hsould be constnat
		// https://github.com/pingcap/go-tpc/blob/6eb40da50d66c776540184607442214bfcd824dd/tpcc/rand.go#L114
		sylN = (helpers.RandInt(0, 256)|helpers.RandInt(0,1000)+helpers.RandInt(0,256))%1000
	}
	lastName = SYLLABLES[sylN/100] +
		SYLLABLES[(sylN/10)%10] +
		SYLLABLES[sylN%10]

	address_ := w.generateRandomAddress()

	credit := GOOD_CREDIT
	if isBadCredit {
		credit = BAD_CREDIT
	}

	return models.Customer{
		C_ID:       cId,
		C_D_ID:     cDId,
		C_W_ID:     cWId,
		C_FIRST:    helpers.RandString(helpers.RandInt(MIN_FIRST, MAX_FIRST)),
		C_MIDDLE:   MIDDLE,
		C_LAST:     lastName,
		C_STREET_1: address_.street_1,
		C_STREET_2: address_.street_2,
		C_CITY:     address_.city,
		C_STATE:    address_.state,
		C_ZIP:      address_.zip,
		C_PHONE:    helpers.RandNumericString(PHONE),
		C_SINCE:    time.Now(),
		C_CREDIT:       credit,
		C_CREDIT_LIM:   INITIAL_CREDIT_LIM,
		C_DISCOUNT:     helpers.RandFloat(MIN_DISCOUNT, MAX_DISCOUNT, DISCOUNT_DECIMALS),
		C_BALANCE:      INITIAL_BALANCE,
		C_YTD_PAYMENT:  INITIAL_YTD_PAYMENT,
		C_PAYMENT_CNT:  INITIAL_PAYMENT_CNT,
		C_DELIVERY_CNT: INITIAL_DELIVERY_CNT,
		C_DATA:         helpers.RandString(helpers.RandInt(MIN_C_DATA, MAX_C_DATA)),
	}
}
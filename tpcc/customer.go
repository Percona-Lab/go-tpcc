package tpcc

import (
	"mongo-tpcc/helpers"
	"time"
)
//@TODO@
// Może warto wywalić je do Struct address i potem jakoś merge-ować, ale nie wiem

type Customer struct {
	C_ID           int `bson:"C_ID"`
	C_D_ID         int `bson:"C_D_ID"`
	C_W_ID         int `bson:"C_W_ID"`
	C_FIRST        string `bson:"C_FIRST"`
	C_MIDDLE       string `bson:"C_MIDDLE"`
	C_LAST         string `bson:"C_LAST"`
	C_STREET_1     string `bson:"C_STREET_1"`
	C_STREET_2     string `bson:"C_STREET_2"`
	C_CITY         string `bson:"C_CITY"`
	C_STATE        string `bson:"C_STATE"`
	C_ZIP          string `bson:"C_ZIP"`
	C_PHONE        string `bson:"C_PHONE"`
	C_SINCE        time.Time `bson:"C_SINCE"`
	C_CREDIT       string `bson:"C_CREDIT"`
	C_CREDIT_LIM   float64 `bson:"C_CREDIT_LIM"`
	C_DISCOUNT     float64 `bson:"C_DISCOUNT"`
	C_BALANCE      float64 `bson:"C_BALANCE"`
	C_YTD_PAYMENT  float64 `bson:"C_YTD_PAYMENT"`
	C_PAYMENT_CNT  int `bson:"C_PAYMENT_CNT"`
	C_DELIVERY_CNT int `bson:"C_DELIVERY_CNT"`
	C_DATA         string `bson:"C_DATA"`
}

func (w* Worker) generateCustomer(cId int, cWId int, cDId int, isBadCredit bool) Customer {

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
	lastName = SYLLABES[sylN/100] +
		SYLLABES[(sylN/10)%10] +
		SYLLABES[sylN%10]

	address_ := w.generateRandomAddress()

	credit := GOOD_CREDIT
	if isBadCredit {
		credit = BAD_CREDIT
	}

	return Customer{
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
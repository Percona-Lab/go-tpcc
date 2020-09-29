package tpcc

import (
	"math/rand"
	"mongo-tpcc/helpers"
	"time"
)

type Warehouse struct {
	W_ID       int `bson:"W_ID"`
	W_NAME     string `bson:"W_NAME"`
	W_STREET_1 string `bson:"W_STREET_1"`
	W_STREET_2 string `bson:"W_STREET_2"`
	W_CITY     string `bson:"W_CITY"`
	W_STATE    string `bson:"W_STATE"`
	W_ZIP      string `bson:"W_ZIP"`
	W_TAX      float64 `bson:"W_TAX"`
	W_YTD      float64 `bson:"W_YTD"`
}

func (w *Worker) GenerateWarehouse(id int) Warehouse {

	address_ := w.generateRandomAddress()
	return Warehouse{
		W_ID:       id,
		W_NAME:     helpers.RandString(helpers.RandInt(MIN_NAME, MAX_NAME)),
		W_STREET_1: address_.street_1,
		W_STREET_2: address_.street_2,
		W_CITY:     address_.city,
		W_STATE:    address_.state,
		W_ZIP:      address_.zip,
		W_TAX:      helpers.RandFloat(MIN_TAX, MAX_TAX, TAX_DECIMALS),
		W_YTD:      INITIAL_W_YTD,
	}
}

func (w *Worker) LoadWarehouse(id int) {
	warehouse := w.GenerateWarehouse(id)
	w.ex.Save(TABLENAME_WAREHOUSE, warehouse)

	for i := 1; i<w.sc.DistrictsPerWarehouse+1; i++ {
		district := w.generateDistrict(i, id, w.sc.CustomersPerDistrict+1)
		w.ex.Save(TABLENAME_DISTRICT, district)
		badCredits := helpers.SelectUniqueIds(w.sc.CustomersPerDistrict/10, 1, w.sc.CustomersPerDistrict)

		var customersId []int

		for c := 1; c < w.sc.CustomersPerDistrict+1; c++ {
			isBadCredit := false

			//@TODO@


			for _, item := range badCredits {
				if item == i {
					isBadCredit = true
					break
				}
			}

			customersId = append(customersId, c)
			w.ex.SaveBatch(TABLENAME_CUSTOMER, w.generateCustomer(c, id, i, isBadCredit))
			w.ex.SaveBatch(TABLENAME_HISTORY, w.generateHistory(id, i, c))
		}

		w.ex.Flush(TABLENAME_CUSTOMER)
		w.ex.Flush(TABLENAME_HISTORY)

		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(customersId), func(i, j int) { customersId[i], customersId[j] = customersId[j], customersId[i] })
		for c := 1; c < w.sc.CustomersPerDistrict+1; c++ {
			orderCount := helpers.RandInt(MIN_OL_CNT, MAX_OL_CNT)

			isNewOrder := false
			if w.sc.CustomersPerDistrict - w.sc.NewOrdersPerDistrict < c {
				isNewOrder = true
				w.ex.SaveBatch(TABLENAME_NEW_ORDER, w.generateNewOrder(id, i, c))
			}

			order := w.generateOrder(id, i, c, customersId[c-1], orderCount, isNewOrder)

			for o := 0; o < orderCount; o++ {
				//@TODO@
				//For other databases it should be probably a different table
				//ex.SaveBatch("orderLine", w.generateOrderLine(id, i, c, o, w.sc.Items, isNewOrder))
				//orderLines = append(orderLines, )
				order.ORDER_LINE = append(order.ORDER_LINE, w.generateOrderLine(id, i, c, o, w.sc.Items, isNewOrder))
			}

			w.ex.SaveBatch(TABLENAME_ORDERS, order)

		}

		w.ex.Flush(TABLENAME_ORDERS)
		w.ex.Flush(TABLENAME_NEW_ORDER)
	}

	originalStocks := helpers.SelectUniqueIds(w.sc.Items/10, 1, w.sc.Items)

	for i := 1; i < w.sc.Items+1; i++ {
		isOriginal := false
		for _, item := range originalStocks {
			if item == i {
				isOriginal = true
				break
			}
		}

		w.ex.SaveBatch(TABLENAME_STOCK, w.generateStock(id, i, isOriginal))
	}

	w.ex.Flush(TABLENAME_STOCK)
}
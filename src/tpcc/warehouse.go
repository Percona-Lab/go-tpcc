package tpcc

import (
	"github.com/slocke716/go-tpcc/helpers"
	"github.com/slocke716/go-tpcc/tpcc/models"
	"math/rand"
	"time"
)

func (w *Worker) GenerateWarehouse(id int) models.Warehouse {

	address_ := w.generateRandomAddress()
	return models.Warehouse{
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

func (w *Worker) LoadWarehouse(id int) error {
	var err error
	warehouse := w.GenerateWarehouse(id)
	err = w.ex.Save(TABLENAME_WAREHOUSE, warehouse)
	if err != nil {
		return err
	}

	for i := 1; i <= w.sc.DistrictsPerWarehouse+1; i++ {
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
			err = w.ex.SaveBatch(TABLENAME_CUSTOMER, w.generateCustomer(c, id, i, isBadCredit))
			if err != nil {
				return err
			}

			err = w.ex.SaveBatch(TABLENAME_HISTORY, w.generateHistory(id, i, c))
			if err != nil {
				return err
			}
		}

		err = w.ex.Flush(TABLENAME_CUSTOMER)
		if err != nil {
			return err
		}
		err = w.ex.Flush(TABLENAME_HISTORY)
		if err != nil {
			return err
		}

		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(customersId), func(i, j int) { customersId[i], customersId[j] = customersId[j], customersId[i] })
		for c := 1; c < w.sc.CustomersPerDistrict+1; c++ {
			orderCount := helpers.RandInt(MIN_OL_CNT, MAX_OL_CNT)

			isNewOrder := false
			if w.sc.CustomersPerDistrict-w.sc.NewOrdersPerDistrict < c {
				isNewOrder = true
				err = w.ex.SaveBatch(TABLENAME_NEW_ORDER, w.generateNewOrder(id, i, c))
				if err != nil {
					return err
				}
			}

			order := w.generateOrder(id, i, c, customersId[c-1], orderCount, isNewOrder)
			if w.denormalized {
				for o := 0; o < orderCount; o++ {
					//@TODO@
					//For other databases it should be probably a different table
					//ex.SaveBatch("orderLine", w.generateOrderLine(id, i, c, o, w.sc.Items, isNewOrder))
					//orderLines = append(orderLines, )
					order.ORDER_LINE = append(order.ORDER_LINE, w.generateOrderLine(id, i, c, o, w.sc.Items, isNewOrder))
				}
				err = w.ex.SaveBatch(TABLENAME_ORDERS, order)
				if err != nil {
					return err
				}
			} else {
				err = w.ex.SaveBatch(TABLENAME_ORDERS, order)
				if err != nil {
					return err
				}
				for o := 0; o < orderCount; o++ {
					err = w.ex.SaveBatch(TABLENAME_ORDER_LINE, w.generateOrderLine(id, i, c, o, w.sc.Items, isNewOrder))
					if err != nil {
						return err
					}
				}
				err = w.ex.Flush(TABLENAME_ORDER_LINE)
				if err != nil {
					return err
				}
			}

		}

		err = w.ex.Flush(TABLENAME_ORDERS)
		if err != nil {
			return err
		}
		err = w.ex.Flush(TABLENAME_NEW_ORDER)
		if err != nil {
			return err
		}
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

		err = w.ex.SaveBatch(TABLENAME_STOCK, w.generateStock(id, i, isOriginal))
		if err != nil {
			return err
		}
	}

	err = w.ex.Flush(TABLENAME_STOCK)
	if err != nil {
		return err
	}

	return nil
}

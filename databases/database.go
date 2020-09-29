package databases

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx"
	"time"
)

type Database struct {
	Client *mongo.Client
	C *mongo.Database
	Aggregate bool
	findAndModify bool
	transactions bool
}

func NewDb(uri string, dbname string, transactions bool, findandmodify bool) (*Database, error){
	//mongodb://localhost:27017
	//todo auth

	client, err := mongo.NewClient(options.Client().ApplyURI(uri))

	if err != nil {
		return nil, err
	}

	err = client.Connect(context.Background())

	if err != nil {
		return nil, err
	}

	err = client.Ping(context.TODO(), nil)

	if err != nil {
		return nil, err
	}

	return &Database{
		Client: client,
		C: client.Database(dbname),
		Aggregate: false,
		transactions: transactions,
		findAndModify: findandmodify,
	}, nil
}

func (db *Database) CreateIndexes() error {
	ascending := bsonx.Int32(1)
	descending := bsonx.Int32(-1)

	_, err := db.C.Collection("ITEM").Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys: bsonx.Doc{
				{"W_ID", ascending},
				{"W_TAX", ascending},
			},
		},
	})

	if err != nil {
		return err
	}

	_, err = db.C.Collection("WAREHOUSE").Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys: bsonx.Doc{
				{"I_W_ID", ascending},
				{"I_ID", ascending},
			},
		},
	})

	if err != nil {
		return err
	}

	_, err = db.C.Collection("DISTRICT").Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys: bsonx.Doc{
				{"D_W_ID", ascending},
				{"D_ID", ascending},
				{"D_NEXT_O_ID", ascending},
				{"D_TAX", ascending},
			},
		},
	})

	if err != nil {
		return err
	}

	_, err = db.C.Collection("CUSTOMER").Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys: bsonx.Doc{
				{"C_W_ID", ascending},
				{"C_D_ID", ascending},
				{"C_ID", ascending},
			},
		},
		{
			Keys: bsonx.Doc {
				{"C_W_ID", ascending},
				{"C_D_ID", ascending},
				{"C_LAST", ascending},
			},
		},
	})

	if err != nil {
		return err
	}

	_, err = db.C.Collection("STOCK").Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys: bsonx.Doc{
				{"S_W_ID", ascending},
				{"S_I_ID", ascending},
				{"S_QUANTITY", ascending},
			},
		},
		{
			Keys: bsonx.Doc{
				{"S_I_ID", ascending},
			},
		},
	})

	if err != nil {
		return err
	}

	_, err = db.C.Collection("ORDERS").Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys: bsonx.Doc{
				{"O_W_ID", ascending},
				{"O_D_ID", ascending},
				{"O_ID", ascending},
				{"O_C_ID", ascending},
			},
		},
		{
			Keys: bsonx.Doc{
				{"O_C_ID", ascending},
				{"O_D_ID", ascending},
				{"O_W_ID", ascending},
				{"O_ID", descending},
				{"O_CARRIER_ID", ascending},
				{"O_ENTRY_ID", ascending},
			},
		},
	})

	if err != nil {
		return err
	}


	_, err = db.C.Collection("NEW_ORDER").Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys: bsonx.Doc{
				{"NO_W_ID", ascending},
				{"NO_D_ID", ascending},
				{"NO_O_ID", ascending},
			},
		},
	})

	if err != nil {
		return err
	}

	_, err = db.C.Collection("ORDER_LINE").Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys: bsonx.Doc{
				{"OL_O_ID", ascending},
				{"OL_D_ID", ascending},
				{"OL_W_ID", ascending},
				{"OL_NUMBER", ascending},
			},
		},
		{
			Keys: bsonx.Doc{
				{"OL_O_ID", ascending},
				{"OL_D_ID", ascending},
				{"OL_W_ID", ascending},
				{"OL_I_ID", descending},
				{"OL_AMOUNT", ascending},

			},
		},
	})

	if err != nil {
		return err
	}

	return nil
}

func (db *Database) InsertOne(collectionName string, d interface{}) error {
	collection := db.C.Collection(collectionName)
	_, err := collection.InsertOne(context.TODO(), d)
	if err != nil {
		return err
	}

	return nil
}

func (db *Database) InsertBatch(collectionName string, d []interface{}) error {
	collection := db.C.Collection(collectionName)
	_, err := collection.InsertMany(context.TODO(), d)
	if err != nil {
		return err
	}

	return nil
}


func (db *Database) DoTrxRetries(fn func(ctx context.Context) error , retries int) error {
	s,err := db.Client.StartSession(nil)
	if err != nil {
		return err
	}

	if ! db.transactions {
		retries = 1
	}

	for i := 0; i < retries; i++ {
		if db.transactions {
			err = s.StartTransaction()
			if err != nil {
				return err
			}
		}

		err = mongo.WithSession(context.Background(), s, func(sc mongo.SessionContext) error {
			e := fn(sc)

			if e != nil {
				if db.transactions {
					s.AbortTransaction(sc)
				}
				return e
			}

			if db.transactions {
				s.CommitTransaction(sc)
			}

			return nil
		})

		if err == nil {
			break
		}
	}
	return err
}


func (db *Database) DoStockLevelTrx(ctx context.Context, wId int, dId int, threshold int) error {

	collection := db.C.Collection("DISTRICT")
	var p bson.M

	var query = &bson.D{
			{"D_W_ID", wId,},
			{"D_ID", dId,},
	}

	err := collection.FindOne(
		ctx,
		query,
		options.FindOne().SetProjection(bson.D{
			{"_id", 0},
			{"D_NEXT_O_ID", 1},
		}).SetComment("STOCK_LEVEL")).Decode(&p)

	if err != nil {
		return err
	}

	orderId := int(p["D_NEXT_O_ID"].(int32))
	collection = db.C.Collection("ORDERS")

	cursor, err := collection.Find(ctx,
		bson.D{
			{"O_W_ID", wId},
			{"O_D_ID", dId},
			{"O_ID", bson.D{
				{"$lt", orderId},
				{"$gte", orderId - 20},
			}},
		}, options.Find().SetProjection(bson.D{
			{"ORDER_LINE.OL_I_ID", 1},
		}).SetComment("STOCK_LEVEL"))

	if err != nil {
		return err
	}

	defer cursor.Close(ctx)
	var orderIds []int32

	for cursor.Next(ctx) {
		var order bson.M
		if err = cursor.Decode(&order); err != nil {
			return err
		}

		for _, value := range order["ORDER_LINE"].(primitive.A) {
			orderIds = append(orderIds, value.(primitive.M)["OL_I_ID"].(int32))
		}
	}

	_, err = db.C.Collection("STOCK").CountDocuments(ctx, bson.D{
		{"S_W_ID", wId},
		{"S_I_ID", bson.D{
			{"$in", orderIds},
		}},
		{"S_QUANTITY", bson.D {
			{"$lt", threshold},
		}},
	})

	if err != nil {
		return err
	}

	return nil
}
func (db *Database) DoDelivery(ctx context.Context, wId int, oCarrierId int, olDeliveryD time.Time, dId int) error {
	var no bson.M
	newOrder := bson.D{
		{"NO_D_ID", dId},
		{"NO_W_ID", wId},
	}

	newOrderProjection := bson.D{
		{"_id", 0},
		{"NO_D_ID", 1},
		{"NO_W_ID", 1},
		{"NO_O_ID", 1},
	}

	newOrderSort := bson.D{{"NO_O_ID", 1}}
	var err error

	if db.findAndModify {
		err = db.C.Collection("NEW_ORDER").FindOneAndDelete(
			ctx,
			newOrder,
			options.FindOneAndDelete().SetSort(newOrderSort).SetProjection(newOrderProjection),
		).Decode(&no)

		if err != nil {
			return err
		}
	} else {
		err = db.C.Collection("NEW_ORDER").FindOne(
			ctx,
			newOrder,
			options.FindOne().SetProjection(newOrderProjection).SetSort(newOrderSort),
		).Decode(&no)

		if err != nil {
			return err
		}
	}

	if len(no) == 0 {
		return fmt.Errorf("No new order found")
	}

	orderId := int(no["NO_O_ID"].(int32))

	orderQuery := bson.D{
		{"O_ID", orderId},
		{"O_D_ID", dId},
		{"O_W_ID", wId},
	}

	var order bson.M

	if db.findAndModify {
		err = db.C.Collection("ORDERS").FindOneAndUpdate(ctx, orderQuery,
			bson.D{
				{"$set", bson.D{
					{"O_CARRIER_ID", oCarrierId},
					{"ORDER_LINE.$[].OL_DELIVERY_D", olDeliveryD},
				}},
			},
			nil,
			).Decode(&order)

		if err != nil {
			return err
		}
	} else {
		err = db.C.Collection("ORDERS").FindOne(ctx, orderQuery).Decode(&order)

		if err != nil {
			return err
		}
	}

	customerId := order["O_C_ID"]

	//if denormalize
	orderLineTotal := float64(0)
	for _, v := range order["ORDER_LINE"].(primitive.A) {
		orderLineTotal += v.(primitive.M)["OL_AMOUNT"].(float64)
	}

	if ! db.findAndModify {
		_, err = db.C.Collection("ORDERS").UpdateOne(ctx,
			bson.D {
				{"_id", order["_id"]},
			},
			bson.D{
				{"$set", bson.D{
					{"O_CARRIER_ID", oCarrierId},
					{"ORDER_LINE.$[].OL_DELIVERY_D", olDeliveryD},
				}},
			},
			nil,
			)

		if err != nil {
			return err
		}
	}

	_, err = db.C.Collection("CUSTOMER").UpdateOne(ctx,
		bson.D{
			{"C_ID", customerId},
			{"C_D_ID", dId},
			{"C_W_ID", wId},
		},
		bson.D{
			{"$inc", bson.D{
				{"C_BALANCE", orderLineTotal},
			}},
		},
		nil,
		)

	if err != nil {
		return err
	}

	if ! db.findAndModify {
		_,err = db.C.Collection("NEW_ORDER").DeleteOne(ctx, no)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *Database) DoOrderStatus(ctx context.Context, wId int, dId int, cId int, cLast string) error {

	var customer bson.M
	var err error

	if cId > 0 {
		err = db.C.Collection("CUSTOMER").FindOne(ctx, bson.D{
			{"C_W_ID", wId},
			{"C_D_ID", dId},
			{"C_ID", cId},
		}, options.FindOne().SetComment("ORDER_STATUS").SetProjection(bson.D{
			{"_id", 0},
			{"C_ID", 1},
			{"C_FIRST", 1},
			{"C_MIDDLE", 1},
			{"C_LAST", 1},
			{"C_BALANCE", 1},
		})).Decode(&customer)

		if err != nil {
			return err
		}

		if len(customer) == 0 {
			return fmt.Errorf("No customer found")
		}
	} else {
		cursor, err := db.C.Collection("CUSTOMER").Find(ctx, bson.D{
			{"C_W_ID", wId},
			{"C_D_ID", dId},
			{"C_LAST", cLast},
		}, options.Find().SetComment("ORDER_STATUS").SetProjection(bson.D{
			{"_id", 0},
			{"C_ID", 1},
			{"C_FIRST", 1},
			{"C_MIDDLE", 1},
			{"C_LAST", 1},
			{"C_BALANCE", 1},
		}))

		if err != nil {
			return err
		}

		var customers []bson.M
		err = cursor.All(ctx, &customers)


		if err != nil {
			return err
		}
		if len(customers) == 0 {
			return fmt.Errorf("No customer found")
		}

		i_ := int((len(customers) - 1) / 2)
		customer = customers[i_]
		cId = int(customer["C_ID"].(int32))
	}

	_, err = db.C.Collection("ORDERS").Find(ctx, bson.D{
		{"O_W_ID", wId},
		{"O_D_ID", dId},
		{"O_C_ID", cId},
	},
	options.Find().SetProjection(bson.D{
		{"O_ID", 1},
		{"O_CARRIER_ID", 1},
		{"O_ENTRY_D", 1},
		{"ORDER_LINE", 1},
	}).SetComment("ORDER_STATUS"))

	if err != nil {
		return err
	}

	return nil
}

func (db *Database) DoPayment(ctx context.Context, wId int, dId int, hAmount float64, cWId int,
	cDId int, cId int, cLast string, hDate time.Time, badCredit string, cDataLen int) error {

	districtProjection := bson.D{
		{"D_NAME", 1},
		{"D_STREET_1", 1},
		{"D_STREET_2", 1},
		{"D_CITY", 1},
		{"D_STATE", 1},
		{"D_ZIP", 1},
	}

	var district bson.M
	var err error

	if db.findAndModify {
		err = db.C.Collection("DISTRICT").FindOneAndUpdate(ctx, bson.D{
			{"D_ID", dId},
			{"D_W_ID", wId},
		},
		bson.D{
			{"$inc", bson.D{
				{"D_YTD", hAmount},
			}},
		},
		options.FindOneAndUpdate().SetProjection(districtProjection),
		).Decode(&district)

		if err != nil {
			return err
		}

	} else {
		err = db.C.Collection("DISTRICT").FindOne(ctx, bson.D{
			{"D_W_ID", wId},
			{"D_ID", dId},
		}, options.FindOne().SetComment("PAYMENT").SetProjection(districtProjection)).Decode(&district)

		if err != nil {
			return err
		}

		_, err = db.C.Collection("DISTRICT").UpdateOne(ctx, bson.D{
			{"_id", district["_id"]},
		}, bson.D{
			{"$inc", bson.D{
				{"D_YTD", hAmount},
			}},
		}, nil)

		if err != nil {
			return err
		}
	}

	warehouseProjection := bson.D{
		{"W_NAME", 1},
		{"W_STREET_1", 1},
		{"W_STREET_2", 1},
		{"W_CITY", 1},
		{"W_STATE", 1},
		{"W_ZIP", 1},
	}

	var warehouse bson.M

	if db.findAndModify {
		err = db.C.Collection("WAREHOUSE").FindOneAndUpdate(ctx,
			bson.D{
				{"W_ID", wId},
			},
			bson.D{
				{"$inc", bson.D{
					{"W_YTD", hAmount},
				}},
			},
			options.FindOneAndUpdate().SetProjection(warehouseProjection),
			).Decode(&warehouse)

		if err != nil {
			return err
		}

	} else {
		err = db.C.Collection("WAREHOUSE").FindOne(ctx, bson.D{
			{"W_ID", wId},
		},
		options.FindOne().SetProjection(warehouseProjection),
		).Decode(&warehouse)

		if err != nil {
			return err
		}

		_, err = db.C.Collection("WAREHOUSE").UpdateOne(ctx, bson.D{
			{"_id", warehouse["_id"]},
		},
		bson.D{
			{"$inc", bson.D{
				{"W_YTD", hAmount},
			}},
		},
		)

		if err != nil {
			return err
		}
	}

	var customer bson.M

	if cId > 0 {
		err = db.C.Collection("CUSTOMER").FindOne(ctx, bson.D{
			{"C_W_ID", wId},
			{"C_D_ID", dId},
			{"C_ID", cId},
		}, options.FindOne().SetProjection(bson.D{
			{"C_BALANCE", 0},
			{"C_YTD_PAYMENT", 0},
			{"C_PAYMENT_CNT", 0},
		})).Decode(&customer)

		if err != nil {
			return err
		}
	} else {

		cursor, err := db.C.Collection("CUSTOMER").Find(ctx, bson.D{
			{"C_W_ID", wId},
			{"C_D_ID", dId},
			{"C_LAST", cLast},
		}, options.Find().SetProjection(bson.D{
			{"C_BALANCE", 0},
			{"C_YTD_PAYMENT", 0},
			{"C_PAYMENT_CNT", 0},
		}))

		if err != nil {
			return err
		}

		var customers []bson.M
		err = cursor.All(ctx, &customers)

		if err != nil {
			return err
		}

		if len(customers) == 0 {
			return fmt.Errorf("No customers found")
		}

		i_ := int((len(customers) - 1) / 2)
		customer = customers[i_]
		cId = int(customer["C_ID"].(int32))
	}

	if cId == 0 {
		return fmt.Errorf("Customer not found")
	}

	upd := bson.D{
		{"$inc", bson.D{
			{"C_BALANCE", -1* hAmount},
			{"C_YTD_PAYMENT", hAmount},
			{"C_PAYMENT_CNT", 1},
		}},
	}

	var buf string
	if customer["C_CREDIT"] == badCredit {
		buf = fmt.Sprintf("%v %v %v %v %v %v|%v", cId, cDId, cWId, dId, wId, hAmount, customer["C_DATA"])
		if len(buf) > cDataLen {
			upd = append(upd, bson.E{"$set", bson.D{
				{"C_DATA", buf[:cDataLen]},
			}})
		}
	}

	_, err = db.C.Collection("CUSTOMER").UpdateOne(ctx,
		bson.D{
			{"_id", customer["_id"]},
		},
		upd, nil)

	if err != nil {
		return err
	}

	hData := fmt.Sprintf("%v    %v", warehouse["W_NAME"], district["D_NAME"])

	_, err = db.C.Collection("HISTORY").InsertOne(ctx, bson.D{
		{"H_D_ID", dId},
		{"H_W_ID", wId},
		{"H_DATE", hDate},
		{"H_AMOUNT", hAmount},
		{"H_DATA", hData},
	})

	if err != nil {
		return err
	}

	return nil
}


func (db *Database) DoNewOrder(
	ctx context.Context,
	w_id int,
	d_id int,
	c_id int,
	o_entry_d time.Time,
	i_ids []int,
	i_w_ids []int,
	i_qtys []int,
) error {

	districtProjection := bson.D{
		{"_id", 0},
		{"D_ID", 1},
		{"D_W_ID", 1},
		{"D_TAX", 1},
		{"D_NEXT_O_ID", 1},
	}

	var district bson.M
	var err error
	if db.findAndModify {
		err = db.C.Collection("DISTRICT").FindOneAndUpdate(ctx,
			bson.D{
			{"D_ID", d_id},
			{"D_W_ID", w_id},
			},
			bson.D{
			{"$inc", bson.D{
				{"D_NEXT_O_ID", 1},
			}},
			},
			options.FindOneAndUpdate().SetProjection(districtProjection).SetSort(bson.D{
				{"NO_O_ID", 1},
			}),
			).Decode(&district)

		if err != nil {
			return err
		}
	} else {
		err = db.C.Collection("DISTRICT").FindOne(ctx, bson.D{
			{"D_ID", d_id},
			{"D_W_ID", w_id},
		}, options.FindOne().SetProjection(districtProjection).SetSort(bson.D{
			{"NO_O_ID", 1},
		})).Decode(&district)

		if err != nil {
			return err
		}

		_, err = db.C.Collection("DISTRICT").UpdateOne(ctx, district,
			bson.D{
				{"$inc", bson.D{
					{"D_NEXT_O_ID", 1},
				}}},
				nil,
			)

		if err != nil {
			return err
		}
	}

	//@TODO shards
	// In PyTPCC they shard with i_w_id key, and here it needs to be implemented. Same for the loader
	cursor, err := db.C.Collection("ITEM").Find(ctx, bson.D{
		{"I_ID", bson.D{
			{"$in", i_ids},
		}}},
		options.Find().SetProjection(bson.D{
			{"_id", 0},
			{"I_ID", 1},
			{"I_PRICE", 1},
			{"I_NAME", 1},
			{"I_DATA", 1},
		}),
	)


	if err != nil {
		return err
	}


	var items []bson.M

	err = cursor.All(ctx, &items)

	if err != nil {
		return err
	}


	if len(items) != len(i_ids) {
		return fmt.Errorf("TPCC defines 1% of neworder gives a wrong itemid, causing rollback. This happens on purpose")
	}

	//@TODO@
	// shall items be sorted?

	var warehouse bson.M

	err = db.C.Collection("WAREHOUSE").FindOne(ctx,
		bson.D{
			{"W_ID", w_id},
		},
		options.FindOne().SetProjection(bson.D{
		{"_id", 0},
		{"W_TAX", 1},
		})).Decode(&warehouse)


	if err != nil {
		return err
	}

	var customer bson.M

	err = db.C.Collection("CUSTOMER").FindOne(ctx,
		bson.D{
			{"C_ID", c_id},
			{"C_D_ID", d_id},
			{"C_W_ID", w_id},
		}, options.FindOne().SetProjection(bson.D{
		{"C_DISCOUNT", 1},
		{"C_LAST", 1},
		{"C_CREDIT", 1},
		})).Decode(&customer)

	if err != nil {
		return err
	}
	_, err = db.C.Collection("NEW_ORDER").InsertOne(ctx,
		bson.D{
			{"NO_O_ID", district["D_NEXT_O_ID"]},
			{"NO_D_ID", d_id},
			{"NO_W_ID", w_id},
		})

	if err != nil {
		return err
	}


	allLocal := 1
	for _, item := range i_w_ids {
		if item != w_id {
			allLocal = 0
			break
		}
	}

	order := bson.D{
		{"O_ID", district["D_NEXT_O_ID"]},
		{"O_ENTRY_D", o_entry_d},
		{"O_CARRIER_ID", 0}, //@todo@ it should be constant
		{"O_OL_CNT", len(i_ids)},
		{"O_ALL_LOCAL", allLocal},
		{"O_D_ID", d_id},
		{"O_W_ID", w_id},
		{"O_C_ID", c_id},

	}

	distCol := fmt.Sprintf("S_DIST_%02d", d_id)
	stockProjection := bson.D{
		{"_id", 0},
		{"S_I_ID", 1},
		{"S_W_ID", 1},
		{"S_QUANTITY", 1},
		{"S_DATA", 1},
		{"S_YTD", 1},
		{"S_ORDER_CNT", 1},
		{"S_REMOTE_CNT", 1},
		{distCol, 1},
	}


	if allLocal == 1 {
		cursor,err =  db.C.Collection("STOCK").Find(ctx,
			bson.D{
				{"S_I_ID", bson.D{
					{"$in", i_ids},
				}},
				{"S_W_ID", w_id},
			}, options.Find().SetProjection(stockProjection))

		if err != nil {
			return err
		}
	} else {
		var searchList []bson.D
		for item, value := range i_ids {
			searchList = append(searchList, bson.D{
				{"S_I_ID", value},
				{"S_W_ID", i_w_ids[item]},
			})
		}
		cursor, err = db.C.Collection("STOCK").Find(ctx,
		bson.D{
			{"$or", searchList},

		},options.Find().SetProjection(stockProjection))

		if err != nil {
			 return err
		}
	}

	var stocks []bson.M

	//todo
	//sorting

	err = cursor.All(ctx, &stocks)
	if err != nil {
		return err
	}

	if len(stocks) != len(i_ids) {
		return fmt.Errorf("len(stocks) != len(i_ids)")
	}

	for i:=0; i < len(i_ids); i++ {

		sQuantity := int(stocks[i]["S_QUANTITY"].(int32))

		if sQuantity >= 10 + i_qtys[i] {
			sQuantity -=  i_qtys[0]
		} else {
			sQuantity += 91 - i_qtys[0]
		}

		if i_w_ids[i] != w_id {

			_, err := db.C.Collection("STOCK").UpdateOne(
				ctx,
				stocks[i],
				bson.D{
				{"$set", bson.D{
					{"S_QUANTITY", sQuantity},
					{"S_YTD", int(stocks[i]["S_YTD"].(int32)) + i_qtys[i]},
					{"S_ORDER_CNT", 1 + int(stocks[i]["S_ORDER_CNT"].(int32))},
					{"S_REMOTE_CNT", 1 + int(stocks[i]["S_REMOTE_CNT"].(int32))},
				}},
			},
			)

			if err != nil {
				return err
			}
		}

		order = append(order, bson.E{
			"ORDER_LINE", bson.D{
				{"OL_O_ID", district["D_NEXT_O_ID"]},
				{"OL_NUMBER", i + 1 },
				{"OL_I_ID", i_ids[i]},
				{"OL_SUPPLY_W_ID", i_w_ids[i]},
				{"OL_DELIVERY_D", o_entry_d},
				{"OL_QUANTITY", i_qtys[i]},
				{"OL_AMOUNT", float64(i_qtys[i]) * items[0]["I_PRICE"].(float64)},
				{"OL_DIST_INFO", stocks[i][distCol].(string)},
			},
		})
	}

	_, err = db.C.Collection("ORDER").InsertOne(ctx, order)
	if err != nil {
		return err
	}

	return nil
}
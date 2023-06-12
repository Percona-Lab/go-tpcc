package mongodb

import (
	"context"
	"fmt"
	"github.com/slocke716/go-tpcc/tpcc/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx"
	"time"
)

type MongoDB struct {
	Client        *mongo.Client
	C             *mongo.Database
	Aggregate     bool
	findAndModify bool
	transactions  bool
	ctx           mongo.SessionContext
}

func NewMongoDb(uri string, dbname string, transactions bool, findandmodify bool) (*MongoDB, error) {
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

	session, err := client.StartSession()

	if err != nil {
		return nil, err
	}

	return &MongoDB{
		Client:        client,
		C:             client.Database(dbname),
		Aggregate:     false,
		transactions:  transactions,
		findAndModify: findandmodify,
		ctx:           mongo.NewSessionContext(context.Background(), session),
	}, nil
}

func (db *MongoDB) CreateSchema() error {
	return nil
}

func (db *MongoDB) StartTrx() error {
	sess := mongo.SessionFromContext(db.ctx)
	err := sess.StartTransaction()
	if err != nil {
		return err
	}

	return nil
}

func (db *MongoDB) CommitTrx() error {
	sess := mongo.SessionFromContext(db.ctx)
	err := sess.CommitTransaction(db.ctx)
	if err != nil {
		return err
	}

	return nil
}

func (db *MongoDB) RollbackTrx() error {
	sess := mongo.SessionFromContext(db.ctx)
	err := sess.AbortTransaction(db.ctx)
	if err != nil {
		return err
	}

	return nil
}

func (db *MongoDB) CreateIndexes() error {
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
			Keys: bsonx.Doc{
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

func (db *MongoDB) InsertOne(tableName string, d interface{}) error {
	collection := db.C.Collection(tableName)
	_, err := collection.InsertOne(db.ctx, d)
	if err != nil {
		return err
	}

	return nil
}

func (db *MongoDB) InsertBatch(tableName string, d []interface{}) error {
	collection := db.C.Collection(tableName)
	_, err := collection.InsertMany(db.ctx, d)
	if err != nil {
		return err
	}

	return nil
}

// Get District using warehouseId and districtId and return pointer to models.District or error instead.
func (db *MongoDB) IncrementDistrictOrderId(warehouseId int, districtId int) error {
	filter := bson.D{
		{"D_ID", districtId},
		{"D_W_ID", warehouseId},
	}

	update := bson.D{
		{"$inc", bson.D{
			{"D_NEXT_O_ID", 1},
		}},
	}

	u, err := db.C.Collection("DISTRICT").UpdateOne(db.ctx, filter, update, nil)

	if err != nil {
		return err
	}

	if u.MatchedCount == 0 {
		return fmt.Errorf("update of District document failed")
	}

	return nil
}

// It also deletes new order, as MongoDB can do that findAndModify is set to 0
func (db *MongoDB) GetNewOrder(warehouseId int, districtId int) (*models.NewOrder, error) {
	var NewOrder models.NewOrder
	var err error

	newOrderProjection := bson.D{
		{"_id", 0},
		{"NO_D_ID", 1},
		{"NO_W_ID", 1},
		{"NO_O_ID", 1},
	}

	filter := bson.D{
		{"NO_D_ID", districtId},
		{"NO_W_ID", warehouseId},
	}

	newOrderSort := bson.D{{"NO_O_ID", 1}}

	if db.findAndModify {
		err = db.C.Collection("NEW_ORDER").FindOneAndDelete(db.ctx, filter, options.FindOneAndDelete().SetSort(newOrderSort).SetProjection(newOrderProjection)).Decode(&NewOrder)

		if err != nil {
			return nil, err
		}
	} else {
		err = db.C.Collection("NEW_ORDER").FindOne(db.ctx, filter, options.FindOne().SetProjection(newOrderProjection).SetSort(newOrderSort)).Decode(&NewOrder)
	}

	return &NewOrder, nil
}

func (db *MongoDB) DeleteNewOrder(orderId int, warehouseId int, districtId int) error {
	var err error

	filter := bson.D{
		{"NO_O_ID", orderId},
		{"NO_D_ID", districtId},
		{"NO_W_ID", warehouseId},
	}

	if db.findAndModify {
		return nil
	}

	r, err := db.C.Collection("NEW_ORDER").DeleteOne(db.ctx, filter, nil)

	if err != nil {
		return err
	}

	if r.DeletedCount == 0 {
		return fmt.Errorf("no documents found")
	}

	return nil
}

func (db *MongoDB) GetCustomer(customerId int, warehouseId int, districtId int) (*models.Customer, error) {
	var err error

	var c models.Customer

	err = db.C.Collection("CUSTOMER").FindOne(db.ctx, bson.D{
		{"C_ID", customerId},
		{"C_D_ID", districtId},
		{"C_W_ID", warehouseId},
	}).Decode(&c)

	if err != nil {
		return nil, err
	}

	return &c, nil
}

// GetCId
func (db *MongoDB) GetCustomerIdOrder(orderId int, warehouseId int, districtId int) (int, error) {
	var err error

	filter := bson.D{
		{"O_ID", orderId},
		{"O_D_ID", districtId},
		{"O_W_ID", warehouseId},
	}

	var doc bson.M
	err = db.C.Collection("ORDERS").FindOne(db.ctx, filter, options.FindOne().SetProjection(bson.D{
		{"_id", 0},
		{"O_C_ID", 1},
	})).Decode(&doc)

	if err != nil {
		return 0, err
	}

	return int(doc["O_C_ID"].(int32)), nil
}

func (db *MongoDB) UpdateOrders(orderId int, warehouseId int, districtId int, oCarrierId int, deliveryDate time.Time) error {
	var err error

	filter := bson.D{
		{"O_ID", orderId},
		{"O_D_ID", districtId},
		{"O_W_ID", warehouseId},
	}

	r, err := db.C.Collection("ORDERS").UpdateOne(db.ctx, filter, bson.D{
		{"$set", bson.D{
			{"O_CARRIER_ID", oCarrierId},
			{"ORDER_LINE.$[].OL_DELIVERY_D", deliveryDate},
		}},
	})

	if err != nil {
		return err
	}

	if r.MatchedCount == 0 {
		return fmt.Errorf("UpdateOrders: no documents matched")
	}

	return nil
}

func (db *MongoDB) SumOLAmount(orderId int, warehouseId int, districtId int) (float64, error) {
	var err error

	match := bson.D{
		{"$match", bson.D{
			{"O_ID", orderId},
			{"O_D_ID", districtId},
			{"O_W_ID", warehouseId},
		}},
	}

	unwind := bson.D{
		{"$unwind", "$ORDER_LINE"},
	}

	group := bson.D{
		{"$group", bson.D{
			{"_id", "OL_O_ID"},
			{"sumOlAmount", bson.D{
				{"$sum", "$ORDER_LINE.OL_AMOUNT"},
			}},
		}},
	}

	cursor, err := db.C.Collection("ORDERS").Aggregate(db.ctx, mongo.Pipeline{match, unwind, group})
	defer cursor.Close(db.ctx)
	if err != nil {
		return 0, err
	}

	cursor.Next(db.ctx)

	var agg bson.M
	err = cursor.Decode(&agg)
	if err != nil {
		return 0, err
	}

	return float64(agg["sumOlAmount"].(float64)), nil

}

func (db *MongoDB) UpdateCustomer(customerId int, warehouseId int, districtId int, sumOlTotal float64) error {
	var err error

	r, err := db.C.Collection("CUSTOMER").UpdateOne(db.ctx, bson.D{
		{"C_ID", customerId},
		{"C_D_ID", districtId},
		{"C_W_ID", warehouseId},
	}, bson.D{
		{"$inc", bson.D{
			{"C_BALANCE", sumOlTotal},
		}},
	}, nil)

	if err != nil {
		return err
	}

	if r.MatchedCount == 0 {
		return fmt.Errorf("no matched documents")
	}

	return nil
}

func (db *MongoDB) GetNextOrderId(warehouseId int, districtId int) (int, error) {

	var oid bson.M
	var query = &bson.D{
		{"D_W_ID", warehouseId},
		{"D_ID", districtId},
	}

	err := db.C.Collection("DISTRICT").FindOne(db.ctx, query, options.FindOne().SetProjection(bson.D{
		{"_id", 0},
		{"D_NEXT_O_ID", 1},
	}).SetComment("STOCK_LEVEL")).Decode(&oid)

	if err != nil {
		return 0, err
	}

	return int(oid["D_NEXT_O_ID"].(int32)), nil
}

func (db *MongoDB) GetStockCount(orderIdLt int, orderIdGt int, threshold int, warehouseId int, districtId int) (int64, error) {

	cursor, err := db.C.Collection("ORDERS").Find(db.ctx, bson.D{
		{"O_W_ID", warehouseId},
		{"O_D_ID", districtId},
		{"O_ID", bson.D{
			{"$lt", orderIdLt},
			{"$gte", orderIdGt},
		}},
	}, options.Find().SetProjection(bson.D{
		{"ORDER_LINE.OL_I_ID", 1},
	}).SetComment("STOCK_LEVEL"))

	if err != nil {
		return 0, err
	}

	defer cursor.Close(db.ctx)
	var orderIds []int32

	for cursor.Next(db.ctx) {
		var order bson.M
		if err = cursor.Decode(&order); err != nil {
			return 0, err
		}

		for _, value := range order["ORDER_LINE"].(primitive.A) {
			orderIds = append(orderIds, value.(primitive.M)["OL_I_ID"].(int32))
		}
	}

	c, err := db.C.Collection("STOCK").CountDocuments(db.ctx, bson.D{
		{"S_W_ID", warehouseId},
		{"S_I_ID", bson.D{
			{"$in", orderIds},
		}},
		{"S_QUANTITY", bson.D{
			{"$lt", threshold},
		}},
	})

	if err != nil {
		return 0, err
	}

	return c, nil
}

func (db *MongoDB) GetCustomerById(customerId int, warehouseId int, districtId int) (*models.Customer, error) {

	var err error
	var customer models.Customer

	projection := bson.D{
		{"_id", 0},
		{"C_ID", 1},
		{"C_FIRST", 1},
		{"C_MIDDLE", 1},
		{"C_LAST", 1},
		{"C_BALANCE", 1},
	}

	err = db.C.Collection("CUSTOMER").FindOne(db.ctx, bson.D{
		{"C_W_ID", warehouseId},
		{"C_D_ID", districtId},
		{"C_ID", customerId},
	}, options.FindOne().SetComment("ORDER_STATUS").SetProjection(projection)).Decode(&customer)

	if err != nil {
		return nil, err
	}

	return &customer, nil
}

func (db *MongoDB) GetCustomerByName(name string, warehouseId int, districtId int) (*models.Customer, error) {

	var customer models.Customer

	projection := bson.D{
		{"_id", 0},
		{"C_ID", 1},
		{"C_FIRST", 1},
		{"C_MIDDLE", 1},
		{"C_LAST", 1},
		{"C_BALANCE", 1},
	}

	cursor, err := db.C.Collection("CUSTOMER").Find(db.ctx, bson.D{
		{"C_W_ID", warehouseId},
		{"C_D_ID", districtId},
		{"C_LAST", name},
	}, options.Find().SetProjection(projection))

	defer cursor.Close(db.ctx)

	if err != nil {
		return nil, err
	}

	var customers []models.Customer
	err = cursor.All(db.ctx, &customers)

	if err != nil {
		return nil, err
	}
	if len(customers) == 0 {
		return nil, fmt.Errorf("No customer found")
	}

	i_ := int((len(customers) - 1) / 2)

	customer = customers[i_]

	return &customer, nil
}

func (db *MongoDB) GetLastOrder(customerId int, warehouseId int, districtId int) (*models.Order, error) {
	var err error
	var order models.Order

	projection := bson.D{
		{"O_ID", 1},
		{"O_CARRIER_ID", 1},
		{"O_ENTRY_D", 1},
	}

	sort := bson.D{{"O_ID", 1}}

	err = db.C.Collection("ORDERS").FindOne(db.ctx, bson.D{
		{"O_W_ID", warehouseId},
		{"O_D_ID", districtId},
		{"O_C_ID", customerId},
	}, options.FindOne().SetProjection(projection).SetSort(sort)).Decode(&order)

	if err != nil {
		return nil, err
	}

	return &order, nil
}

func (db *MongoDB) GetOrderLines(orderId int, warehouseId int, districtId int) (*[]models.OrderLine, error) {
	var err error
	var order models.Order

	projection := bson.D{
		{"ORDER_LINE", 1},
	}

	err = db.C.Collection("ORDERS").FindOne(db.ctx, bson.D{
		{"O_W_ID", warehouseId},
		{"O_D_ID", districtId},
		{"O_ID", orderId},
	}, options.FindOne().SetProjection(projection)).Decode(&order)

	if err != nil {
		return nil, err
	}

	return &order.ORDER_LINE, nil
}

func (db *MongoDB) GetWarehouse(warehouseId int) (*models.Warehouse, error) {

	var err error

	warehouseProjection := bson.D{
		{"W_NAME", 1},
		{"W_STREET_1", 1},
		{"W_STREET_2", 1},
		{"W_CITY", 1},
		{"W_STATE", 1},
		{"W_ZIP", 1},
	}

	var warehouse models.Warehouse

	err = db.C.Collection("WAREHOUSE").FindOne(db.ctx, bson.D{
		{"W_ID", warehouseId},
	}, options.FindOne().SetProjection(warehouseProjection)).Decode(&warehouse)

	if err != nil {
		return nil, err
	}

	return &warehouse, nil
}

func (db *MongoDB) UpdateWarehouseBalance(warehouseId int, amount float64) error {

	r, err := db.C.Collection("WAREHOUSE").UpdateOne(db.ctx, bson.D{
		{"W_ID", warehouseId},
	}, bson.D{
		{"$inc", bson.D{
			{"W_YTD", amount},
		}},
	})

	if err != nil {
		return err
	}

	if r.MatchedCount == 0 {
		return fmt.Errorf("no warehouse found")
	}

	return nil
}

func (db *MongoDB) GetDistrict(warehouseId int, districtId int) (*models.District, error) {
	var err error

	var district models.District

	err = db.C.Collection("DISTRICT").FindOne(db.ctx, bson.D{
		{"D_ID", districtId},
		{"D_W_ID", warehouseId},
	}).Decode(&district)

	if err != nil {
		return nil, err
	}

	return &district, nil
}

func (db *MongoDB) UpdateDistrictBalance(warehouseId int, districtId int, amount float64) error {
	filter := bson.D{
		{"D_ID", districtId},
		{"D_W_ID", warehouseId},
	}

	update := bson.D{
		{"$inc", bson.D{
			{"D_YTD", amount},
		}},
	}

	r, err := db.C.Collection("DISTRICT").UpdateOne(db.ctx, filter, update, nil)

	if r.MatchedCount == 0 {
		return fmt.Errorf("No district found")
	}

	if err != nil {
		return err
	}

	return nil
}

func (db *MongoDB) InsertHistory(warehouseId int, districtId int, date time.Time, amount float64, data string) error {

	_, err := db.C.Collection("HISTORY").InsertOne(db.ctx, bson.D{
		{"H_D_ID", districtId},
		{"H_W_ID", warehouseId},
		{"H_C_W_ID", warehouseId},
		{"H_C_D_ID", districtId},
		{"H_DATE", date},
		{"H_AMOUNT", amount},
		{"H_DATA", date},
	})

	return err
}

func (db *MongoDB) UpdateCredit(customerId int, warehouseId int, districtId int, balance float64, data string) error {
	//updateBCCustomer
	var err error
	update := bson.D{
		{"$inc", bson.D{
			{"C_BALANCE", -1 * balance},
			{"C_YTD_PAYMENT", balance},
			{"C_PAYMENT_CNT", 1},
		}},
	}

	if len(data) > 0 {
		update = append(update, bson.E{"$set", bson.D{
			{"C_DATA", data},
		}})
	}

	_, err = db.C.Collection("CUSTOMER").UpdateOne(db.ctx, bson.D{
		{"C_ID", customerId},
		{"C_W_ID", warehouseId},
		{"C_D_ID", districtId},
	}, update, nil)

	if err != nil {
		return err
	}

	return nil
}

func (db *MongoDB) CreateOrder(orderId int, customerId int, warehouseId int, districtId int, oCarrierId int, oOlCnt int, allLocal int, orderEntryDate time.Time, orderLine []models.OrderLine) error {

	order := models.Order{
		O_ID:         orderId,
		O_C_ID:       customerId,
		O_D_ID:       districtId,
		O_W_ID:       warehouseId,
		O_ENTRY_D:    orderEntryDate,
		O_CARRIER_ID: oCarrierId,
		O_OL_CNT:     oOlCnt,
		O_ALL_LOCAL:  allLocal,
		ORDER_LINE:   orderLine,
	}

	_, err := db.C.Collection("NEW_ORDER").InsertOne(db.ctx, bson.D{
		{"NO_O_ID", orderId},
		{"NO_D_ID", districtId},
		{"NO_W_ID", warehouseId},
	})

	if err != nil {
		return err
	}

	_, err = db.C.Collection("ORDERS").InsertOne(db.ctx, order)

	if err != nil {
		return nil
	}

	return nil
}

// todo: sharding
func (db *MongoDB) GetItems(itemIds []int) (*[]models.Item, error) {

	cursor, err := db.C.Collection("ITEM").Find(db.ctx, bson.D{
		{"I_ID", bson.D{
			{"$in", itemIds},
		}}}, options.Find().SetProjection(bson.D{
		{"_id", 0},
		{"I_ID", 1},
		{"I_PRICE", 1},
		{"I_NAME", 1},
		{"I_DATA", 1},
	}))

	if err != nil {
		return nil, err
	}

	var items []models.Item
	err = cursor.All(db.ctx, &items)

	if err != nil {
		return nil, err
	}

	return &items, nil
}

func (db *MongoDB) GetStockInfo(districtId int, iIds []int, iWids []int, allLocal int) (*[]models.Stock, error) {
	var err error
	distCol := fmt.Sprintf("S_DIST_%02d", districtId)
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

	var cursor *mongo.Cursor
	if allLocal == 1 {
		cursor, err = db.C.Collection("STOCK").Find(db.ctx, bson.D{
			{"S_I_ID", bson.D{
				{"$in", iIds},
			}},
			{"S_W_ID", iWids[0]},
		})

		if err != nil {
			return nil, err
		}
	} else {
		var searchList []bson.D
		for item, value := range iIds {
			searchList = append(searchList, bson.D{
				{"S_I_ID", value},
				{"S_W_ID", iWids[item]},
			})
		}

		cursor, err = db.C.Collection("STOCK").Find(db.ctx, bson.D{
			{"$or", searchList},
		}, options.Find().SetProjection(stockProjection))

		if err != nil {
			return nil, err
		}
	}

	var stocks []models.Stock

	err = cursor.All(db.ctx, &stocks)
	if err != nil {
		return nil, err
	}

	return &stocks, nil
}

func (db *MongoDB) UpdateStock(stockId int, warehouseId int, quantity int, ytd int, ordercnt int, remotecnt int) error {
	ru, err := db.C.Collection("STOCK").UpdateOne(db.ctx, bson.D{
		{"S_I_ID", stockId},
		{"S_W_ID", warehouseId},
	}, bson.D{
		{"$set", bson.D{
			{"S_QUANTITY", quantity},
			{"S_YTD", ytd},
			{"S_ORDER_CNT", ordercnt},
			{"S_REMOTE_CNT", remotecnt},
		}},
	})

	if err != nil {
		return err
	}

	if ru.MatchedCount == 0 {
		return fmt.Errorf("0 document matched")
	}

	return nil
}

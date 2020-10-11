package databases

import (
	"github.com/Percona-Lab/go-tpcc/databases/mongodb"
	"github.com/Percona-Lab/go-tpcc/databases/mysql"
	"github.com/Percona-Lab/go-tpcc/databases/postgresql"
	"github.com/Percona-Lab/go-tpcc/tpcc/models"
	"time"
)

type Database interface {
	StartTrx() error
	CommitTrx() error
	RollbackTrx() error
	CreateSchema() error
	CreateIndexes() error
	InsertOne(tableName string, d interface{}) error
	InsertBatch(tableName string, d []interface{}) error
	IncrementDistrictOrderId(warehouseId int, districtId int) error
	GetNewOrder(warehouseId int, districtId int) (*models.NewOrder, error)
	DeleteNewOrder(orderId int, warehouseId int, districtId int) error
	GetCustomer(customerId int, warehouseId int, districtId int) (*models.Customer, error)
	GetCustomerIdOrder(orderId int, warehouseId int, districtId int) (int, error)
	UpdateOrders(orderId int, warehouseId int, districtId int, oCarrierId int, deliveryDate time.Time) error
	SumOLAmount(orderId int, warehouseId int, districtId int) (float64, error)
	UpdateCustomer(customerId int, warehouseId int, districtId int, sumOlTotal float64) error
	GetNextOrderId(warehouseId int, districtId int) (int, error)
	GetStockCount(orderIdLt int, orderIdGt int, threshold int, warehouseId int, districtId int) (int64, error)
	GetCustomerById(customerId int, warehouseId int, districtId int) (*models.Customer, error)
	GetCustomerByName(name string, warehouseId int, districtId int) (*models.Customer, error)
	GetLastOrder(customerId int, warehouseId int, districtId int) (*models.Order, error)
	GetOrderLines(orderId int, warehouseId int, districtId int) (*[]models.OrderLine, error)
	GetWarehouse(warehouseId int) (*models.Warehouse, error)
	UpdateWarehouseBalance(warehouseId int, amount float64) error
	GetDistrict(warehouseId int, districtId int) (*models.District, error)
	UpdateDistrictBalance(warehouseId int, districtId int, amount float64) error
	InsertHistory(warehouseId int, districtId int, date time.Time, amount float64, data string) error
	UpdateCredit(customerId int, warehouseId int, districtId int, balance float64, data string) error
	CreateOrder(orderId int, customerId int, warehouseId int, districtId int, oCarrierId int, oOlCnt int, allLocal int, orderEntryDate time.Time, orderLine []models.OrderLine) error
	GetItems(itemIds []int) (*[]models.Item, error)
	UpdateStock(stockId int, warehouseId int, quantity int, ytd int, ordercnt int, remotecnt int) error
	GetStockInfo(districtId int, iIds []int, iWids []int, allLocal int) (*[]models.Stock, error)
}

func NewDatabase(driver, uri, dbname, username, password string, transactions bool, findandmodify bool) (Database, error) {
	var d Database
	var err error
	
	switch driver {
	case "mongodb":
		d, err = mongodb.NewMongoDb(uri, dbname, transactions, findandmodify)
	case "mysql":
		d, err = mysql.NewMySQL(uri, dbname, transactions)
	case "postgresql":
		d, err = postgresql.NewPostgreSQL(uri, dbname, transactions)
	default:
		panic("Unknown database driver")
	}

	if err != nil {
		return nil, err
	}

	return d, nil
}

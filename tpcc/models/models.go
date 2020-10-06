package models

import "time"

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

type History struct {
	H_C_ID   int `bson:"H_C_ID" 	`
	H_C_D_ID int `bson:"H_C_D_ID"`
	H_C_W_ID int `bson:"H_C_W_ID"`
	H_D_ID   int `bson:"H_D_ID"`
	H_W_ID   int `bson:"H_W_ID"`
	H_DATE   time.Time `bson:"H_DATE"`
	H_AMOUNT float64 `bson:"H_AMOUNT"`
	H_DATA   string `bson:"H_DATA"`
}

type Order struct {
	O_ID         int `bson:"O_ID"`
	O_C_ID       int `bson:"O_C_ID"`
	O_D_ID       int `bson:"O_D_ID"`
	O_W_ID       int `bson:"O_W_ID"`
	O_ENTRY_D    time.Time `bson:"O_ENTRY_D"`
	O_CARRIED_ID int `bson:"O_CARRIED_ID"`
	O_OL_CNT     int `bson:"O_OL_CNT"`
	O_ALL_LOCAL  int `bson:"O_ALL_LOCAL"`
	ORDER_LINE []OrderLine `bson:"ORDER_LINE,omitempty"`
}

type NewOrder struct {
	NO_O_ID int `bson:"NO_O_ID"`
	NO_D_ID int `bson:"NO_D_ID"`
	NO_W_ID int `bson:"NO_W_ID"`
}

type OrderLine struct {
	OL_O_ID        int `bson:"OL_O_ID"`
	OL_D_ID        int `bson:"OL_D_ID"`
	OL_W_ID        int `bson:"OL_W_ID"`
	OL_NUMBER      int `bson:"OL_NUMBER"`
	OL_I_ID        int `bson:"OL_I_ID"`
	OL_SUPPLY_W_ID int `bson:"OL_SUPPLY_W_ID"`
	OL_DELIVERY_D  time.Time `bson:"OL_DELIVERY_D"`
	OL_QUANTITY     int `bson:"OL_QUANTITY"`
	OL_AMOUNT      float64 `bson:"OL_AMOUNT"`
	OL_DIST_INFO   string `bson:"OL_DIST_INFO"`
}

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

type Stock struct {
	S_I_ID       int    `bson:"S_I_ID"`
	S_W_ID       int    `bson:"S_W_ID"`
	S_QUANTITY   int    `bson:"S_QUANTITY"`
	S_DIST_01    string `bson:"S_DIST_01"`
	S_DIST_02    string `bson:"S_DIST_02"`
	S_DIST_03    string `bson:"S_DIST_03"`
	S_DIST_04    string `bson:"S_DIST_04"`
	S_DIST_05    string `bson:"S_DIST_05"`
	S_DIST_06    string `bson:"S_DIST_06"`
	S_DIST_07    string `bson:"S_DIST_07"`
	S_DIST_08    string `bson:"S_DIST_08"`
	S_DIST_09    string `bson:"S_DIST_09"`
	S_DIST_10    string `bson:"S_DIST_10"`
	S_YTD        int    `bson:"S_YTD"`
	S_ORDER_CNT  int    `bson:"S_ORDER_CNT"`
	S_REMOTE_CNT int    `bson:"S_REMOTE_CNT"`
	S_DATA       string `bson:"S_DATA"`
	distCol      string
}


type Item struct {
	I_ID    int `bson:"I_ID"`
	I_IM_ID int `bson:"I_IM_ID"`
	I_NAME  string `bson:"I_NAME"`
	I_PRICE float64 `bson:"I_PRICE"`
	I_DATA  string `bson:"I_DATA"`
}
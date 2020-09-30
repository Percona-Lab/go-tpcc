package tpcc

import (
	"github.com/Percona-Lab/go-tpcc/helpers"
	"time"
)

type History struct {
	H_C_ID   int `bson:"H_C_ID"`
	H_C_D_ID int `bson:"H_C_D_ID"`
	H_C_W_ID int `bson:"H_C_W_ID"`
	H_D_ID   int `bson:"H_D_ID"`
	H_W_ID   int `bson:"H_W_ID"`
	H_DATE   time.Time `bson:"H_DATE"`
	H_AMOUNT float64 `bson:"H_AMOUNT"`
	H_DATA   string `bson:"H_DATA"`
}

func (w *Worker) generateHistory(hCWId int, hCDId int, hCId int) History {
	return History{
		H_C_ID:   hCId,
		H_C_D_ID: hCDId,
		H_C_W_ID: hCWId,
		H_D_ID:   hCDId,
		H_W_ID:   hCWId,
		H_DATE:   time.Now(),
		H_AMOUNT: INITIAL_AMOUNT,
		H_DATA:   helpers.RandString(helpers.RandInt(MIN_DATA, MAX_DATA)),
	}
}

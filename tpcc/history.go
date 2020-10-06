package tpcc

import (
	"github.com/Percona-Lab/go-tpcc/helpers"
	"github.com/Percona-Lab/go-tpcc/tpcc/models"
	"time"
)



func (w *Worker) generateHistory(hCWId int, hCDId int, hCId int) models.History {
	return models.History{
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

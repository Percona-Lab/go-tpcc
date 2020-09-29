package cmd

import (
	"context"
	"fmt"
	_ "mongo-tpcc/helpers"
	"mongo-tpcc/tpcc"
	_ "mongo-tpcc/tpcc"
	"sync"
	"time"

	"github.com/spf13/cobra"
)

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run",
	Run: func(cmd *cobra.Command, args []string) {

		warehouses, _ := cmd.PersistentFlags().GetInt("warehouses")
		threads, _ := cmd.PersistentFlags().GetInt("threads")
		scalefactor, _ := cmd.PersistentFlags().GetFloat64("scalefactor")
		ri, _ := cmd.PersistentFlags().GetInt("report-interval")
		time, _ := cmd.PersistentFlags().GetInt("time")
		dbname, _ := cmd.Root().PersistentFlags().GetString("db")
		uri, _ := cmd.Root().PersistentFlags().GetString("uri")
		trx,_ := cmd.Root().PersistentFlags().GetBool("trx")




		var t tpcc.Configuration
		t.Threads = 1
		ctx, cancel := context.WithCancel(context.Background())
		wg := &sync.WaitGroup{}
		c := make(chan tpcc.Transaction, 1024)

		for i:=0; i<threads; i++ {
			wg.Add(1)
			go func(i int) {

				conf := tpcc.Configuration{
					DBName:         dbname,
					Threads:        threads,
					WriteConcern:   0,
					ReadConcern:    0,
					ReportInterval: ri,
					WareHouses:     warehouses,
					ScaleFactor:    scalefactor,
					URI: uri,
					Transactions: trx,
				}

				w, err := tpcc.NewWorker(ctx, &conf, wg, c, i)
				if err != nil  {
					panic(err)
				}
				w.Execute()
			}(i)
		}

		wg.Add(1)
		go stats(cancel, c, wg, time, ri)
		wg.Wait()
	},
}

func init() {
	rootCmd.AddCommand(runCmd)

	runCmd.PersistentFlags().Int("threads", 8, "Amount of threads that will be used when preparing. min(threads, warehouses) will be used at most")
	runCmd.PersistentFlags().Int("report-interval", 1, "Report interval")
	runCmd.PersistentFlags().Int("time", 10, "How long to run the test")
	runCmd.PersistentFlags().Int("warehouses", 10, "Number of warehouses to generate the data")
	runCmd.PersistentFlags().Float64("scalefactor", 1, "Scale-factor")

	rootCmd.MarkFlagRequired("uri")
	rootCmd.MarkFlagRequired("db")
}

func stats( cancel context.CancelFunc, c chan tpcc.Transaction,  wg *sync.WaitGroup, ttime int, ri int ) {
	defer wg.Done()
	ticker := time.NewTicker(time.Duration(ri) * time.Second)
	timeout := time.After(time.Duration(ttime) * time.Second + 99 * time.Millisecond)
	i:=ri
	type Transactions struct {
		StockLevelCnt int
		DeliveryCnt int
		OrderStatusCnt int
		PaymentCnt int
		NewOrderCnt int
		Failed int
	}

	globalStats := make(map[int]*Transactions)
	batchStats := make(map[int]*Transactions)


	for {
		select {
			case <-timeout:
				cancel()
				time.Sleep(1 * time.Second)
				return
			case v:=<-c:

				_, exist := globalStats[v.ThreadId]

				if ! exist {
					globalStats[v.ThreadId] = &Transactions{
						StockLevelCnt:  0,
						DeliveryCnt:    0,
						OrderStatusCnt: 0,
						PaymentCnt:     0,
						NewOrderCnt:    0,
					}
				}

				_, exist = batchStats[v.ThreadId]

				if ! exist {
					batchStats[v.ThreadId] = &Transactions{
						StockLevelCnt:  0,
						DeliveryCnt:    0,
						OrderStatusCnt: 0,
						PaymentCnt:     0,
						NewOrderCnt:    0,
					}
				}

				if v.Failed {
					batchStats[v.ThreadId].Failed++
					globalStats[v.ThreadId].Failed++
				}

				switch v.Type {
				case tpcc.StockLevelTrx:
					batchStats[v.ThreadId].StockLevelCnt++
					globalStats[v.ThreadId].StockLevelCnt++
				case tpcc.DeliveryTrx:
					batchStats[v.ThreadId].DeliveryCnt++
					globalStats[v.ThreadId].DeliveryCnt++
				case tpcc.OrderStatusTrx:
					batchStats[v.ThreadId].OrderStatusCnt++
					globalStats[v.ThreadId].OrderStatusCnt++
				case tpcc.PaymentTrx:
					batchStats[v.ThreadId].PaymentCnt++
					globalStats[v.ThreadId].PaymentCnt++
				case tpcc.NewOrderTrx:
					batchStats[v.ThreadId].NewOrderCnt++
					globalStats[v.ThreadId].NewOrderCnt++
				}
			case <-ticker.C:
				sCnt := 0
				dCnt := 0
				oCnt := 0
				pCnt := 0
				nCnt := 0
				failed := 0

				for _, value := range batchStats {
					sCnt += value.StockLevelCnt
					dCnt += value.DeliveryCnt
					oCnt += value.OrderStatusCnt
					pCnt += value.PaymentCnt
					nCnt += value.NewOrderCnt
					failed += value.Failed
				}
				batchStats = make(map[int]*Transactions)

				fmt.Printf(
					"[ %ds ] TPS: %.2f StockLevel: %d Delivery: %d OrderStatus: %d Payment: %d NewOrder: %d Failed: %d\n",
					i,
					float64(sCnt+dCnt+oCnt+pCnt+nCnt)/float64(ri),
					sCnt,
					dCnt,
					oCnt,
					pCnt,
					nCnt,
					failed,
				)
				i += ri
			default:
		}
	}
}

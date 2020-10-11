package cmd

import (
	"context"
	"fmt"
	"github.com/Percona-Lab/go-tpcc/tpcc"
	"math"
	"sort"
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
		rf_, _ := cmd.PersistentFlags().GetString("report-format")
		perc, _ := cmd.PersistentFlags().GetInt("percentile")
		percfail, _ := cmd.PersistentFlags().GetInt("percent-fail")
		dbdriver, _ := cmd.Root().PersistentFlags().GetString("dbdriver")


		if perc > 100 || perc < 0 {
			panic("percentile not correct")
		}

		var rf OutputType
		switch rf_ {
		case "json":
			rf = JSONOutput

		case "csv":
			rf=CSVOutput

		default:
			rf=DefaultOutput
		}

		var t tpcc.Configuration
		t.Threads = 1
		ctx, cancel := context.WithCancel(context.Background())
		wg := &sync.WaitGroup{}
		c := make(chan tpcc.Transaction, 1024)

		for i:=0; i<threads; i++ {
			wg.Add(1)
			go func(i int) {

				conf := tpcc.Configuration{
					DBDriver: 		dbdriver,
					DBName:         dbname,
					Threads:        threads,
					WriteConcern:   0,
					ReadConcern:    0,
					ReportInterval: ri,
					WareHouses:     warehouses,
					ScaleFactor:    scalefactor,
					URI: uri,
					Transactions: trx,
					PercentFail: percfail,
				}

				w, err := tpcc.NewWorker(ctx, &conf, wg, c, i)
				if err != nil  {
					panic(err)
				}
				w.Execute()
			}(i)
		}

		wg.Add(1)
		go stats(cancel, c, wg, time, ri, rf, float64(perc))
		wg.Wait()
	},
}

func init() {
	rootCmd.AddCommand(runCmd)

	runCmd.PersistentFlags().Int("threads", 8, "Amount of threads that will be used when preparing. min(threads, warehouses) will be used at most")
	runCmd.PersistentFlags().Int("report-interval", 1, "Report interval")
	runCmd.PersistentFlags().Int("time", 10, "How long to run the test")
	runCmd.PersistentFlags().Int("warehouses", 10, "Number of warehouses to generate the data")
	runCmd.PersistentFlags().Int("percentile", 95, "Percentile for latency reporting")
	runCmd.PersistentFlags().Int("percent-fail", 0, "How much % of New Order trxs should fail [0-100]")


	runCmd.PersistentFlags().Float64("scalefactor", 1, "Scale-factor")
	runCmd.PersistentFlags().String("report-format", "default", "default|json|csv")

	rootCmd.MarkFlagRequired("uri")
	rootCmd.MarkFlagRequired("db")
}

type OutputType int
const (
	DefaultOutput = iota
	CSVOutput
	JSONOutput
)

func stats( cancel context.CancelFunc, c chan tpcc.Transaction,  wg *sync.WaitGroup, ttime int, ri int, output OutputType, percentile float64) {
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
	latencies := make(map[tpcc.TransactionType][]float64)


	if output == CSVOutput {
		fmt.Println("Time,TPS,StockLevel,StockLevelLatency,Delivery,DeliveryLatency,OrderStatus,OrderStatusLatency,Payment,PaymentLatency,NewOrder,NewOrderLatency,Failed")
	}

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

			latencies[v.Type] = append(latencies[v.Type], v.Time)


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
				var format string
				switch output {
				case CSVOutput:
					format = "%d,%.2f,%d,%.2f,%d,%.2f,%d,%.2f,%d,%.2f,%d,%.2f,%d\n"
				case JSONOutput:
					format = "{\"time\": %d, \"tps\": %.2f, \"StockLevel\": { \"Trx\": %d, \"LatencyPercentile\": %.2f}, " +
						"\"Delivery\": { \"Trx\": %d, \"LatencyPercentile\": %.2f}, " +
						"\"OrderStatus\": { \"Trx\": %d, \"LatencyPercentile\":%.2f}, " +
						"\"Payment\": { \"Trx\": %d, \"LatencyPercentile\": %.2f}, " +
						"\"NewOrder\": { \"Trx\": %d, \"LatencyPercentile\": %.2f}," +
						"\"Failed\": %d}\n"
				default:
					format = "[ %ds ] TPS: %.2f StockLevel: %d (%.2f ms) Delivery: %d (%.2f ms) OrderStatus: %d (%.2f ms) Payment: %d (%.2f ms) NewOrder: %d (%.2f ms) Failed: %d\n"
				}

				fmt.Printf(
					format,
					i,
					float64(sCnt+dCnt+oCnt+pCnt+nCnt)/float64(ri),
					sCnt,
					float64(perc(latencies[tpcc.StockLevelTrx], percentile)),
					dCnt,
					float64(perc(latencies[tpcc.DeliveryTrx], percentile)),
					oCnt,
					float64(perc(latencies[tpcc.OrderStatusTrx], percentile)),
					pCnt,
					float64(perc(latencies[tpcc.PaymentTrx], percentile)),
					nCnt,
					float64(perc(latencies[tpcc.NewOrderTrx], percentile)),
					failed,
				)

				i += ri
				latencies = make(map[tpcc.TransactionType][]float64)
		default:
		}
	}
}

func perc(a []float64, p float64) float64 {
	p = p/100
	if len(a) == 0 {
		return 0
	}
	sort.Float64s(a)
	idx := int( math.Round(float64(len(a))*p) )
	return a[idx-1]
}
package cmd

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/Percona-Lab/go-tpcc/tpcc"
)


var prepareCmd = &cobra.Command{
	Use:   "prepare",
	Short: "Prepare the TPC-C dataset",
	Run: func(cmd *cobra.Command, args []string) {


		warehouses, _ := cmd.PersistentFlags().GetInt("warehouses")
		threads, _ := cmd.PersistentFlags().GetInt("threads")
		scalefactor, _ := cmd.PersistentFlags().GetFloat64("scalefactor")
		dbname, _ := cmd.Root().PersistentFlags().GetString("db")
		dbdriver, _ := cmd.Root().PersistentFlags().GetString("dbdriver")

		uri, _ := cmd.Root().PersistentFlags().GetString("uri")
		trx,_ := cmd.Root().PersistentFlags().GetBool("trx")

		wj := make(chan int, warehouses)
		wr := make(chan int, warehouses)

		if dbname == "" || uri == "" {
			panic("empty")
		}

		for i:=1;i <= warehouses; i++ {
			wj <- i
		}

		c := tpcc.Configuration{
			DBDriver: 		dbdriver,
			DBName:         dbname,
			Threads:        threads,
			WriteConcern:   0,
			ReadConcern:    0,
			ReportInterval: 0,
			WareHouses:     warehouses,
			ScaleFactor:    scalefactor,
			URI: uri,
			Transactions: trx,
		}

		ddl, err := tpcc.NewWorker(context.Background(), &c, nil, nil, 0)
		if err != nil {
			panic(err)
		}

		fmt.Println("Creating schema")
		err = ddl.CreateSchema()
		if err != nil {
			panic(err)
		}
		fmt.Println("... done")

		if err != nil {
			panic(err)
		}

		for i:=0; i < threads; i++ {
			go func(i int) {

				w, err := tpcc.NewWorker(context.Background(), &c, nil, nil, i)
				if err != nil  {
					panic(err)
				}

				if i == 0 {
					fmt.Println("Loading items")
					w.LoadItems()
				}

				for wId := range wj {

					fmt.Printf("Loading warehouse %d\n", wId)
					err := w.LoadWarehouse(wId)
					if err != nil {
						panic(err)
					}
					wr <- wId
				}

			}(i)
		}

		for i:=1;i <= warehouses; i++ {
			<- wr
		}

		fmt.Println("Creating indexes")
		ddl.CreateIndexes()

		if err != nil {
			panic(err)
		}



		fmt.Println("... done")

	},
}

func init() {
	rootCmd.AddCommand(prepareCmd)

	prepareCmd.PersistentFlags().Int("threads", 8, "Amount of threads that will be used when preparing. min(threads, warehouses) will be used at most")
	prepareCmd.PersistentFlags().Int("warehouses", 10, "Number of warehouses to generate the data")
	prepareCmd.PersistentFlags().Float64("scalefactor", 1, "Scale-factor")

	prepareCmd.Root().MarkFlagRequired("uri")
	prepareCmd.Root().MarkFlagRequired("db")

}
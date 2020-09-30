// Copyright © 2020 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
					w.LoadWarehouse(wId)
					wr <- wId
				}

			}(i)
		}

		for i:=1;i <= warehouses; i++ {
			<- wr
		}

		w, err := tpcc.NewWorker(context.Background(), &c, nil, nil, 0)
		if err != nil {
			panic(err)
		}
		err = w.CreateIndexes()

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
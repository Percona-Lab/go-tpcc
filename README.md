# go-tpcc

## Preparing dataset

```
./go-tpcc prepare  --threads 10 --warehouses 20 --uri mongodb://localhost:27017 --db DatabaseName
```


## Running test


```
./go-tpcc run  --threads 1 --warehouses 2 --uri mongodb://localhost:27017 --db DatabaseName --time 200 --trx --report-format json --percentile 95 --report-interval 1 --percent-fail 0

 ./go-tpcc help run
Run

Usage:
  go-tpcc run [flags]

Flags:
  -h, --help                   help for run
      --percent-fail int       How much % of New Order trxs should fail [0-100]
      --percentile int         Percentile for latency reporting (default 95)
      --report-format string   default|json|csv (default "default")
      --report-interval int    Report interval (default 1)
      --scalefactor float      Scale-factor (default 1)
      --threads int            Amount of threads that will be used when preparing. min(threads, warehouses) will be used at most (default 8)
      --time int               How long to run the test (default 10)
      --warehouses int         Number of warehouses to generate the data (default 10)

Global Flags:
      --db string    database name to use
      --trx          use trx?. false by default
      --uri string   DSN

```
package cmd

import (
	"fmt"
	"os"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgFile string

var rootCmd = &cobra.Command{
	Use:   "go-tpcc",
	Short: "TPC-C implementation for various databases. Currently only MongoDB is supported",
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	//rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.mongo-tpcc.yaml)")
	rootCmd.PersistentFlags().String("uri", "", "DSN")
	rootCmd.PersistentFlags().String("db", "", "database name to use")
	rootCmd.PersistentFlags().String("dbdriver", "mysql", "db driver to use (mongodb|mysql)")
	rootCmd.PersistentFlags().Bool("trx", false, "use trx?. false by default")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".mongo-tpcc" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".mongo-tpcc")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}

}

/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"log"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "cmsrvu",
	Short: "Get CMS RVU files and load them into db",
	Long:  ``,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	// Run: func(cmd *cobra.Command, args []string) { },
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	v := viper.New()
	v.SetConfigName("cmsrvu")
	v.SetConfigType("yaml")
	v.SetEnvPrefix("CMSRVU_")
	cfgDir, err := os.UserConfigDir()
	if err != nil {
		log.Println(err)
	}
	v.AddConfigPath(cfgDir)
	pwd, err := os.Getwd()
	if err != nil {
		log.Println(err)
	}
	v.AddConfigPath(pwd)

	err = rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	// rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.cmsrvu.yaml)")

}

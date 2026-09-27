package cmd

import (
	"fmt"

	"github.com/cinvat/peretum/api"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var apiCmd = &cobra.Command{
	Use:   "api",
	Short: "rest api",
	Long:  ``,
	PreRun: func(cmd *cobra.Command, args []string) {
		viper.BindPFlag("PORT", cmd.Flags().Lookup("port"))
	},
	Run: func(cmd *cobra.Command, args []string) {
		httpServer := api.Register()
		httpServer.Run(fmt.Sprintf("0.0.0.0:%d", viper.GetInt("PORT")))
	},
}

func init() {
	apiCmd.Flags().IntP("port", "P", 8080, "API server port.")
	rootCmd.AddCommand(apiCmd)
}

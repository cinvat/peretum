package cmd

import (
	"fmt"

	"github.com/cinvat/peretum/api"
	"github.com/cinvat/peretum/api/v1alpha1/service"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var apiCmd = &cobra.Command{
	Use:   "api",
	Short: "rest api",
	Long:  ``,
	PreRun: func(cmd *cobra.Command, args []string) {
		viper.BindPFlag("PORT", cmd.Flags().Lookup("port"))
		viper.BindPFlag("CACHE_DIR", cmd.Flags().Lookup("cache-dir"))
		service.SetCacheDir(viper.GetString("CACHE_DIR"))
		viper.BindPFlag("NATS_URI", cmd.Flags().Lookup("nats-uri"))
		service.SetNATSURI(viper.GetString("NATS_URI"))
	},
	Run: func(cmd *cobra.Command, args []string) {
		httpServer := api.Register()
		httpServer.Run(fmt.Sprintf("0.0.0.0:%d", viper.GetInt("PORT")))
	},
}

func init() {
	apiCmd.Flags().IntP("port", "P", 8080, "API server port.")
	apiCmd.Flags().String("cache-dir", "./cache", "Disk-cache root shared with the proxy (used by DELETE /cache).")
	apiCmd.Flags().String("nats-uri", "", "NATS URL for cluster mode: DELETE /cache publishes to every edge instead of purging local files.")
	rootCmd.AddCommand(apiCmd)
}

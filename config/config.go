package config

import (
	"fmt"
	"github.com/spf13/viper"
)

type Config struct {
	Env    string
	Logger logger
	Csv    csv
	Sql    sql
}

type logger struct {
	Path    string
	Level   string
	Console bool
}

type csv struct {
	Path                                     string
	OldAdPvClickCostDailyCsvFileName         string
	OldCampaignDataDailySummariesCsvFileName string
	OldCreativeDataDailySummariesCsvFileName string
	OldUserDataDailySummariesCsvFileName     string
	OldAllUsersDataDailySummariesCsvFileName string
}

type sql struct {
	Path                                  string
	AdPvClickCostDailySqlFileName         string
	CampaignDataDailySummariesSqlFileName string
	CampaignDataSummariesSqlFileName      string
	CreativeDataDailySummariesSqlFileName string
	CreativeDataSummariesSqlFileName      string
	UserDataDailySummariesSqlFileName     string
	AllUsersDataDailySummariesSqlFileName string
}

func NewConfig(params *Params) *Config {
	conf := Config{}
	viper.SetConfigName("config" + "_" + params.Env)
	viper.SetConfigType("yaml")
	viper.AddConfigPath(params.ConfigPath)

	if err := viper.ReadInConfig(); err != nil {
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}

	if err := viper.Unmarshal(&conf); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	return &conf
}

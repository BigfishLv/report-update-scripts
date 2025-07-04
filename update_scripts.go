package main

import (
	"context"
	"report-update-scripts/config"
	"report-update-scripts/domain/csv"
	"report-update-scripts/logger"
	"report-update-scripts/reader"
	"report-update-scripts/service"
)

func main() {
	params := config.NewParams()
	config := config.NewConfig(params)
	logger.InitLogger(config.Logger.Path, config.Logger.Level, config.Logger.Console)
	sqlService := service.NewGenerateSqlFileService()

	oldAdPvFileProcessor := reader.CsvFileProcessor[csv.AdPvClickCostDailyCsvData]{}
	filePath := config.Csv.Path + config.Csv.OldAdPvClickCostDailyCsvFileName
	oldAdPvClickCostCsvDataArray, err := oldAdPvFileProcessor.Read(filePath)
	if err != nil {
		logger.Error(context.Background(), "oldAdPvFileProcessor read filePath : %s, error : %+v", filePath, err)
		return
	}
	sqlPath := config.Sql.Path + config.Sql.AdPvClickCostDailySqlFileName
	sqlService.GenerateAdPvClickCostDailySqlFile(oldAdPvClickCostCsvDataArray, sqlPath)

	oldCampaignDataDailyFileProcessor := reader.CsvFileProcessor[csv.CampaignDataDailySummariesCsvData]{}
	filePath = config.Csv.Path + config.Csv.OldCampaignDataDailySummariesCsvFileName
	oldCampaignDataDailyCsvDataArray, err := oldCampaignDataDailyFileProcessor.Read(filePath)
	if err != nil {
		logger.Error(context.Background(), "oldCampaignDataDailyFileProcessor read filePath : %s, error : %+v", filePath, err)
		return
	}
	sqlPath = config.Sql.Path + config.Sql.CampaignDataDailySummariesSqlFileName
	sqlService.GenerateCampaignDataDailySummariesSqlFile(oldCampaignDataDailyCsvDataArray, sqlPath)

	sqlPath = config.Sql.Path + config.Sql.CampaignDataSummariesSqlFileName
	sqlService.GenerateCampaignDataSummariesSqlFile(oldCampaignDataDailyCsvDataArray, sqlPath)

	oldCreativeDataDailyFileProcessor := reader.CsvFileProcessor[csv.CreativeDataDailySummariesCsvData]{}
	filePath = config.Csv.Path + config.Csv.OldCreativeDataDailySummariesCsvFileName
	oldCreativeDataDailyCsvDataArray, err := oldCreativeDataDailyFileProcessor.Read(filePath)
	if err != nil {
		logger.Error(context.Background(), "oldCreativeDataDailyFileProcessor read filePath : %s, error : %+v", filePath, err)
		return
	}
	sqlPath = config.Sql.Path + config.Sql.CreativeDataDailySummariesSqlFileName
	sqlService.GenerateCreativeDataDailySummariesSqlFile(oldCreativeDataDailyCsvDataArray, sqlPath)

	sqlPath = config.Sql.Path + config.Sql.CreativeDataSummariesSqlFileName
	sqlService.GenerateCreativeDataSummariesSqlFile(oldCreativeDataDailyCsvDataArray, sqlPath)

	oldUserDataDailyFileProcessor := reader.CsvFileProcessor[csv.UserDataDailySummariesCsvData]{}
	filePath = config.Csv.Path + config.Csv.OldUserDataDailySummariesCsvFileName
	oldUserDataDailyCsvDataArray, err := oldUserDataDailyFileProcessor.Read(filePath)
	if err != nil {
		logger.Error(context.Background(), "oldUserDataDailyFileProcessor read filePath : %s, error : %+v", filePath, err)
		return
	}
	sqlPath = config.Sql.Path + config.Sql.UserDataDailySummariesSqlFileName
	sqlService.GenerateUserDataDailySummariesSqlFile(oldUserDataDailyCsvDataArray, sqlPath)

	oldAllUsersDataDailyFileProcessor := reader.CsvFileProcessor[csv.AllUsersDataDailySummariesCsvData]{}
	filePath = config.Csv.Path + config.Csv.OldAllUsersDataDailySummariesCsvFileName
	oldAllUsersDataDailyCsvDataArray, err := oldAllUsersDataDailyFileProcessor.Read(filePath)
	if err != nil {
		logger.Error(context.Background(), "oldAllUsersDataDailyFileProcessor read filePath : %s, error : %+v", filePath, err)
		return
	}
	sqlPath = config.Sql.Path + config.Sql.AllUsersDataDailySummariesSqlFileName
	sqlService.GenerateAllUsersDataDailySummariesSqlFile(oldAllUsersDataDailyCsvDataArray, sqlPath)
	logger.Info(context.Background(), "report update script run succeed.")
}

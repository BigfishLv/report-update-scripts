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
	newAdPvFileProcessor := reader.CsvFileProcessor[csv.AdPvClickCostDailyCsvData]{}
	filePath = config.Csv.Path + config.Csv.NewAdPvClickCostDailyCsvFileName
	newAdPvClickCostCsvDataArray, err := newAdPvFileProcessor.Read(filePath)
	if err != nil {
		logger.Error(context.Background(), "oldAdPvFileProcessor read filePath : %s, error : %+v", filePath, err)
		return
	}
	sqlPath := config.Sql.Path + config.Sql.AdPvClickCostDailySqlFileName
	sqlService.GenerateAdPvClickCostDailySqlFile(oldAdPvClickCostCsvDataArray, newAdPvClickCostCsvDataArray, sqlPath)

	oldCampaignDataDailyFileProcessor := reader.CsvFileProcessor[csv.CampaignDataDailySummariesCsvData]{}
	filePath = config.Csv.Path + config.Csv.OldCampaignDataDailySummariesCsvFileName
	oldCampaignDataDailyCsvDataArray, err := oldCampaignDataDailyFileProcessor.Read(filePath)
	if err != nil {
		logger.Error(context.Background(), "oldCampaignDataDailyFileProcessor read filePath : %s, error : %+v", filePath, err)
		return
	}
	newCampaignDataDailyFileProcessor := reader.CsvFileProcessor[csv.CampaignDataDailySummariesCsvData]{}
	filePath = config.Csv.Path + config.Csv.NewCampaignDataDailySummariesCsvFileName
	newCampaignDataDailyCsvDataArray, err := newCampaignDataDailyFileProcessor.Read(filePath)
	if err != nil {
		logger.Error(context.Background(), "newCampaignDataDailyFileProcessor read filePath : %s, error : %+v", filePath, err)
		return
	}
	sqlPath = config.Sql.Path + config.Sql.CampaignDataDailySummariesSqlFileName
	sqlService.GenerateCampaignDataDailySummariesSqlFile(oldCampaignDataDailyCsvDataArray, newCampaignDataDailyCsvDataArray, sqlPath)

	newCampaignDataFileProcessor := reader.CsvFileProcessor[csv.CampaignDataSummariesCsvData]{}
	filePath = config.Csv.Path + config.Csv.NewCampaignDataSummariesCsvFileName
	newCampaignDataCsvDataArray, err := newCampaignDataFileProcessor.Read(filePath)
	if err != nil {
		logger.Error(context.Background(), "newCampaignDataFileProcessor read filePath : %s, error : %+v", filePath, err)
		return
	}
	sqlPath = config.Sql.Path + config.Sql.CampaignDataSummariesSqlFileName
	sqlService.GenerateCampaignDataSummariesSqlFile(oldCampaignDataDailyCsvDataArray, newCampaignDataCsvDataArray, sqlPath)

	oldCreativeDataDailyFileProcessor := reader.CsvFileProcessor[csv.CreativeDataDailySummariesCsvData]{}
	filePath = config.Csv.Path + config.Csv.OldCreativeDataDailySummariesCsvFileName
	oldCreativeDataDailyCsvDataArray, err := oldCreativeDataDailyFileProcessor.Read(filePath)
	if err != nil {
		logger.Error(context.Background(), "oldCreativeDataDailyFileProcessor read filePath : %s, error : %+v", filePath, err)
		return
	}
	newCreativeDataDailyFileProcessor := reader.CsvFileProcessor[csv.CreativeDataDailySummariesCsvData]{}
	filePath = config.Csv.Path + config.Csv.NewCreativeDataDailySummariesCsvFileName
	newCreativeDataDailyCsvDataArray, err := newCreativeDataDailyFileProcessor.Read(filePath)
	if err != nil {
		logger.Error(context.Background(), "newCreativeDataDailyFileProcessor read filePath : %s, error : %+v", filePath, err)
		return
	}
	sqlPath = config.Sql.Path + config.Sql.CreativeDataDailySummariesSqlFileName
	sqlService.GenerateCreativeDataDailySummariesSqlFile(oldCreativeDataDailyCsvDataArray, newCreativeDataDailyCsvDataArray, sqlPath)

	newCreativeDataFileProcessor := reader.CsvFileProcessor[csv.CreativeDataSummariesCsvData]{}
	filePath = config.Csv.Path + config.Csv.NewCreativeDataSummariesCsvFileName
	newCreativeDataCsvDataArray, err := newCreativeDataFileProcessor.Read(filePath)
	if err != nil {
		logger.Error(context.Background(), "newCreativeDataFileProcessor read filePath : %s, error : %+v", filePath, err)
		return
	}
	sqlPath = config.Sql.Path + config.Sql.CreativeDataSummariesSqlFileName
	sqlService.GenerateCreativeDataSummariesSqlFile(oldCreativeDataDailyCsvDataArray, newCreativeDataCsvDataArray, sqlPath)

	oldUserDataDailyFileProcessor := reader.CsvFileProcessor[csv.UserDataDailySummariesCsvData]{}
	filePath = config.Csv.Path + config.Csv.OldUserDataDailySummariesCsvFileName
	oldUserDataDailyCsvDataArray, err := oldUserDataDailyFileProcessor.Read(filePath)
	if err != nil {
		logger.Error(context.Background(), "oldUserDataDailyFileProcessor read filePath : %s, error : %+v", filePath, err)
		return
	}
	newUserDataDailyFileProcessor := reader.CsvFileProcessor[csv.UserDataDailySummariesCsvData]{}
	filePath = config.Csv.Path + config.Csv.NewUserDataDailySummariesCsvFileName
	newUserDataDailyCsvDataArray, err := newUserDataDailyFileProcessor.Read(filePath)
	if err != nil {
		logger.Error(context.Background(), "newUserDataDailyCsvDataArray read filePath : %s, error : %+v", filePath, err)
		return
	}
	sqlPath = config.Sql.Path + config.Sql.UserDataDailySummariesSqlFileName
	sqlService.GenerateUserDataDailySummariesSqlFile(oldUserDataDailyCsvDataArray, newUserDataDailyCsvDataArray, sqlPath)

	oldAllUsersDataDailyFileProcessor := reader.CsvFileProcessor[csv.AllUsersDataDailySummariesCsvData]{}
	filePath = config.Csv.Path + config.Csv.OldAllUsersDataDailySummariesCsvFileName
	oldAllUsersDataDailyCsvDataArray, err := oldAllUsersDataDailyFileProcessor.Read(filePath)
	if err != nil {
		logger.Error(context.Background(), "oldAllUsersDataDailyFileProcessor read filePath : %s, error : %+v", filePath, err)
		return
	}
	newAllUsersDataDailyFileProcessor := reader.CsvFileProcessor[csv.AllUsersDataDailySummariesCsvData]{}
	filePath = config.Csv.Path + config.Csv.NewAllUsersDataDailySummariesCsvFileName
	newAllUsersDataDailyCsvDataArray, err := newAllUsersDataDailyFileProcessor.Read(filePath)
	if err != nil {
		logger.Error(context.Background(), "newAllUsersDataDailyFileProcessor read filePath : %s, error : %+v", filePath, err)
		return
	}
	sqlPath = config.Sql.Path + config.Sql.AllUsersDataDailySummariesSqlFileName
	sqlService.GenerateAllUsersDataDailySummariesSqlFile(oldAllUsersDataDailyCsvDataArray, newAllUsersDataDailyCsvDataArray, sqlPath)
	logger.Info(context.Background(), "report update script run succeed.")
}

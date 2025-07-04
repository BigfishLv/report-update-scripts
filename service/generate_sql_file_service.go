package service

import (
	"context"
	"fmt"
	"os"
	"report-update-scripts/domain/csv"
	"report-update-scripts/logger"
)

type GenerateSqlFileService struct {
}

func NewGenerateSqlFileService() *GenerateSqlFileService {
	return &GenerateSqlFileService{}
}

func (service *GenerateSqlFileService) GenerateAdPvClickCostDailySqlFile(oldCsvDataArray []*csv.AdPvClickCostDailyCsvData, filePath string) error {
	logger.Info(context.Background(), "GenerateAdPvClickCostDailySqlFile started.")
	layout := "2006-01-02"
	// 创建sql文件
	file, err := os.Create(filePath)
	if err != nil {
		logger.Error(context.Background(), "os open filePath : %s, error : %+v", filePath, err)
		return err
	}
	defer file.Close()

	insertSqlFormat := "insert into bp.ad_pv_click_cost_daily(happened_date, user_id, campaign_id, creative_id, bidding_type, view_count, click_count, spent, should_spent, user_balance) values('%s', %d, %d, %d, %d, %d, %d, %d, %d, %d) on duplicate key update view_count = view_count + %d, click_count = click_count + %d;\n"
	file.WriteString("begin;\n")
	for _, oldCsvData := range oldCsvDataArray {
		happenedDate := oldCsvData.HappenedDate.Time.Format(layout)
		insertSql := fmt.Sprintf(insertSqlFormat, happenedDate, oldCsvData.UserId, oldCsvData.CampaignId, oldCsvData.CreativeId, oldCsvData.BiddingType, oldCsvData.ViewCount, oldCsvData.ClickCount, oldCsvData.Spent, oldCsvData.ShouldSpent, oldCsvData.UserBalance, oldCsvData.ViewCount, oldCsvData.ClickCount)
		file.WriteString(insertSql)

	}
	file.WriteString("commit;\n")
	logger.Info(context.Background(), "GenerateAdPvClickCostDailySqlFile succeed.")
	return nil
}

func (service *GenerateSqlFileService) GenerateCampaignDataDailySummariesSqlFile(oldCsvDataArray []*csv.CampaignDataDailySummariesCsvData, filePath string) error {
	logger.Info(context.Background(), "GenerateCampaignDataDailySummariesSqlFile started.")
	layout := "2006-01-02"
	// 创建sql文件
	file, err := os.Create(filePath)
	if err != nil {
		logger.Error(context.Background(), "os open filePath : %s, error : %+v", filePath, err)
		return err
	}
	defer file.Close()

	insertSqlFormat := "insert into bp.campaign_data_daily_summaries(happened_date, campaign_id, view_count, click_count, spent, should_spent) values('%s', %d, %d, %d, %d, %d) on duplicate key update view_count = view_count + %d, click_count = click_count + %d;\n"
	file.WriteString("begin;\n")
	for _, oldCsvData := range oldCsvDataArray {
		happenedDate := oldCsvData.HappenedDate.Time.Format(layout)
		insertSql := fmt.Sprintf(insertSqlFormat, happenedDate, oldCsvData.CampaignId, oldCsvData.ViewCount, oldCsvData.ClickCount, oldCsvData.Spent, oldCsvData.ShouldSpent, oldCsvData.ViewCount, oldCsvData.ClickCount)
		file.WriteString(insertSql)
	}
	file.WriteString("commit;\n")
	logger.Info(context.Background(), "GenerateCampaignDataDailySummariesSqlFile succeed.")
	return nil
}

func (service *GenerateSqlFileService) GenerateCampaignDataSummariesSqlFile(dailyCampaignCsvDataArray []*csv.CampaignDataDailySummariesCsvData, filePath string) error {
	logger.Info(context.Background(), "GenerateCampaignDataSummariesSqlFile started.")

	dailyCampaignCsvDataMap := make(map[int64]*csv.CampaignDataSummariesCsvData)
	for _, csvData := range dailyCampaignCsvDataArray {
		if data, ok := dailyCampaignCsvDataMap[csvData.CampaignId]; ok {
			data.ViewCount += csvData.ViewCount
			data.ClickCount += csvData.ClickCount
		} else {
			newCsvData := &csv.CampaignDataSummariesCsvData{
				CampaignId:  csvData.CampaignId,
				ViewCount:   csvData.ViewCount,
				ClickCount:  csvData.ClickCount,
				Spent:       csvData.Spent,
				ShouldSpent: csvData.ShouldSpent,
				Version:     csvData.Version,
				CreatedAt:   csvData.CreatedAt,
				UpdatedAt:   csvData.UpdatedAt,
			}
			dailyCampaignCsvDataMap[csvData.CampaignId] = newCsvData
		}
	}

	// 创建sql文件
	file, err := os.Create(filePath)
	if err != nil {
		logger.Error(context.Background(), "os open filePath : %s, error : %+v", filePath, err)
		return err
	}
	defer file.Close()

	insertSqlFormat := "insert into bp.campaign_data_summaries(campaign_id, view_count, click_count, spent, should_spent) values(%d, %d, %d, %d, %d) on duplicate key update view_count = view_count + %d, click_count = click_count + %d;\n"
	file.WriteString("begin;\n")
	for _, campaignData := range dailyCampaignCsvDataMap {
		insertSql := fmt.Sprintf(insertSqlFormat, campaignData.CampaignId, campaignData.ViewCount, campaignData.ClickCount, campaignData.Spent, campaignData.ShouldSpent, campaignData.ViewCount, campaignData.ClickCount)
		file.WriteString(insertSql)
	}
	file.WriteString("commit;\n")
	logger.Info(context.Background(), "GenerateCampaignDataSummariesSqlFile succeed.")
	return nil
}

func (service *GenerateSqlFileService) GenerateCreativeDataDailySummariesSqlFile(oldCsvDataArray []*csv.CreativeDataDailySummariesCsvData, filePath string) error {
	logger.Info(context.Background(), "GenerateCreativeDataDailySummariesSqlFile started.")
	layout := "2006-01-02"
	// 创建sql文件
	file, err := os.Create(filePath)
	if err != nil {
		logger.Error(context.Background(), "os open filePath : %s, error : %+v", filePath, err)
		return err
	}
	defer file.Close()

	insertSqlFormat := "insert into bp.creative_data_daily_summaries(happened_date, creative_id, view_count, click_count, spent, should_spent) values('%s', %d, %d, %d, %d, %d) on duplicate key update view_count = view_count + %d, click_count = click_count + %d;\n"
	file.WriteString("begin;\n")
	for _, oldCsvData := range oldCsvDataArray {
		happenedDate := oldCsvData.HappenedDate.Time.Format(layout)
		insertSql := fmt.Sprintf(insertSqlFormat, happenedDate, oldCsvData.CreativeId, oldCsvData.ViewCount, oldCsvData.ClickCount, oldCsvData.Spent, oldCsvData.ShouldSpent, oldCsvData.ViewCount, oldCsvData.ClickCount)
		file.WriteString(insertSql)
	}
	file.WriteString("commit;\n")
	logger.Info(context.Background(), "GenerateCreativeDataDailySummariesSqlFile succeed.")
	return nil
}

func (service *GenerateSqlFileService) GenerateCreativeDataSummariesSqlFile(dailyCreativeCsvDataArray []*csv.CreativeDataDailySummariesCsvData, filePath string) error {
	logger.Info(context.Background(), "GenerateCreativeDataSummariesSqlFile started.")

	dailyCreativeCsvDataMap := make(map[int64]*csv.CreativeDataSummariesCsvData)
	for _, csvData := range dailyCreativeCsvDataArray {
		if data, ok := dailyCreativeCsvDataMap[csvData.CreativeId]; ok {
			data.ViewCount += csvData.ViewCount
			data.ClickCount += csvData.ClickCount
		} else {
			newCsvData := &csv.CreativeDataSummariesCsvData{
				CreativeId:  csvData.CreativeId,
				ViewCount:   csvData.ViewCount,
				ClickCount:  csvData.ClickCount,
				Spent:       csvData.Spent,
				ShouldSpent: csvData.ShouldSpent,
				Version:     csvData.Version,
				CreatedAt:   csvData.CreatedAt,
				UpdatedAt:   csvData.UpdatedAt,
			}
			dailyCreativeCsvDataMap[csvData.CreativeId] = newCsvData
		}
	}

	// 创建sql文件
	file, err := os.Create(filePath)
	if err != nil {
		logger.Error(context.Background(), "os open filePath : %s, error : %+v", filePath, err)
		return err
	}
	defer file.Close()

	insertSqlFormat := "insert into bp.creative_data_summaries(creative_id, view_count, click_count, spent, should_spent) values(%d, %d, %d, %d, %d) on duplicate key update view_count = view_count + %d, click_count = click_count + %d;\n"
	file.WriteString("begin;\n")
	for _, creativeData := range dailyCreativeCsvDataMap {
		insertSql := fmt.Sprintf(insertSqlFormat, creativeData.CreativeId, creativeData.ViewCount, creativeData.ClickCount, creativeData.Spent, creativeData.ShouldSpent, creativeData.ViewCount, creativeData.ClickCount)
		file.WriteString(insertSql)
	}
	file.WriteString("commit;\n")
	logger.Info(context.Background(), "GenerateCreativeDataSummariesSqlFile succeed.")
	return nil
}

func (service *GenerateSqlFileService) GenerateUserDataDailySummariesSqlFile(oldCsvDataArray []*csv.UserDataDailySummariesCsvData, filePath string) error {
	logger.Info(context.Background(), "GenerateUserDataDailySummariesSqlFile started.")
	layout := "2006-01-02"
	// 创建sql文件
	file, err := os.Create(filePath)
	if err != nil {
		logger.Error(context.Background(), "os open filePath : %s, error : %+v", filePath, err)
		return err
	}
	defer file.Close()

	insertSqlFormat := "insert into bp.user_data_daily_summaries(happened_date, user_id, view_count, click_count, spent, should_spent, bidding_type, balance) values('%s', %d, %d, %d, %d, %d, %d, %d) on duplicate key update view_count = view_count + %d, click_count = click_count + %d;\n"
	file.WriteString("begin;\n")
	for _, oldCsvData := range oldCsvDataArray {
		happenedDate := oldCsvData.HappenedDate.Time.Format(layout)
		insertSql := fmt.Sprintf(insertSqlFormat, happenedDate, oldCsvData.UserId, oldCsvData.ViewCount, oldCsvData.ClickCount, oldCsvData.Spent, oldCsvData.ShouldSpent, oldCsvData.BiddingType, oldCsvData.Balance, oldCsvData.ViewCount, oldCsvData.ClickCount)
		file.WriteString(insertSql)
	}
	file.WriteString("commit;\n")
	logger.Info(context.Background(), "GenerateUserDataDailySummariesSqlFile succeed.")
	return nil
}

func (service *GenerateSqlFileService) GenerateAllUsersDataDailySummariesSqlFile(oldCsvDataArray []*csv.AllUsersDataDailySummariesCsvData, filePath string) error {
	logger.Info(context.Background(), "GenerateAllUsersDataDailySummariesSqlFile started.")
	layout := "2006-01-02"
	// 创建sql文件
	file, err := os.Create(filePath)
	if err != nil {
		logger.Error(context.Background(), "os open filePath : %s, error : %+v", filePath, err)
		return err
	}
	defer file.Close()

	insertSqlFormat := "insert into bp.all_users_data_daily_summaries(happened_date, view_count, click_count, spent, should_spent, bidding_type, balance) values('%s', %d, %d, %d, %d, %d, %d) on duplicate key update view_count = view_count + %d, click_count = click_count + %d;\n"
	file.WriteString("begin;\n")
	for _, oldCsvData := range oldCsvDataArray {
		happenedDate := oldCsvData.HappenedDate.Time.Format(layout)
		insertSql := fmt.Sprintf(insertSqlFormat, happenedDate, oldCsvData.ViewCount, oldCsvData.ClickCount, oldCsvData.Spent, oldCsvData.ShouldSpent, oldCsvData.BiddingType, oldCsvData.Balance, oldCsvData.ViewCount, oldCsvData.ClickCount)
		file.WriteString(insertSql)
	}
	file.WriteString("commit;\n")
	logger.Info(context.Background(), "GenerateAllUsersDataDailySummariesSqlFile succeed.")
	return nil
}

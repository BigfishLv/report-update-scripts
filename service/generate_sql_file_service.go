package service

import (
	"context"
	"fmt"
	"os"
	"report-update-scripts/domain/csv"
	"report-update-scripts/logger"
)

var (
	delimiterChar = "_"
)

type GenerateSqlFileService struct {
}

func NewGenerateSqlFileService() *GenerateSqlFileService {
	return &GenerateSqlFileService{}
}

func (service *GenerateSqlFileService) GenerateAdPvClickCostDailySqlFile(oldCsvDataArray []*csv.AdPvClickCostDailyCsvData, newCsvDataArray []*csv.AdPvClickCostDailyCsvData, filePath string) error {
	logger.Info(context.Background(), "GenerateAdPvClickCostDailySqlFile started.")
	csvDataMap := make(map[string]*csv.AdPvClickCostDailyCsvData)
	layout := "2006-01-02"
	for _, csvData := range newCsvDataArray {
		identify := fmt.Sprintf("%s_%d_%d", csvData.HappenedDate.Time.Format(layout), csvData.CampaignId, csvData.CreativeId)
		csvDataMap[identify] = csvData
	}
	// 创建sql文件
	file, err := os.Create(filePath)
	if err != nil {
		logger.Error(context.Background(), "os open filePath : %s, error : %+v", filePath, err)
		return err
	}
	defer file.Close()

	updateSqlFormat := "update bp.ad_pv_click_cost_daily set view_count = view_count + %d, click_count = click_count + %d where happened_date = '%s' and campaign_id = %d and creative_id = %d;\n"
	insertSqlFormat := "insert into bp.ad_pv_click_cost_daily(happened_date, user_id, campaign_id, creative_id, bidding_type, view_count, click_count, spent, should_spent, user_balance) values('%s', %d, %d, %d, %d, %d, %d, %d, %d, %d);\n"
	file.WriteString("begin;\n")
	for _, oldCsvData := range oldCsvDataArray {
		happenedDate := oldCsvData.HappenedDate.Time.Format(layout)
		identify := fmt.Sprintf("%s_%d_%d", happenedDate, oldCsvData.CampaignId, oldCsvData.CreativeId)
		if _, ok := csvDataMap[identify]; ok {
			updateSql := fmt.Sprintf(updateSqlFormat, oldCsvData.ViewCount, oldCsvData.ClickCount, happenedDate, oldCsvData.CampaignId, oldCsvData.CreativeId)
			file.WriteString(updateSql)
		} else {
			insertSql := fmt.Sprintf(insertSqlFormat, happenedDate, oldCsvData.UserId, oldCsvData.CampaignId, oldCsvData.CreativeId, oldCsvData.BiddingType, oldCsvData.ViewCount, oldCsvData.ClickCount, oldCsvData.Spent, oldCsvData.ShouldSpent, oldCsvData.UserBalance)
			file.WriteString(insertSql)
		}
	}
	file.WriteString("commit;\n")
	logger.Info(context.Background(), "GenerateAdPvClickCostDailySqlFile succeed.")
	return nil
}

func (service *GenerateSqlFileService) GenerateCampaignDataDailySummariesSqlFile(oldCsvDataArray []*csv.CampaignDataDailySummariesCsvData, newCsvDataArray []*csv.CampaignDataDailySummariesCsvData, filePath string) error {
	logger.Info(context.Background(), "GenerateCampaignDataDailySummariesSqlFile started.")
	csvDataMap := make(map[string]*csv.CampaignDataDailySummariesCsvData)
	layout := "2006-01-02"
	for _, csvData := range newCsvDataArray {
		identify := fmt.Sprintf("%s_%d", csvData.HappenedDate.Time.Format(layout), csvData.CampaignId)
		csvDataMap[identify] = csvData
	}
	// 创建sql文件
	file, err := os.Create(filePath)
	if err != nil {
		logger.Error(context.Background(), "os open filePath : %s, error : %+v", filePath, err)
		return err
	}
	defer file.Close()

	updateSqlFormat := "update bp.campaign_data_daily_summaries set view_count = view_count + %d, click_count = click_count + %d where happened_date = '%s' and campaign_id = %d;\n"
	insertSqlFormat := "insert into bp.campaign_data_daily_summaries(happened_date, campaign_id, view_count, click_count, spent, should_spent) values('%s', %d, %d, %d, %d, %d);\n"
	file.WriteString("begin;\n")
	for _, oldCsvData := range oldCsvDataArray {
		happenedDate := oldCsvData.HappenedDate.Time.Format(layout)
		identify := fmt.Sprintf("%s_%d", happenedDate, oldCsvData.CampaignId)
		if _, ok := csvDataMap[identify]; ok {
			updateSql := fmt.Sprintf(updateSqlFormat, oldCsvData.ViewCount, oldCsvData.ClickCount, happenedDate, oldCsvData.CampaignId)
			file.WriteString(updateSql)
		} else {
			insertSql := fmt.Sprintf(insertSqlFormat, happenedDate, oldCsvData.CampaignId, oldCsvData.ViewCount, oldCsvData.ClickCount, oldCsvData.Spent, oldCsvData.ShouldSpent)
			file.WriteString(insertSql)
		}
	}
	file.WriteString("commit;\n")
	logger.Info(context.Background(), "GenerateCampaignDataDailySummariesSqlFile succeed.")
	return nil
}

func (service *GenerateSqlFileService) GenerateCampaignDataSummariesSqlFile(dailyCampaignCsvDataArray []*csv.CampaignDataDailySummariesCsvData, campaignCsvDataArray []*csv.CampaignDataSummariesCsvData, filePath string) error {
	logger.Info(context.Background(), "GenerateCampaignDataSummariesSqlFile started.")
	campaignCsvDataMap := make(map[int64]*csv.CampaignDataSummariesCsvData)
	for _, csvData := range campaignCsvDataArray {
		campaignCsvDataMap[csvData.CampaignId] = csvData
	}

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

	updateSqlFormat := "update bp.campaign_data_summaries set view_count = view_count + %d, click_count = click_count + %d where campaign_id = %d;\n"
	insertSqlFormat := "insert into bp.campaign_data_summaries(campaign_id, view_count, click_count, spent, should_spent) values(%d, %d, %d, %d, %d);\n"
	file.WriteString("begin;\n")
	for campaignId, campaignData := range dailyCampaignCsvDataMap {
		if _, ok := campaignCsvDataMap[campaignId]; ok {
			updateSql := fmt.Sprintf(updateSqlFormat, campaignData.ViewCount, campaignData.ClickCount, campaignId)
			file.WriteString(updateSql)
		} else {
			insertSql := fmt.Sprintf(insertSqlFormat, campaignData.CampaignId, campaignData.ViewCount, campaignData.ClickCount, campaignData.Spent, campaignData.ShouldSpent)
			file.WriteString(insertSql)
		}
	}
	file.WriteString("commit;\n")
	logger.Info(context.Background(), "GenerateCampaignDataSummariesSqlFile succeed.")
	return nil
}

func (service *GenerateSqlFileService) GenerateCreativeDataDailySummariesSqlFile(oldCsvDataArray []*csv.CreativeDataDailySummariesCsvData, newCsvDataArray []*csv.CreativeDataDailySummariesCsvData, filePath string) error {
	logger.Info(context.Background(), "GenerateCreativeDataDailySummariesSqlFile started.")
	csvDataMap := make(map[string]*csv.CreativeDataDailySummariesCsvData)
	layout := "2006-01-02"
	for _, csvData := range newCsvDataArray {
		identify := fmt.Sprintf("%s_%d", csvData.HappenedDate.Time.Format(layout), csvData.CreativeId)
		csvDataMap[identify] = csvData
	}
	// 创建sql文件
	file, err := os.Create(filePath)
	if err != nil {
		logger.Error(context.Background(), "os open filePath : %s, error : %+v", filePath, err)
		return err
	}
	defer file.Close()

	updateSqlFormat := "update bp.creative_data_daily_summaries set view_count = view_count + %d, click_count = click_count + %d where happened_date = '%s' and creative_id = %d;\n"
	insertSqlFormat := "insert into bp.creative_data_daily_summaries(happened_date, creative_id, view_count, click_count, spent, should_spent) values('%s', %d, %d, %d, %d, %d);\n"
	file.WriteString("begin;\n")
	for _, oldCsvData := range oldCsvDataArray {
		happenedDate := oldCsvData.HappenedDate.Time.Format(layout)
		identify := fmt.Sprintf("%s_%d", happenedDate, oldCsvData.CreativeId)
		if _, ok := csvDataMap[identify]; ok {
			updateSql := fmt.Sprintf(updateSqlFormat, oldCsvData.ViewCount, oldCsvData.ClickCount, happenedDate, oldCsvData.CreativeId)
			file.WriteString(updateSql)
		} else {
			insertSql := fmt.Sprintf(insertSqlFormat, happenedDate, oldCsvData.CreativeId, oldCsvData.ViewCount, oldCsvData.ClickCount, oldCsvData.Spent, oldCsvData.ShouldSpent)
			file.WriteString(insertSql)
		}
	}
	file.WriteString("commit;\n")
	logger.Info(context.Background(), "GenerateCreativeDataDailySummariesSqlFile succeed.")
	return nil
}

func (service *GenerateSqlFileService) GenerateCreativeDataSummariesSqlFile(dailyCreativeCsvDataArray []*csv.CreativeDataDailySummariesCsvData, creativeCsvDataArray []*csv.CreativeDataSummariesCsvData, filePath string) error {
	logger.Info(context.Background(), "GenerateCreativeDataSummariesSqlFile started.")
	creativeCsvDataMap := make(map[int64]*csv.CreativeDataSummariesCsvData)
	for _, csvData := range creativeCsvDataArray {
		creativeCsvDataMap[csvData.CreativeId] = csvData
	}

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

	updateSqlFormat := "update bp.creative_data_summaries set view_count = view_count + %d, click_count = click_count + %d where creative_id = %d;\n"
	insertSqlFormat := "insert into bp.creative_data_summaries(creative_id, view_count, click_count, spent, should_spent) values(%d, %d, %d, %d, %d);\n"
	file.WriteString("begin;\n")
	for creativeId, creativeData := range dailyCreativeCsvDataMap {
		if _, ok := creativeCsvDataMap[creativeId]; ok {
			updateSql := fmt.Sprintf(updateSqlFormat, creativeData.ViewCount, creativeData.ClickCount, creativeId)
			file.WriteString(updateSql)
		} else {
			insertSql := fmt.Sprintf(insertSqlFormat, creativeData.CreativeId, creativeData.ViewCount, creativeData.ClickCount, creativeData.Spent, creativeData.ShouldSpent)
			file.WriteString(insertSql)
		}
	}
	file.WriteString("commit;\n")
	logger.Info(context.Background(), "GenerateCreativeDataSummariesSqlFile succeed.")
	return nil
}

func (service *GenerateSqlFileService) GenerateUserDataDailySummariesSqlFile(oldCsvDataArray []*csv.UserDataDailySummariesCsvData, newCsvDataArray []*csv.UserDataDailySummariesCsvData, filePath string) error {
	logger.Info(context.Background(), "GenerateUserDataDailySummariesSqlFile started.")
	csvDataMap := make(map[string]*csv.UserDataDailySummariesCsvData)
	layout := "2006-01-02"
	for _, csvData := range newCsvDataArray {
		identify := fmt.Sprintf("%s_%d_%d", csvData.HappenedDate.Time.Format(layout), csvData.UserId, csvData.BiddingType)
		csvDataMap[identify] = csvData
	}
	// 创建sql文件
	file, err := os.Create(filePath)
	if err != nil {
		logger.Error(context.Background(), "os open filePath : %s, error : %+v", filePath, err)
		return err
	}
	defer file.Close()

	updateSqlFormat := "update bp.user_data_daily_summaries set view_count = view_count + %d, click_count = click_count + %d where happened_date = '%s' and user_id = %d and bidding_type = %d;\n"
	insertSqlFormat := "insert into bp.user_data_daily_summaries(happened_date, user_id, view_count, click_count, spent, should_spent, bidding_type, balance) values('%s', %d, %d, %d, %d, %d, %d, %d);\n"
	file.WriteString("begin;\n")
	for _, oldCsvData := range oldCsvDataArray {
		happenedDate := oldCsvData.HappenedDate.Time.Format(layout)
		identify := fmt.Sprintf("%s_%d_%d", happenedDate, oldCsvData.UserId, oldCsvData.BiddingType)
		if _, ok := csvDataMap[identify]; ok {
			updateSql := fmt.Sprintf(updateSqlFormat, oldCsvData.ViewCount, oldCsvData.ClickCount, happenedDate, oldCsvData.UserId, oldCsvData.BiddingType)
			file.WriteString(updateSql)
		} else {
			insertSql := fmt.Sprintf(insertSqlFormat, happenedDate, oldCsvData.UserId, oldCsvData.ViewCount, oldCsvData.ClickCount, oldCsvData.Spent, oldCsvData.ShouldSpent, oldCsvData.BiddingType, oldCsvData.Balance)
			file.WriteString(insertSql)
		}
	}
	file.WriteString("commit;\n")
	logger.Info(context.Background(), "GenerateUserDataDailySummariesSqlFile succeed.")
	return nil
}

func (service *GenerateSqlFileService) GenerateAllUsersDataDailySummariesSqlFile(oldCsvDataArray []*csv.AllUsersDataDailySummariesCsvData, newCsvDataArray []*csv.AllUsersDataDailySummariesCsvData, filePath string) error {
	logger.Info(context.Background(), "GenerateAllUsersDataDailySummariesSqlFile started.")
	csvDataMap := make(map[string]*csv.AllUsersDataDailySummariesCsvData)
	layout := "2006-01-02"
	for _, csvData := range newCsvDataArray {
		identify := fmt.Sprintf("%s_%d", csvData.HappenedDate.Time.Format(layout), csvData.BiddingType)
		csvDataMap[identify] = csvData
	}
	// 创建sql文件
	file, err := os.Create(filePath)
	if err != nil {
		logger.Error(context.Background(), "os open filePath : %s, error : %+v", filePath, err)
		return err
	}
	defer file.Close()

	updateSqlFormat := "update bp.all_users_data_daily_summaries set view_count = view_count + %d, click_count = click_count + %d where happened_date = '%s' and bidding_type = %d;\n"
	insertSqlFormat := "insert into bp.all_users_data_daily_summaries(happened_date, view_count, click_count, spent, should_spent, bidding_type, balance) values('%s', %d, %d, %d, %d, %d, %d);\n"
	file.WriteString("begin;\n")
	for _, oldCsvData := range oldCsvDataArray {
		happenedDate := oldCsvData.HappenedDate.Time.Format(layout)
		identify := fmt.Sprintf("%s_%d", happenedDate, oldCsvData.BiddingType)
		if _, ok := csvDataMap[identify]; ok {
			updateSql := fmt.Sprintf(updateSqlFormat, oldCsvData.ViewCount, oldCsvData.ClickCount, happenedDate, oldCsvData.BiddingType)
			file.WriteString(updateSql)
		} else {
			insertSql := fmt.Sprintf(insertSqlFormat, happenedDate, oldCsvData.ViewCount, oldCsvData.ClickCount, oldCsvData.Spent, oldCsvData.ShouldSpent, oldCsvData.BiddingType, oldCsvData.Balance)
			file.WriteString(insertSql)
		}
	}
	file.WriteString("commit;\n")
	logger.Info(context.Background(), "GenerateAllUsersDataDailySummariesSqlFile succeed.")
	return nil
}

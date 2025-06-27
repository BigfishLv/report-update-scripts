package csv

import "time"

type AdPvClickCostDailyCsvData struct {
	HappenedDate CustomDate `csv:"happened_date"`
	UserId       int64      `csv:"user_id"`
	CampaignId   int64      `csv:"campaign_id"`
	CreativeId   int64      `csv:"creative_id"`
	BiddingType  int32      `csv:"bidding_type"`
	ViewCount    int64      `csv:"view_count"`
	ClickCount   int64      `csv:"click_count"`
	Spent        int64      `csv:"spent"`
	ShouldSpent  int64      `csv:"should_spent"`
	UserBalance  int64      `csv:"user_balance"`
	Version      int64      `csv:"version"`
	CreatedAt    CustomTime `csv:"created_at"`
	UpdatedAt    CustomTime `csv:"updated_at"`
}

type CampaignDataDailySummariesCsvData struct {
	HappenedDate CustomDate `csv:"happened_date"`
	CampaignId   int64      `csv:"campaign_id"`
	ViewCount    int64      `csv:"view_count"`
	ClickCount   int64      `csv:"click_count"`
	Spent        int64      `csv:"spent"`
	ShouldSpent  int64      `csv:"should_spent"`
	Version      int64      `csv:"version"`
	CreatedAt    CustomTime `csv:"created_at"`
	UpdatedAt    CustomTime `csv:"updated_at"`
}

type CampaignDataSummariesCsvData struct {
	CampaignId  int64      `csv:"campaign_id"`
	ViewCount   int64      `csv:"view_count"`
	ClickCount  int64      `csv:"click_count"`
	Spent       int64      `csv:"spent"`
	ShouldSpent int64      `csv:"should_spent"`
	Version     int64      `csv:"version"`
	CreatedAt   CustomTime `csv:"created_at"`
	UpdatedAt   CustomTime `csv:"updated_at"`
}

type CreativeDataDailySummariesCsvData struct {
	HappenedDate CustomDate `csv:"happened_date"`
	CreativeId   int64      `csv:"creative_id"`
	ViewCount    int64      `csv:"view_count"`
	ClickCount   int64      `csv:"click_count"`
	Spent        int64      `csv:"spent"`
	ShouldSpent  int64      `csv:"should_spent"`
	Version      int64      `csv:"version"`
	CreatedAt    CustomTime `csv:"created_at"`
	UpdatedAt    CustomTime `csv:"updated_at"`
}

type CreativeDataSummariesCsvData struct {
	CreativeId  int64      `csv:"creative_id"`
	ViewCount   int64      `csv:"view_count"`
	ClickCount  int64      `csv:"click_count"`
	Spent       int64      `csv:"spent"`
	ShouldSpent int64      `csv:"should_spent"`
	Version     int64      `csv:"version"`
	CreatedAt   CustomTime `csv:"created_at"`
	UpdatedAt   CustomTime `csv:"updated_at"`
}

type UserDataDailySummariesCsvData struct {
	HappenedDate CustomDate `csv:"happened_date"`
	UserId       int64      `csv:"user_id"`
	ViewCount    int64      `csv:"view_count"`
	ClickCount   int64      `csv:"click_count"`
	Spent        int64      `csv:"spent"`
	ShouldSpent  int64      `csv:"should_spent"`
	BiddingType  int32      `csv:"bidding_type"`
	Balance      int64      `csv:"balance"`
	Version      int64      `csv:"version"`
	CreatedAt    CustomTime `csv:"created_at"`
	UpdatedAt    CustomTime `csv:"updated_at"`
}

type AllUsersDataDailySummariesCsvData struct {
	HappenedDate CustomDate `csv:"happened_date"`
	ViewCount    int64      `csv:"view_count"`
	ClickCount   int64      `csv:"click_count"`
	Spent        int64      `csv:"spent"`
	ShouldSpent  int64      `csv:"should_spent"`
	BiddingType  int32      `csv:"bidding_type"`
	Balance      int64      `csv:"balance"`
	Version      int64      `csv:"version"`
	CreatedAt    CustomTime `csv:"created_at"`
	UpdatedAt    CustomTime `csv:"updated_at"`
}

// 自定义类型
type CustomDate struct {
	Time time.Time
}

// 实现 CSV 反序列化接口
func (ct *CustomDate) UnmarshalCSV(text string) error {
	t, err := time.Parse("2006-01-02", text)
	if err != nil {
		return err
	}
	ct.Time = t
	return nil
}

// 自定义类型
type CustomTime struct {
	Time time.Time
}

// 实现 CSV 反序列化接口
func (ct *CustomTime) UnmarshalCSV(csv string) error {
	t, err := time.Parse("2006-01-02 15:04:05", csv) // 匹配日志中的时间格式
	if err != nil {
		return err
	}
	ct.Time = t
	return nil
}

--  以下是老数据库导出数据
mysql -uroot -p -e "SELECT happened_date, user_id, campaign_id, creative_id, bidding_type, view_count, click_count, spent, should_spent, user_balance, version, created_at, updated_at FROM bp.ad_pv_click_cost_daily where happened_date >= '2025-06-24' and happened_date <= '2025-06-25' order by happened_date, campaign_id, creative_id"  | sed 's/\t/,/g' > old_ad_pv_click_cost_daily.csv

mysql -uroot -p -e "SELECT campaign_id, happened_date, view_count, click_count, spent, should_spent, version, created_at, updated_at FROM bp.campaign_data_daily_summaries where happened_date >= '2025-06-24' and happened_date <= '2025-06-25' order by happened_date, campaign_id"  | sed 's/\t/,/g' > old_campaign_data_daily_summaries.csv

mysql -uroot -p -e "SELECT campaign_id, view_count, click_count, spent, should_spent, version, created_at, updated_at FROM bp.campaign_data_summaries order by campaign_id" | sed 's/\t/,/g' > old_campaign_data_summaries.csv

mysql -uroot -p -e "SELECT creative_id, happened_date, view_count, click_count, spent, should_spent, version, created_at, updated_at FROM bp.creative_data_daily_summaries where happened_date >= '2025-06-24' and happened_date <= '2025-06-25' order by happened_date, creative_id"  | sed 's/\t/,/g' > old_creative_data_daily_summaries.csv

mysql -uroot -p -e "SELECT creative_id, view_count, click_count, spent, should_spent, version, created_at, updated_at FROM bp.creative_data_summaries order by creative_id"  | sed 's/\t/,/g' > old_creative_data_summaries.csv

mysql -uroot -p -e "SELECT user_id, happened_date, view_count, click_count, spent, should_spent, bidding_type, balance, version, created_at, updated_at FROM bp.user_data_daily_summaries where happened_date >= '2025-06-24' and happened_date <= '2025-06-25' order by happened_date, user_id, bidding_type"  | sed 's/\t/,/g' > old_user_data_daily_summaries.csv

mysql -uroot -p -e "SELECT happened_date, view_count, click_count, spent, should_spent, bidding_type, balance, version, created_at, updated_at FROM bp.all_users_data_daily_summaries where happened_date >= '2025-06-24' and happened_date <= '2025-06-25' order by happened_date, bidding_type"  | sed 's/\t/,/g' > old_all_users_data_daily_summaries.csv


--  以下是新数据库导出数据
mysql -uroot -p -e "SELECT happened_date, user_id, campaign_id, creative_id, bidding_type, view_count, click_count, spent, should_spent, user_balance, version, created_at, updated_at FROM bp.ad_pv_click_cost_daily where happened_date >= '2025-06-24' and happened_date <= '2025-06-25' order by happened_date, campaign_id, creative_id"  | sed 's/\t/,/g' > new_ad_pv_click_cost_daily.csv

mysql -uroot -p -e "SELECT campaign_id, happened_date, view_count, click_count, spent, should_spent, version, created_at, updated_at FROM bp.campaign_data_daily_summaries where happened_date >= '2025-06-24' and happened_date <= '2025-06-25' order by happened_date, campaign_id"  | sed 's/\t/,/g' > new_campaign_data_daily_summaries.csv

mysql -uroot -p -e "SELECT campaign_id, view_count, click_count, spent, should_spent, version, created_at, updated_at FROM bp.campaign_data_summaries order by campaign_id" | sed 's/\t/,/g' > new_campaign_data_summaries.csv

mysql -uroot -p -e "SELECT creative_id, happened_date, view_count, click_count, spent, should_spent, version, created_at, updated_at FROM bp.creative_data_daily_summaries where happened_date >= '2025-06-24' and happened_date <= '2025-06-25' order by happened_date, creative_id"  | sed 's/\t/,/g' > new_creative_data_daily_summaries.csv

mysql -uroot -p -e "SELECT creative_id, view_count, click_count, spent, should_spent, version, created_at, updated_at FROM bp.creative_data_summaries order by creative_id"  | sed 's/\t/,/g' > new_creative_data_summaries.csv

mysql -uroot -p -e "SELECT user_id, happened_date, view_count, click_count, spent, should_spent, bidding_type, balance, version, created_at, updated_at FROM bp.user_data_daily_summaries where happened_date >= '2025-06-24' and happened_date <= '2025-06-25' order by happened_date, user_id, bidding_type"  | sed 's/\t/,/g' > new_user_data_daily_summaries.csv

mysql -uroot -p -e "SELECT happened_date, view_count, click_count, spent, should_spent, bidding_type, balance, version, created_at, updated_at FROM bp.all_users_data_daily_summaries where happened_date >= '2025-06-24' and happened_date <= '2025-06-25' order by happened_date, bidding_type"  | sed 's/\t/,/g' > new_all_users_data_daily_summaries.csv
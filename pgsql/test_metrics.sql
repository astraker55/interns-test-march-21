WITH CTE AS (
	SELECT ROW_NUMBER() OVER (PARTITION by country_name) as row_num, 
	data_date, country_name,
	CASE WHEN cases_cnt = 0 THEN 0
	ELSE ROUND((cases_cnt/tests_cnt)*100, 2) 
	END
	as tests_metric
	FROM ydata_detail_main_data 
)
SELECT CTE.* FROM cte WHERE tests_metric >= 30 ORDER BY tests_metric DESC;



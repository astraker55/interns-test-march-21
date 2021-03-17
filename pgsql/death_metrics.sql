WITH CTE AS (
	SELECT ROW_NUMBER() OVER (PARTITION by country_name) as row_num, 
	country_name, data_date, cases_cnt, deaths_cnt
	FROM ydata_detail_main_data 
) SELECT t.country_name, t.data_date, ROUND(t.deaths_test, 2) FROM (SELECT c.country_name, c.data_date, 
					CASE WHEN c.ROW_NUM >= 14 THEN
						(c.deaths_cnt / (SELECT m.cases_cnt+0.0000001 FROM cte m 
									  WHERE c.row_num - m.row_num = 14 
									  AND c.country_name = m.country_name))*100 
						ELSE 0
					END AS deaths_test
					FROM cte c) t WHERE t.deaths_test >= 35 ORDER BY deaths_test DESC;



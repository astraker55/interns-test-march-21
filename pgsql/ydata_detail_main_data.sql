DROP TABLE IF EXISTS ydata_detail_main_data ;
CREATE TABLE ydata_detail_main_data (
	country_name varchar(25),
	data_date timestamp,
	cases_cnt numeric, 
	deaths_cnt numeric, 
	tests_cnt numeric,
	version_dt timestamp
);
WITH CTE AS (/*
	SELECT * FROM ydata_main_data  
	GROUP BY country_name, data_date, cases_cnt, deaths_cnt, tests_cnt, version_dt
	HAVING version_dt =  MAX(version_dt)
	*/
	SELECT country_name, data_date, cases_cnt, 
		deaths_cnt, tests_cnt, MAX(version_dt) OVER (
		PARTITION by version_dt) AS version_dt 
		FROM ydata_main_data
) INSERT INTO ydata_detail_main_data (country_name, data_date, cases_cnt, deaths_cnt, tests_cnt, version_dt)
SELECT country_name, data_date, cases_cnt, deaths_cnt, tests_cnt, version_dt FROM CTE;
DROP TABLE IF EXISTS ydata_main_data;
CREATE TABLE ydata_main_data (
	country_name varchar(25),
	data_date timestamp,
	cases_cnt numeric, 
	deaths_cnt numeric, 
	tests_cnt numeric,
	version_dt timestamp,
	PRIMARY KEY(country_name, data_date, version_dt)
);
CREATE INDEX ON ydata_main_data(version_dt);
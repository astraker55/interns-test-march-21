import requests
from configparser import ConfigParser
import psycopg2
from datetime import datetime, timedelta
import json

conf = ConfigParser()
conf.read('config.ini')
pg_data = conf['DATABASE']
conn = psycopg2.connect(dbname=pg_data['dbname'],
                            user=pg_data['user'], 
                            password=pg_data['password'],
                            host=pg_data['host'])
cursor = conn.cursor()
query_row = 'INSERT INTO ydata_main_data VALUES (%s, %s, %s, %s, %s, %s)'
link = 'https://yastat.net/s3/milab/2020/covid19-stat/data/v7/main_data.json?v=1615889471730'
data = json.loads(requests.get(link).text)['country_comp_struct']
parse_start = datetime.now()
start_day = parse_start-timedelta(weeks=52)
countries = data["cases"].keys()

for country in countries:
    graph_points_cases = data["cases"][country]["data"]
    graph_points_deaths = data["deaths"][country]["data"]
    graph_points_tests = data["tests"][country]["data"]
    for p in range(len(graph_points_cases)):
        try:
            test_value = graph_points_tests[p]["v"]
        except IndexError:
            test_value = None
        day_delta = timedelta(days=graph_points_cases[p]["c"])
        cursor.execute(query_row, (country, start_day+day_delta,
        graph_points_cases[p]["v"],graph_points_deaths[p]["v"],
        test_value, parse_start))
        conn.commit()

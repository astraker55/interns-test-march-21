from app import app
import json
from flask import request
from configparser import ConfigParser
import psycopg2
from psycopg2.extras import RealDictCursor
conf = ConfigParser()
conf.read('config.ini')
pg_data = conf['DATABASE']

@app.route('/get_stat', methods=['GET'])
def get_handler():
    query = """SELECT * FROM ydata_detail_main_data 
    WHERE country_name=%s AND data_date::date = date %s"""
    conn = psycopg2.connect(dbname=pg_data['dbname'],
                            user=pg_data['user'], 
                            password=pg_data['password'],
                            host=pg_data['host'])
    
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    country = request.args.get('country')
    date = request.args.get('date')
    cursor.execute(query,(country,date))
    resp = json.dumps(cursor.fetchall(), default=str)
    cursor.close()
    conn.close()
    return resp

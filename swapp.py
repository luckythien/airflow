
# https://www.pg4e.com/code/swapi.py
# https://www.pg4e.com/code/myutils.py

# If needed:
# https://www.pg4e.com/code/hidden-dist.py
# copy hidden-dist.py to hidden.py
# edit hidden.py and put in your credentials

# python3 swapi.py
# Pulls data from the swapi.py4e.com API and puts it into our swapi table

import psycopg2
import hidden
import time
import requests
import json

# def summary(cur) :
#     total = myutils.queryValue(cur, 'SELECT COUNT(*) FROM swapi;')
#     todo = myutils.queryValue(cur, 'SELECT COUNT(*) FROM swapi WHERE status IS NULL;')
#     good = myutils.queryValue(cur, 'SELECT COUNT(*) FROM swapi WHERE status = 200;')
#     error = myutils.queryValue(cur, 'SELECT COUNT(*) FROM swapi WHERE status != 200;')
#     print(f'Total={total} todo={todo} good={good} error={error}')

# Load the secrets
secrets = hidden.secrets()

conn = psycopg2.connect(host=secrets['host'],
        port=secrets['port'],
        database=secrets['database'],
        user=secrets['user'],
        password=secrets['pass'],
        connect_timeout=3)

cur = conn.cursor()

defaulturl = 'http://pokeapi.co/api/v2/'


sql = 'DROP TABLE IF EXISTS pokeapi CASCADE;'
print(sql)
cur.execute(sql)

sql = '''
CREATE TABLE IF NOT EXISTS pokeapi (id INTEGER, body JSONB);
'''
print(sql)
cur.execute(sql)
for i in range(100):
   url = defaulturl + 'pokemon/' + str(i+1)
   response = requests.get(url)
   text = response.text
   js = json.loads(text)
   sql = 'INSERT INTO pokeapi (id,body) VALUES ( %s,%s );'
   cur.execute(sql, (i,text ))


conn.commit()
cur.close()

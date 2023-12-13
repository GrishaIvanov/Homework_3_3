#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Подключение к API
import requests as req

rate_base = 'BTC'
rate_target = 'RUB'
api_key = 'c3d58ed4a965659aeaa34af99de3ec59'
hist_date = "latest"
url_base = 'https://api.exchangerate.host/live'
url = url_base + hist_date

response = req.get(url, params={'source': rate_base})
data = response.json()


# In[ ]:


# Импорт данных
api_key = 'c3d58ed4a965659aeaa34af99de3ec59'
url = f"http://api.exchangerate.host/timeframe?&start_date=2023-11-01&end_date=2023-11-30"

response = req.get(url, params={'access_key': api_key,
                              'source': "BTC",
                              'currencies': "RUB",
                              'format': 1})
data = response.json()


# In[ ]:


# data
#{'success': True,
# 'terms': 'https://currencylayer.com/terms',
#'privacy': 'https://currencylayer.com/privacy',
# 'timeframe': True,
# 'start_date': '2023-11-01',
# 'end_date': '2023-11-30',
# 'source': 'BTC',
# 'quotes': {'2023-11-01': {'BTCRUB': 3282214.766647},
#  '2023-11-02': {'BTCRUB': 3257798.432841},
#  '2023-11-03': {'BTCRUB': 3187060.953946},
#  '2023-11-04': {'BTCRUB': 3219202.564951},
#  '2023-11-05': {'BTCRUB': 3215804.398563},
#  '2023-11-06': {'BTCRUB': 3247129.291859},
#  '2023-11-07': {'BTCRUB': 3264112.648033},
#  '2023-11-08': {'BTCRUB': 3275275.611136},
#  '2023-11-09': {'BTCRUB': 3366532.203058},
#  '2023-11-10': {'BTCRUB': 3375167.294056},
#  '2023-11-11': {'BTCRUB': 3420068.538218},
#  '2023-11-12': {'BTCRUB': 3415717.410716},
#  '2023-11-13': {'BTCRUB': 3347218.820131},
#  '2023-11-14': {'BTCRUB': 3218042.446235},
#  '2023-11-15': {'BTCRUB': 3389031.698838},
#  '2023-11-16': {'BTCRUB': 3231700.050841},
#  '2023-11-17': {'BTCRUB': 3270386.787239},
#  '2023-11-18': {'BTCRUB': 3269454.675211},
#  '2023-11-19': {'BTCRUB': 3337152.659109},
#  '2023-11-20': {'BTCRUB': 3318390.250654},
#  '2023-11-21': {'BTCRUB': 3169590.802743},
#  '2023-11-22': {'BTCRUB': 3300023.658352},
#  '2023-11-23': {'BTCRUB': 3292552.761317},
#  '2023-11-24': {'BTCRUB': 3342165.265705},
#  '2023-11-25': {'BTCRUB': 3344704.729857},
#  '2023-11-26': {'BTCRUB': 3326701.367558},
#  '2023-11-27': {'BTCRUB': 3322654.600989},
#  '2023-11-28': {'BTCRUB': 3347013.55929},
#  '2023-11-29': {'BTCRUB': 3335462.574596},
#  '2023-11-30': {'BTCRUB': 3389083.881064}}}


# In[ ]:


import psycopg2

pg_hostname = 'localhost'
pg_port = '5432'
pg_username = 'postgres'
pg_pass = 'password'
pg_db = 'exchangedb'

conn = psycopg2.connect(host=pg_hostname,
                        port=pg_port,
                        user=pg_username,
                        password=pg_pass,
                        database=pg_db)
cursor = conn.cursor()

# Создание таблицы в postgres
create_table_query = '''
    CREATE TABLE IF NOT EXISTS btc_rates (
        id SERIAL PRIMARY KEY,
        measurement_date DATE,
        first_currency VARCHAR(3),
        second_currency VARCHAR(3),
        rate FLOAT
    )
'''
cur = conn.cursor()
cur.execute(create_table_query)

# Наполнение таблицы данными
import datetime

data = {
    'success': True,
    'terms': 'https://currencylayer.com/terms',
    'privacy': 'https://currencylayer.com/privacy',
    'timeframe': True,
    'start_date': '2023-11-01',
    'end_date': '2023-11-30',
    'source': 'BTC',
    'quotes': {
    '2023-11-01': {'BTCRUB': 3282214.766647},
    '2023-11-02': {'BTCRUB': 3257798.432841},
    '2023-11-03': {'BTCRUB': 3187060.953946},
    '2023-11-04': {'BTCRUB': 3219202.564951},
    '2023-11-05': {'BTCRUB': 3215804.398563},
    '2023-11-06': {'BTCRUB': 3247129.291859},
    '2023-11-07': {'BTCRUB': 3264112.648033},
    '2023-11-08': {'BTCRUB': 3275275.611136},
    '2023-11-09': {'BTCRUB': 3366532.203058},
    '2023-11-10': {'BTCRUB': 3375167.294056},
    '2023-11-11': {'BTCRUB': 3420068.538218},
    '2023-11-12': {'BTCRUB': 3415717.410716},
    '2023-11-13': {'BTCRUB': 3347218.820131},
    '2023-11-14': {'BTCRUB': 3218042.446235},
    '2023-11-15': {'BTCRUB': 3389031.698838},
    '2023-11-16': {'BTCRUB': 3231700.050841},
    '2023-11-17': {'BTCRUB': 3270386.787239},
    '2023-11-18': {'BTCRUB': 3269454.675211},
    '2023-11-19': {'BTCRUB': 3337152.659109},
    '2023-11-20': {'BTCRUB': 3318390.250654},
    '2023-11-21': {'BTCRUB': 3169590.802743},
    '2023-11-22': {'BTCRUB': 3300023.658352},
    '2023-11-23': {'BTCRUB': 3292552.761317},
    '2023-11-24': {'BTCRUB': 3342165.265705},
    '2023-11-25': {'BTCRUB': 3344704.729857},
    '2023-11-26': {'BTCRUB': 3326701.367558},
    '2023-11-27': {'BTCRUB': 3322654.600989},
    '2023-11-28': {'BTCRUB': 3347013.55929},
    '2023-11-29': {'BTCRUB': 3335462.574596},
    '2023-11-30': {'BTCRUB': 3389083.881064}
    }
}

for date, quote in data['quotes'].items():
    measurement_date = datetime.datetime.strptime(date, '%Y-%m-%d').date()
    rate = quote['BTCRUB']
    
    insert_query = '''
        INSERT INTO btc_rates (measurement_date, first_currency, second_currency, rate)
        VALUES (%s, 'BTC', 'RUB', %s)
    '''
    cur.execute(insert_query, (measurement_date, rate))
    
    conn.commit()
conn.close()


# In[ ]:


import psycopg2

conn = psycopg2.connect(database="exchangedb", user="postgres", password="password", host="localhost", port="5432")
cursor = conn.cursor()

# Рассчет показателей
calculation_query = '''
    SELECT
        MAX(measurement_date),
        MIN(measurement_date),
        MAX(rate),
        MIN(rate),
        AVG(rate),
        MAX(rate) AS last_day_rate
    FROM
        btc_rates
    WHERE
        first_currency = 'BTC' AND
        second_currency = 'RUB' AND
        measurement_date >= DATE('2023-11-01') AND
        measurement_date <= DATE('2023-11-30')
    GROUP BY
        first_currency, second_currency
'''
cursor.execute(calculation_query)
result = cursor.fetchone()

cursor.close()
conn.close()

# Результаты расчетов
max_date = result[0]
min_date = result[1]
max_rate = result[2]
min_rate = result[3]
avg_rate = result[4]
last_day_rate = result[5]


# In[ ]:


import psycopg2

conn = psycopg2.connect(database="exchangedb", user="postgres", password="password", host="localhost", port="5432")
cursor = conn.cursor()

# Создание новой таблицы
create_new_table_query = '''
   CREATE TABLE calculated_values (
    id SERIAL PRIMARY KEY,
    month VARCHAR(7),
    first_currency VARCHAR(4),
    second_currency VARCHAR(4),
    max_date DATE,
    min_date DATE,
    max_rate NUMERIC(13, 6),
    min_rate NUMERIC(13, 6),
    avg_rate NUMERIC(13, 6),
    last_day_rate NUMERIC(13, 6)
);
'''
cursor.execute(create_new_table_query)
conn.commit()

# Перенос рассчитанных показателей в таблицу
insert_calculated_values_query = '''
    INSERT INTO calculated_values (month, first_currency, second_currency, max_date, min_date, max_rate, min_rate, avg_rate, last_day_rate)
    VALUES ('2023-11', 'BTC', 'RUB', %s, %s, %s, %s, %s, %s)
'''
cursor.execute(insert_calculated_values_query, (max_date, min_date, max_rate, min_rate, avg_rate, last_day_rate))
conn.commit()



import pandas as pd
import mysql.connector
from mysql.connector import Error

config = {
    'user':'user',
    'password':'pass',
    'host':'host',
    'database':'name'
}

your_table = 'name_table'
df = pd.read_csv('database/students.csv', sep=',')
print(df.head())

cols = df.columns.tolist()
pandas_type = df.dtypes

def type_scan(target):
    if pd.api.types.is_integer_dtype(target):
        return 'INT'
    elif pd.api.types.is_float_dtype(target):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(target):
        return 'BOOLEN'
    elif pd.api.types.is_datetime64_any_dtype(target):
        return 'DATETIME'
    else:
        return 'TEXT'
    
mysql_types = [type_scan(t) for t in pandas_type]

definitions_cols = ', '.join(f'{col} {target}' for col, target in zip(cols, mysql_types))
sql_create_table = f"CREATE TABLE IF NOT EXISTS {your_table} ({definitions_cols})"

try:
    conn = mysql.connector.connect(**config)
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute(sql_create_table)
        cols_str = ', '.join(cols)
        values_str = ', '.join(['%s'] * len(cols))
        sql_insert = f"INSERT INTO {your_table} ({cols_str}) VALUES ({values_str})"

        for _, row in df.iterrows():
            cursor.execute(sql_insert, tuple(row))

        conn.commit()
        print('Dataset inserted with success!')

except Error as e:
    print(f"Erro in connection MySQL: {e}")
finally:
    if conn.is_connected():
        cursor.close()
        conn.close()
        print('Connection Stopped')
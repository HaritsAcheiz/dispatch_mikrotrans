import pandas as pd
from scipy.spatial import cKDTree
import numpy as np
from time import sleep
from sqlalchemy import create_engine
from urllib.parse import quote
from dotenv import load_dotenv
import os

load_dotenv()

def add_bus_stop(data_df):
    bus_stop = pd.read_csv('bus_stop_ujung.csv')

    # r = 1 degree ~~ 111km, r = 0.00045 ~~ 50m
    # r = 1 degree ~~ 111km, r = 0.0009 ~~ 100m

    ck = cKDTree(bus_stop[['stop_lat', 'stop_lon']].values)
    dataset3 = ck.query_ball_point(data_df[['latitude', 'longitude']].values, r=0.00045, p=2)
    y = []
    for i in range(len(dataset3)):
        try:
            y.append(dataset3[i][0])
        except:
            y.append(np.nan)

    data_df['bus_stop_index'] = y
    data_df = data_df.merge(bus_stop[['stop_id', 'trip_id']], how='left', left_on='bus_stop_index', right_on=bus_stop.index)
    data_df.drop('bus_stop_index', axis=1)
    return data_df

def read_data():
    # delta time antara previous dan current 1 menit
    engine = create_engine(f"{os.getenv('DB_DIALECT')}+{os.getenv('DB_DRIVER')}://{os.getenv('DB_USERNAME')}:%s@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_DATABASE')}" % quote(os.getenv('DB_PASSWORD')))
    sql = """
    SELECT p.id, p.gpsdatetime, b.label, p.latitude, p.longitude , FALSE AS is_current FROM gtfs_realtime.cur_gps AS p LEFT JOIN gtfs_realtime.mtr_bus AS b ON p.id = b.id WHERE p.id BETWEEN 70000 AND 100000
    """
    df_sebelum = pd.read_sql_query(con=engine, sql=sql)
    sleep(60)
    sql = """
    SELECT p.id, p.gpsdatetime, b.label, p.latitude, p.longitude , TRUE AS is_current FROM gtfs_realtime.cur_gps AS p LEFT JOIN gtfs_realtime.mtr_bus AS b ON p.id = b.id WHERE p.id BETWEEN 70000 AND 100000
    """
    df_sesudah = pd.read_sql_query(con=engine, sql=sql)
    df_result = pd.concat([df_sebelum, df_sesudah], ignore_index=True)
    return df_result

def get_dispatch_time(data_df):
    data_df.sort_values(by=['label', 'gpsdatetime'], inplace=True, ignore_index=True)
    shifted_df = data_df.shift().copy()
    data_df['dispatch_time'] = data_df.loc[(data_df['label'].eq(shifted_df['label']) & data_df['stop_id'].isna() & shifted_df['stop_id'].notna()), ['gpsdatetime']]
    data_df['from_stop_id'] = shifted_df['stop_id']
    data_df['from_latitude'] = shifted_df['latitude']
    data_df['from_longitude'] = shifted_df['longitude']
    data_df['trip_id'] = data_df['trip_id']
    print(data_df.loc[data_df['label'] == 'LSG220009', ['label', 'from_stop_id']])
    result = data_df.loc[~pd.isnull(data_df['dispatch_time']), ['id', 'label', 'dispatch_time', 'trip_id', 'from_latitude', 'from_longitude', 'from_stop_id']]
    return result

def load_to_sql(data_df):
    engine = create_engine(
        f"{os.getenv('MDB_DIALECT')}+{os.getenv('MDB_DRIVER')}://{os.getenv('MDB_USERNAME')}:%s@{os.getenv('MDB_HOST')}:{os.getenv('MDB_PORT')}/{os.getenv('MDB_DATABASE')}" % quote(
            os.getenv('MDB_PASSWORD')))
    data_df.to_sql('dispatch', con=engine, if_exists='append', index=False)

def main():
    print('reading gps data...')
    df_gps = read_data()
    print('adding bus stop...')
    df_with_bus_stop = add_bus_stop(df_gps)
    print('getting dispatch time...')
    result = get_dispatch_time(df_with_bus_stop)
    print('loading data...')
    load_to_sql(result)
    print('completed!')

if __name__ == '__main__':
    retries = 0
    while retries < 3:
        try:
            main()
        except Exception as e:
            retries += 1
            print(e)
            sleep(60)
    # main()

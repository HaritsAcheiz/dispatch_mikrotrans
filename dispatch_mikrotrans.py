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
    data_df = data_df.merge(bus_stop[['stop_id']], how='left', left_on='bus_stop_index', right_on=bus_stop.index)
    data_df.drop('bus_stop_index', axis=1)
    return data_df

def read_data():
    # delta time antara previous dan current 1 menit
    engine = create_engine(f"{os.getenv('DB_DIALECT')}+{os.getenv('DB_DRIVER')}://{os.getenv('DB_USERNAME')}:%s@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_DATABASE')}" % quote(os.getenv('DB_PASSWORD')))
    sql = """
    SELECT p.gpsdatetime, b.label, p.latitude, p.longitude , FALSE AS is_current FROM gtfs_realtime.cur_gps AS p LEFT JOIN gtfs_realtime.mtr_bus AS b ON p.id = b.id WHERE p.id BETWEEN 70000 AND 100000
    """
    df_sebelum = pd.read_sql_query(con=engine, sql=sql)
    print('previous_data_collected!')
    print('wait for 1 minute...')
    sleep(60)
    sql = """
    SELECT p.gpsdatetime, b.label, p.latitude, p.longitude , TRUE AS is_current FROM gtfs_realtime.cur_gps AS p LEFT JOIN gtfs_realtime.mtr_bus AS b ON p.id = b.id WHERE p.id BETWEEN 70000 AND 100000
    """
    df_sesudah = pd.read_sql_query(con=engine, sql=sql)
    df_result = pd.concat([df_sebelum, df_sesudah], ignore_index=True)
    print('current data collected!')
    return df_result

def get_dispatch_time(data_df):
    data_df.sort_values(by=['label','gpsdatetime'], inplace=True)
    data_df['dispatch_time'] = data_df.loc[data_df['label'].eq(data_df['label'].shift().append(
    data_df.iloc[[-1]]).reset_index(drop = True)) & data_df['stop_id'].isna() & data_df['stop_id'].shift().append(
    data_df.iloc[[-1]]).reset_index(drop = True).notna(), ['gpsdatetime']]
    data_df['from_stop_id'] = data_df['stop_id'].shift()
    print(data_df.loc[~pd.isnull(data_df['dispatch_time']), ['label', 'dispatch_time', 'from_stop_id']])


def main():
    print('reading...')
    df_gps = read_data()
    print('adding bus stop...')
    df_with_bus_stop = add_bus_stop(df_gps)
    print('getting dispatch time...')
    result = get_dispatch_time(df_with_bus_stop)

if __name__ == '__main__':
    retries = 0
    while retries < 3:
        try:
            main()
        except:
            retries += 1
            sleep(1800)

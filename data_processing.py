"""
This script connects to MongoDB, fetches market data, processes it, and outputs clean data. (gen, en, ru, rd)
Author: Chelsie Hu
Date: 2024-2025
"""
from pymongo import MongoClient
import pandas as pd
import warnings
import pytz
from tqdm import tqdm
from functools import reduce
from config import *

warnings.filterwarnings("ignore")


# ------------------------------
# Database Utilities
# ------------------------------
def connect_to_mongodb(db_username, db_password, database):
    """
    Establish a connection to MongoDB.
    :param db_username: MongoDB username
    :param db_password: MongoDB password
    :param database: Database name
    :return: MongoDB database object
    """
    connection_string = f"mongodb+srv://{db_username}:{db_password}@analyticsdata.oxjln.mongodb.net"
    client = MongoClient(connection_string)
    return client[database]


# ------------------------------
# Fetch Data from MongoDB
# ------------------------------
# Get awards data
def get_awards_data(collection, start_date, end_date, market, data_type, product):
    """
    Get data from db = 'market_data'

    :param collection: database collection
    :param market: 'da', 'rt'
    :param data_type: 'award'
    :param product: 'ru', 'rd', 'en'
    """
    result_list = []

    total_days = int((end_date - start_date).total_seconds()//86400)

    current_date = start_date

    with tqdm(total=total_days, desc="Fetching Data", unit="day") as pbar:
        while current_date < end_date:
            query = {
                    '_id.market': market,
                    '_id.trade_date': current_date,
                    '_id.data_type': data_type,
                    '_id.product':product
                }

            query_results = list(collection.find(query))

            for document in query_results:
                trade_date = document["_id"]["trade_date"]

                result_list.append({"trade_date": trade_date, product: document['data']})

            current_date += datetime.timedelta(days = 1)
            pbar.update(1)

    df = pd.DataFrame(result_list)

    # Open the dict in column
    expanded_data = []
    for _, row in df.iterrows():
        en_dict = row[product]

        for i, (timestamp, value) in enumerate(en_dict.items()):
            utc_time = datetime.datetime.utcfromtimestamp(int(timestamp))
            pacific = pytz.timezone("US/Pacific")
            pacific_time = utc_time.replace(tzinfo=pytz.utc).astimezone(pacific)         # In Pacific time

            expanded_data.append({'trade_time': pacific_time, product: value})

    df_expanded = pd.DataFrame(expanded_data)

    return df_expanded

# Get generation data
def get_gen_data(collection, start_date, end_date, device, tag):

    """
    Get generation data from db = 'meter'
    :param device: device id
    :param tag: tag id
    """
    result_list = []
    total_hours = int((end_date - start_date).total_seconds()//3600)

    pacific_tz = pytz.timezone("US/Pacific")
    current_date = pacific_tz.localize(start_date)
    end_date = pacific_tz.localize(end_date)

    current_date = current_date.astimezone(pytz.UTC)
    end_date = end_date.astimezone(pytz.UTC)
    #current_date = start_date
    with tqdm(total=total_hours, desc="Fetching Data", unit="hour") as pbar:
        while current_date<end_date:
            query = {
                    '_id.device': device,
                    '_id.ts_bucket': current_date,
                    '_id.tag': tag
                }

            query_results = list(collection.find(query))

            for document in query_results:
                if 'data' not in list(document.keys()):
                    continue
                else:
                    trade_hour = document["_id"]["ts_bucket"]
                    result_list.append({"trade_hour": trade_hour, 'gen': document['data']})

            current_date += datetime.timedelta(hours = 1)
            pbar.update(1)

    df = pd.DataFrame(result_list)

    expanded_data = []

    for _, row in df.iterrows():
        en_dict = row['gen']

        for i, (timestamp, value) in enumerate(en_dict.items()):
            trade_time = pd.to_datetime(int(timestamp)/1000, unit='s')
            expanded_data.append({'trade_time': trade_time, 'gen': value/1000})           # Transfer to MW

    df_expanded = pd.DataFrame(expanded_data)

    # Aggregate to 5 min
    df_expanded["trade_time"] = pd.to_datetime(df_expanded["trade_time"])
    df_5min = df_expanded.resample("5T", on="trade_time").mean().reset_index()
    df_5min['trade_time'] = pd.to_datetime(df_5min['trade_time'])
    df_5min['trade_time'] = df_5min['trade_time'].dt.tz_localize('UTC')
    df_5min['trade_time'] = df_5min['trade_time'].dt.tz_convert('America/Los_Angeles')         # In Pacific time

    return df_5min

# ------------------------------
# Data Processing
# ------------------------------
def get_all_data(project, gen_db, start_date, end_date, device, tag, project_id):
    # get awards data
    awards_db = connect_to_mongodb(username, password, 'market_data')

    df_ru = get_awards_data(awards_db[project], start_date, end_date, 'da', 'award', 'ru')
    df_rd = get_awards_data(awards_db[project], start_date, end_date, 'da', 'award', 'rd')
    df_en = get_awards_data(awards_db[project], start_date, end_date, 'rt', 'award', 'en')

    # get generation data
    gen_db = connect_to_mongodb(username, password, gen_db)          # 'coso', 'saticoy', 'data'
    df_gen = get_gen_data(gen_db['meter'], start_date, end_date, device, tag)

    # get fmm price data
    price_db = connect_to_mongodb(username, password, 'price_forecast')

    # Put together
    dfs = [df_gen, df_en, df_ru, df_rd]

    df = (reduce(lambda left, right: pd.merge(left, right, on='trade_time', how = 'outer'), dfs))
    df[['ru', 'rd']] = df[['ru','rd']].fillna(method='ffill')
    #df = df.dropna()
    df['trade_time'] = pd.to_datetime(df['trade_time'])
    df['trade_time'] = df['trade_time'].dt.tz_localize(None)

    #If condor, generation is stored as negative
    #if project == 'condor':
     #   df['gen'] = -df['gen']

    return df

# Calculate capped ru signals
def calculate_ru_signal(row):
    # Calculate ru_signal
    if row["ru"] == 0:
        ru_signal = ""
    elif row["ru"] > 0:
        ru_signal = (row["gen"] - row["en"]) / row["ru"]
    else:
        ru_signal = ""

    # Cap ru_signal between [0,1]
    if ru_signal == "":
        return ""
    try:
        ru_signal = float(ru_signal)  # Convert to float
        return max(0, min(ru_signal, 1))  # Cap between [0,1]
    except ValueError:
        return ""  # Handle non-numeric empty values

# Calculate capped rd signals
def calculate_rd_signal(row):
    # Calculate rd_signal
    if row["rd"] == 0:
        rd_signal = ""
    elif row["rd"] > 0:
        rd_signal = (row["en"] - row["gen"]) / row["rd"]
    else:
        rd_signal = ""

    # Cap ru_signal between [0,1]
    if rd_signal == "":
        return ""
    try:
        rd_signal = float(rd_signal)  # Convert to float
        return max(0, min(rd_signal, 1))  # Cap between [0,1]
    except ValueError:
        return ""  # Handle non-numeric empty values

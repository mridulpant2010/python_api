from __future__ import division
import sys
import math
import numpy as np
import pandas as pd

import pyspark
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
import pyspark.sql.functions as fn
from pyspark import SparkContext, SparkConf
from preprocessing import ad_preprocessing_algo as ad
from pysparklogger import logger
from preprocessing.helpers import read_file, read_arv, write_arv, make_action
import argparse
# library to access the properties file
from configparser import ConfigParser
import requests
import yaml

def main():
    try:
        """
        Main function to read input arguments, read required data and call process and detect anomaly
        reading system arguments with argparse
        """
        parser = argparse.ArgumentParser(description="Arguments for Anomaly Detection")
        parser.add_argument('arv_path', help='Path to store intermediate results')
        parser.add_argument('job_name', help='Job Name')
        parser.add_argument('src_path', help='Path to read source file')
        parser.add_argument('dest_path', help='Path to write results')
        parser.add_argument('groupby_list', help='Aggregator level', action=make_action(['comma_split']))
        parser.add_argument('ad_col', help='Target column for anomaly detection')
        parser.add_argument('consider_lower_limit', help='Boolean for Lower limits')
        parser.add_argument('consider_upper_limit', help='Boolean for Upper limits')
        parser.add_argument('strictness', help='Strictness factor', action=make_action(['comma_split', 'float_list']))
        parser.add_argument('seasonal_period', help='Seasonal period', action=make_action(['int']))
        parser.add_argument('anomaly_algorithm', help='List of anomaly algorithms', action=make_action(['comma_split']), default='HW,STL')
        parser.add_argument('anomaly_app_properties', help='Anomaly Application properties file path', nargs='?')
        parser.add_argument('reprocess_date', help='Reprocess Date',  nargs='?', action=make_action(['blank_test']), default= '')
        parser.add_argument('table_name', help='Table Name', nargs='?',action=make_action(['blank_test']), default= '')
        parser.add_argument('input_file_format', help='List of anomaly algorithms', nargs='?', default='orc')

        config = vars(parser.parse_args())
        # Support for two different input file format: 1.orc and 2. parquet
        # External algorithms such as ADX anomaly algorithm:
        config['external_algorithm'], config['adx_connection_string'] = None, None
        # In order to extract connection string

        # Set Spark context
        sc = SparkContext.getOrCreate()
        # obtaining the spark session
        spark = SparkSession.builder.getOrCreate()
        # arrow execution disabled for spark versions lower than 2.3
        # spark.conf.set("spark.sql.execution.arrow.enabled", "true")

        # Encoding is done b/c, the result is a unicode string
        config['spark_version'] = str(sc.version)

        if 'RM' in config['anomaly_algorithm']:
            config['anomaly_algorithm'].remove('RM') # removal due to the implementation residing in scala

        logger.info("Python: Input Configuration - %s", config)
    except Exception as e:
        logger.info("Python: Failed with error : %s", e)
        raise ValueError('****Insufficient input arguments for Python script****')

    # Initialize paths to store intermediate results
    # 1. arv_job_path: Anomaly Range Value results are stored in this location
    # 2. next_limits_path: Anomaly next predictions and the parameters are stored in this location
    appender = '_' + ''.join(config['groupby_list'])
    next_appender = appender + '_anomaly_limits'
    arv_job_path = config['arv_path'] + '/' + config['job_name'] + appender
    next_limits_path = config['arv_path'] + '/' + config['job_name'] + next_appender

    # Initialize column join list
    on_join_list = config['groupby_list'] + [config['ad_col']]

    # Read input and arv files
    input_data = read_file(spark, config, logger)

    # check for reprocess request
    # Need to modify the read function, to read from arv table than the file

    if config['reprocess_date'] is None:
        # Reading the Intermediate results for incremental run
        try:
            arv_stl_hw = read_arv(spark, arv_job_path, logger, 'ANOMALY_RANGE_VALUES')
            arv_stl_hw = arv_stl_hw[on_join_list]  # CHECKPOINT: Column subset eliminates the UPPER LIMIT LOWER LIMIT columns
            next_limits_df = read_arv(spark, next_limits_path, logger, 'PREDICTED_LIMITS')
            arv_present = True
        except Exception as e:
            arv_present = False
            arv_stl_hw, next_limits_df = [pd.DataFrame() for _ in range(2)]
            logger.info("Check for Incremental job or batch %s", e)
    elif config['reprocess_date'] is not None:
        """
        Mysql credentials are access with the below two step process
        1. Read anomaly_application.properties file from the path and get cfs url 
        2. Read yaml file in the cfs to get mysql credentials
        """

        # 1. Read anomaly_application.properties
        app_file = sc.textFile(config['anomaly_app_properties']).collect()
        anomaly_app_properties = '[global_section]\n' + "\n".join(app_file)

        property_parser = ConfigParser()
        property_parser.read_string(anomaly_app_properties)

        # 1.i Get cfs url
        cfs_url = str(property_parser['global_section']['cfs.url'])

        # 2. Read yaml file in the cfs
        res = requests.get(cfs_url).content
        data_map = yaml.safe_load(res)
        mysql_config_dict = data_map['dqp'].get('mysql').get('anomaly_db')

        # 2.i form connection string to access respective mysql table
        table_name = config['table_name']
        jdbc_user_name, jdbc_password, jdbc_driver, jdbc_url = [mysql_config_dict.get(key) for key in ['username', 'password', 'driver', 'url']]
        db_properties = {"user": jdbc_user_name, "password": jdbc_password, "driver": jdbc_driver}

        # Fetch min and match dates of previously stored anomaly range values
        date_col = config['groupby_list'][0]

        anomaly_range_values = spark.read.jdbc(url=jdbc_url, table=table_name, properties=db_properties)
        min_date, max_date = anomaly_range_values.select(fn.min(date_col), fn.max(date_col)).first()
        min_date, max_date = str(min_date), str(max_date)

        # Check if the reprocess request date is less than the ARV's min date
        if config['reprocess_date'] <= min_date:
            # consider the current request as first batch run
            arv_present = False
            arv_stl_hw, next_limits_df = [pd.DataFrame() for _ in range(2)]
        elif min_date < config['reprocess_date'] <= max_date:
            # consider the current request as incremental run
            arv_stl_hw = anomaly_range_values.filter(anomaly_range_values[date_col] < config['reprocess_date']).toPandas()
            arv_stl_hw = arv_stl_hw[on_join_list]  # CHECKPOINT: Column subset eliminates the UPPER LIMIT LOWER LIMIT columns
            predicted_limits_df = spark.read.orc(next_limits_path)
            next_limits_df = predicted_limits_df.filter(predicted_limits_df[date_col] < config['reprocess_date']).toPandas()

            if arv_stl_hw.empty:
                arv_present = False
            else:
                if next_limits_df.empty:
                    next_limits_df = predicted_limits_df.toPandas()
                arv_present = True
        else:
            raise ValueError('Reprocess request is for an invalid date')

    """
    Input parameters to detect anomaly
    arv_present: flag for incremental run
    input_data:  source data
    on_join_list: key to join the input and the arv dataframe
    arv_stl_hw: Intermediate results from previous run
    config: input configuration
    """
    anomaly_result, arv_result, predicted_limits = ad.process_detect_anomaly(arv_present, input_data, on_join_list,
                                                                             arv_stl_hw, next_limits_df, config, logger)
    logger.info("Python: Anomaly Detection process complete")

    # Anomaly result written to Hive in ORC format
    write_arv(spark, anomaly_result, config['dest_path'], logger, 'ANOMALY_RESULT')

    # Anomaly Range Values written to Hive in ORC format
    write_arv(spark, arv_result, arv_job_path, logger, 'ANOMALY_RANGE_VALUES')

    # Predictions and next limits are written to Hive in ORC format
    write_arv(spark, predicted_limits, next_limits_path, logger, 'PREDICTED_LIMITS')


if __name__ == '__main__':
    logger = logger.YarnLogger()
    logger.info("Python: Imported logger")
    main()

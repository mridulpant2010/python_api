import psycopg2, json, requests, time, sys
import pandas as pd, numpy
from requests_oauthlib import OAuth2Session
from datetime import datetime, timedelta
from pyutil import logger as lg
from pyutil import oauth
from pyutil import hawq_operations as hops
from pyutil import file_operations as fo
from datetime import datetime
from dateutil.relativedelta import *
import datetime as dt

import sys
sys.path.append("/usr/local/share/applications/hdw/lib")
from environment_var_loader import *


def get_db_connection():
    print ("getting db con")
    logger.info("[DM_ADOBE_ANALYTICS_EXECUTIVE_INFO]: getting DB connection.")
    # get db connection
    try:
        # obtain db connection and cursor to be reused
        con = hops.getconnection(hawq_host, hawq_username, hawq_database, hawq_password, logger)
        return con.cursor()
    except Exception as ex:
        logger.error("[DM_ADOBE_ANALYTICS_EXECUTIVE_ERROR]: Error in obtaining DB connection: " + str(ex))
        exit(-1)

""" pull out country dimension from db, persist in a dataframe """

def fetch_country_dim_from_db():
    try:
        cur.execute(conf.get("country_dim_query"))
        c_dim_df = cur.fetchall()
        logger.info("[DM_ADOBE_ANALYTICS_EXECUTIVE_INFO]: prepared country dim dataframe")
        return pd.DataFrame(c_dim_df, columns={'country_geo', 'country_itemID'})
        
        # pd.DataFrame(c_dim_df)
        # c_dim_df['country_lower'] = c_dim_df['iso_country_name'].str.lower()
        # c_dim_df.to_csv(conf.get("output_file_path") + 'country_dim.csv',index=False, sep=',')
        # print c_dim_df
    except psycopg2.Error as ex:
        logger.error("[DM_ADOBE_ANALYTICS_EXECUTIVE_ERROR]: error while getting max date from attribution fact: " + str(ex))
        return None


def parse_JSON_with_rows(payload):
    try:
        response_df = pd.read_json(payload, typ='series')
        rows_df = pd.DataFrame(response_df['rows'])
        #print rows_df
        return rows_df
    except Exception as ex:
        logger.error("[DM_ADOBE_ANALYTICS_EXECUTIVE_ERROR]: error while extracting rows info from response json: " + str(ex))
        print("error in rows parsing: " + str(ex))
        exit(-1)


def prepare_final_report(from_date, to_date):

    try:
        for index in range(len(geo_list)):
            df_geo = df_country[df_country['country_geo'] == geo_list[index]]
            print(index, df_geo['country_geo'].unique())
            logger.info("[DM_ADOBE_ANALYTICS_EXECUTIVE_INFO]: running for region "+ geo_list[index] + ", date:" + str(from_date) + " : " + str(to_date))

            final_payload_internal = final_payload
            final_payload_internal = str(final_payload_internal).replace("START_DATE", str(from_date))
            final_payload_internal = str(final_payload_internal).replace("END_DATE", str(to_date))


            geo_country_ids = df_geo['country_itemID']#.astype(basestring)
            geo_country_ids = '","'.join(str(e) for e in geo_country_ids)
            final_payload_internal = str(final_payload_internal).replace("COUNTRY_TOBE_REPLACED", geo_country_ids)
            #import pdb;pdb.set_trace()
            payload = []
            final_payload_internal = str(final_payload_internal).replace("enter_rsid", "vrs_vmware1_vmwareglobaldbbotsex")
            payload.append(final_payload_internal)
            final_payload_internal1 = str(final_payload_internal).replace("vrs_vmware1_vmwareglobaldbbotsex", "vrs_vmware1_digitalonlytraffic")
            payload.append(final_payload_internal1)
            final_payload_internals = str(final_payload_internal1).replace("vrs_vmware1_digitalonlytraffic","vrs_vmware1_qbralltraffic")
            payload.append(final_payload_internals)
            #print(payload)
            
            for i in payload:
                #print(i)
                payload_json = json.loads(i)
                rsid = payload_json['rsid']
    
                final_payload_json = oauth.generate_report(conf.get("report_api"), i,logger).json()
    
                f_df = pd.DataFrame(data=final_payload_json.get("rows"))
    
                flattened_final_report_1 = flatten_final_report(f_df)
                #print(flattened_final_report_1)
                flattened_final_report_1['geo'] = geo_list[index]
                flattened_final_report_1['rsid']=rsid
                
    
                flattened_final_report.append(flattened_final_report_1)
                #print(flattened_final_report_1)
                # if (index == 0):
                #     break

    except Exception as ex:
        logger.error("[DM_ADOBE_ANALYTICS_EXECUTIVE_ERROR]: error while preparing final report: " + str(ex))
        print ("error in preparing final report")




def flatten_primitive(in_df, valuename, itemname):
    try:
        subset_df = in_df[['value', 'itemId']]
        # filter df1 with country_name and country_itemID
        subset_df = subset_df.rename(columns={'value': valuename, 'itemId': itemname})
        #print country_subset_df
        return subset_df
    except Exception as ex:
        logger.error("[DM_ADOBE_ANALYTICS_EXECUTIVE_ERROR]: error while flattening primitive attributes: " + str(ex))


def flatten_final_report(f_df):
    try:
        data_file2_rows_filtered = f_df[['data', 'value']]
        # filter df1 with country_name and country_itemID
        data_file2_rows_transpose = f_df.data.apply(pd.Series).merge(data_file2_rows_filtered, left_index=True, right_index=True).drop(['data'],axis=1)
        return data_file2_rows_transpose
    except Exception as ex:
        logger.error("[DM_ADOBE_ANALYTICS_EXECUTIVE_ERROR]: error while flattening final report: " + str(ex))

def fetch_exec_dashboard_date_range(query):
    try:
        cur.execute(query)
        #cur.execute(conf.get("campaign_dim_date_query"))
        date_dim_df = cur.fetchall()
        logger.info("[DM_ADOBE_ANALYTICS_EXECUTIVE_INFO]: prepared date range for exec dashboard query")
        return date_dim_df

    except psycopg2.Error as ex:
        logger.error("[DM_ADOBE_ANALYTICS_EXECUTIVE_ERROR]: error while getting max date from attribution fact: " + str(ex))
        return None


def fetch_max_date(query):
    try:
        cur.execute(query)
        #cur.execute(conf.get("campaign_dim_date_query"))
        date_dim_df = cur.fetchone()
        logger.info("[DM_ADOBE_ANALYTICS_SUMMARY_INFO]: prepared max date for executive dashboard query")
        return date_dim_df

    except psycopg2.Error as ex:
        logger.error("[DM_ADOBE_ANALYTICS_SUMMARY_ERROR]: error while getting max date from attribution fact: " + str(ex))
        return None


def convert_df_to_desired_format(final_df):

    try:
        my_df_columns_to_rows = final_df.set_index(['FiscalMonth', 'geo','RSID']).stack().reset_index()
        df_of_split_metric_bu_channel = pd.DataFrame(my_df_columns_to_rows.level_3.unique(),columns = ["level_3"])
        df_of_split_metric_bu_channel[["metric", "channel", "BU"]] = df_of_split_metric_bu_channel.level_3.str.split('_', expand=True)
        final_computed_df = df_of_split_metric_bu_channel.merge(my_df_columns_to_rows, how='inner', on="level_3")
        #final_computed_df['RSID'] = final_df['RSID']
        #final_computed_df['RSID'] = final_df['RSID'].apply(lambda x: 'vrs_vmware1_vmwareglobaldbbotsex' if x == 'vrs_vmware1_vmwareglobaldbbotsex' else 'vrs_vmware1_digitalonlytraffic')
        
        #print(final_computed_df)
        return final_computed_df.drop(columns=['level_3'])
    except Exception as ex:
        logger.error("[DM_ADOBE_ANALYTICS_EXECUTIVE_ERROR]: error while converting to desired format " + str(ex))


if __name__ == "__main__":

    # load config
    conf = json.loads(open("../conf/itdsd_hadoop_dm_adobe_analytics_config_executive_rsid.json", "r").read())
    # get logger
    logger = lg.getlogger(conf.get("log_file_path"), conf.get("log_file_name"), conf.get("logger_name"))
    logger.info("[DM_ADOBE_ANALYTICS_EXECUTIVE_INFO]: Starting the process at:" + str(datetime.now()))
    #move exiting files to archive
    #fo.move_files(conf.get("output_file_path"), conf.get("archived_file_path"))
    
    cur = get_db_connection()
    #query_d = conf.get("end_max_date")
    #max_date = fetch_max_date(query_d)
    ##Automation for years
    
    # last_year = 2020
    
    #begin_year = datetime.date(datetime.strptime(max_date[0], '%Y-%m-%d'))
    # end_year = datetime.date(last_year, 12, 31)
    '''   
    end_year = dt.date.today()
    one_day = relativedelta(days=1)
    next_month = begin_year
    start_date = begin_year
    next_month += one_day
    end_date = next_month
    #print(start_date,end_date)
    '''
    #start_date=sys.argv[1]
    #end_date= sys.argv[2]
    #filename = conf.get("output_file_name") + '_' + str(start_date) + '_' + str(end_date) + conf.get("output_file_extn")
    # creating a 0byte touch file as a place holder to starts with
    #flattened_final_report1 = pd.DataFrame()
    #flattened_final_report1.to_csv(conf.get("output_file_path") + filename, index=False, sep=',',header=False)
    
    # get db cursor
    #cur = get_db_connection()
    
    # constants
    geo_list = ['AMER', 'EMEA', 'APAC']
    
    #fiscal_month_name = sys.argv[1] #'FEB-2020'
    #end_date = sys.argv[2] #'2018-12-01'
    
    '''if len(sys.argv) >= 2:
        query = conf.get("date_range_query")
        fiscal_month_name = sys.argv[1]  # 'FEB-2020'
    else:
        query = conf.get("date_range_query_default")
        fiscal_month_name = 'current_date'
    print(fiscal_month_name)'''
    
    #query = query.replace("INPUT_MONTH_VALUE", str(fiscal_month_name))
    '''
    start_date=sys.argv[1]
    end_date=sys.argv[2] 
    '''
    df_dates = fetch_exec_dashboard_date_range(conf.get("date_range_query_default"))

    if (df_dates[0][0]) != None:
        start_date = df_dates[0][0] #  max date in table
        end_date = df_dates[0][1]  #  current  system date
    
    filename = conf.get("output_file_name") + '_' + str(start_date) + '_' + str(end_date) + conf.get("output_file_extn")
    flattened_final_report1 = pd.DataFrame()
    flattened_final_report1.to_csv(conf.get("output_file_path") + filename, index=False, sep=',',header=False)
    print(start_date)
    print(end_date)
    print("getting_country_dim")
    # prepare list of countries by Geo to be used for data extract from Adobe lookup
    df_country = fetch_country_dim_from_db()
    #print(df_country)
    
    final_payload = open('../conf/itdsd_hadoop_dm_adobe_analytics_final_report_executive_rsid.json', 'r').read()
    
    flattened_final_report =[] # pd.DataFrame(columns=conf.get("output_file_title").split(","))
    
    logger.info(
        "[DM_ADOBE_ANALYTICS_EXECUTIVE_INFO]: starting to process data for " +" " + "starting from " + str(
            start_date) + " - " + str(end_date))
    if start_date < end_date:
        prepare_final_report(start_date, end_date)
        #print(flattened_final_report)
        try:
    
            flattened_final_report = pd.concat(flattened_final_report)
            flattened_final_report = flattened_final_report.replace('NaN', 0)
            flattened_final_report.columns = conf.get("flattened_final_report_output_file_title").split(",")
            print(flattened_final_report)
    
            flattened_final_report = convert_df_to_desired_format(flattened_final_report)
            print(flattened_final_report)
            flattened_final_report.to_csv(conf.get("output_file_path") + filename, index=False , sep=',' ,header=conf.get("output_file_title").split(","))
            
            #start_date=current_weekly_date
        except Exception as ex:
            logger.error("[DM_ADOBE_ANALYTICS_EXECUTIVE_ERROR]: error while writing final report to HDFS: " + str(ex))
    else:
        logger.error("[DM_ADOBE_ANALYTICS_EXECUTIVE_ERROR]: date error start date is greater than equal to end date")
    
    
    print("done")
    logger.info("[DM_ADOBE_ANALYTICS_EXECUTIVE_INFO]: Process ended at:" + str(datetime.now()))

    
    
    
    

    

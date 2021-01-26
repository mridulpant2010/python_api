import psycopg2, json, requests, sys, time
import pandas as pd
from datetime import datetime
from pyutil import logger as lg
from pyutil import oauth
from pyutil import hawq_operations as hops
from pyutil import file_operations as fo

import sys
sys.path.append("/usr/local/share/applications/hdw/lib")
from environment_var_loader import *

                          
def get_db_connection():
    print("get db connection")
    logger.info("[DM_ADOBE_ANALYTICS_SEO_DASHBOARD]:getting db connection")
    #get db connection
    try:
        
        con = hops.getconnection(hawq_host, hawq_username, hawq_database, hawq_password, logger)
        return con.cursor()
    except Exception as ex:
        logger.error("[DM_ADOBE_SEO_ERROR]: Error in obtaining DB connection: " + str(ex))
        exit(-1)

def fetch_country_dim_from_db():
    try:
        cur.execute(conf.get("country_dim_query"))
        c_dim_df = cur.fetchall()
        logger.info("[DM_ADOBE_ANALYTICS_SEO_DASHBOARD]  d country dim dataframe")
        return pd.DataFrame(c_dim_df,columns={'country_geo','country_itemID'})
    except psycopg2.Error as ex:
        logger.error("[DM_SEO_ERROR]: error while getting max date from attribution fact: " + str(ex))
        return None

def flatten_primitive(in_df,valuename,itemname):
    try:
        subset_df = in_df[['value','itemId']]
        subset_df = subset_df.rename(columns={'value':valuename,'itemId':itemname})
        return subset_df
    except Exception as ex:
        logger.error("[DM_ADOBE_ANALYTICS_SEO_DASHBOARD_ERROR]: error while flattening primitive attributes: " + str(ex))


#True, rec['date_itemid'], rec['date'], final_payload,country_list,bu_list,logger
def prepare_final_report(by_date,cur_date_itemid,cur_date,final_payload,country_list,bu_list,logger):
    flattened_final_report_2 = []
    try:
        #flattened_final_report = []
        count=0
        for index in range(len(geo_list)):
            df_geo = df_country[df_country['country_geo']==geo_list[index]]
            print(index,df_geo['country_geo'].unique())
            logger.info("[DM_ADOBE_ANALYTICS_SEO]: running for region "+ geo_list[index] + ", date:" + str(cur_date))
            geo_country_ids = df_geo['country_itemID']#.astype(basestring)
            geo_country_ids = '","'.join(str(e) for e in geo_country_ids)
            print(geo_country_ids)
           
            for country_segment,country_name in country_list.items():
                for bu_segment,bu_name in bu_list.items():
                    logger.info("[DM_ADOBE_ANALYTICS_SEO_DASHBOARD_INFO]: running for country "+ country_name + ", bu_segment:" + bu_name)
                    final_payload_internal = final_payload
                    final_payload_internal = str(final_payload_internal).replace("START_DATE", str(start_date))
                    final_payload_internal = str(final_payload_internal).replace("END_DATE", str(end_date))
                    final_payload_internal = str(final_payload_internal).replace("COUNTRY_TOBE_REPLACED",str(geo_country_ids))
                    final_payload_internal = str(final_payload_internal).replace("COUNTRY_SEGMENT",str(country_segment))
                    final_payload_internal = str(final_payload_internal).replace("BU_SEGMENT",str(bu_segment))
                    #print final_payload_internal
                    #print(final_payload_internal)
                    final_payload_json = oauth.generate_report(conf.get("report_api"),final_payload_internal,logger).json()
                    print(final_payload_json)
                    print(count)
                    count+=1
                    if final_payload_json.get("lastPage") == False:
                        logger.error("[DM_ADOBE_ANALYTICS_ERROR]: More than 50000 records are returned in API call. Change the limit in input payload and run again. Exiting...")
                        exit(-1)
                    
                    #handle the condition to get the data for the next page
                    f_df = pd.DataFrame(data=final_payload_json.get("rows"))
                    print(country_segment,bu_segment)
                    flattened_final_report_1 = flatten_final_report(f_df)
                    
                    #print (flattened_final_report_1)
                    flattened_final_report_1['country_segment'] = country_name
                    flattened_final_report_1['bu_segment'] = bu_name
                    flattened_final_report_1['geo'] = geo_list[index]
                    #flattened_final_report_1['date'] = str(start_date) + "-" + str(end_date)
                    #print (flattened_final_report_1)
                    flattened_final_report_2.append(flattened_final_report_1)
            #print(flattened_final_report_2)            
        return flattened_final_report_2
    except Exception as ex:
        logger.error("[DM_ADOBE_ANALYTICS_ERROR]: error while preparing final report: " + str(ex))
        print ("error in preparing final report")



def flatten_final_report(f_df):
    try:
        #print f_df
        data_file2_rows_filtered = f_df[['data', 'value']]
        # filter df1 with country_name and country_itemID
        data_file2_rows_transpose = f_df.data.apply(pd.Series).merge(data_file2_rows_filtered, left_index=True, right_index=True).drop(['data'],axis=1)

        return data_file2_rows_transpose
    except Exception as ex:
        logger.error("[DM_ADOBE_ANALYTICS_SEO_DASHBOARD_ERROR]: error while flattening final report: " + str(ex))

def process_data_by_month(final_payload,flattened_dates_df,country_list,bu_list,logger):
    try:
        final_df = pd.DataFrame()
        for idx,rec in flattened_dates_df.iterrows() :
            print (idx ,rec['date'])
            #first write the code for the prepare_final_report
            flattened_final_report = prepare_final_report(True, rec['date_itemid'], rec['date'], final_payload,country_list,bu_list,logger)
            if not flattened_final_report:
                logger.info("[DM_ADOBE_ANALYTICS_INFO]:record doesn't exists for date "+rec['date'])
                #print "record doesn't exists for date "+rec['date']
                pass
            else:
                final_df_1 = pd.concat( flattened_final_report)
            
                final_df = pd.concat([final_df, final_df_1])
        return final_df
    except Exception as ex:
        logger.error("[DM_ADOBE_ANALYTICS_ERROR]: error in process_data_by_day function: " + str(ex))
		
		
def generate_date_item_ids(start_date,end_date):
    try:
        dates_payload = open("../conf/itdsd_hadoop_dm_adobe_analytics_monthly_date_lists.json",'r').read()
        dates_payload = str(dates_payload).replace("START_DATE",str(start_date))
        dates_payload = str(dates_payload).replace("END_DATE",str(end_date))
        dates_json = oauth.generate_report(conf.get("report_api"),dates_payload,logger).json()
        c_df = pd.DataFrame(data=dates_json.get("rows"))
        flattened_dates_df = flatten_primitive(c_df,'date','date_itemid')
        return flattened_dates_df
    except Exception as ex:
        logger.error("[DM_ADOBE_ANALYTICS_VIDEO_ERROR]: error in generate_date_item_ids function: " + str(ex))

def convert_df_to_desired_format(final_df):
    try:
        my_df_columns_to_rows = final_df.set_index(['country_segment','bu_segment','value','geo']).stack().reset_index()
        print("convert df_to_desired_format")
        print (my_df_columns_to_rows)
        df_of_split_metric_bu_channel = pd.DataFrame(my_df_columns_to_rows.level_4.unique(),columns = ["level_4"])
        
        df_of_split_metric_bu_channel[["metric", "BU"]] = df_of_split_metric_bu_channel.level_4.str.split('_', expand=True)
        
        final_computed_df = df_of_split_metric_bu_channel.merge(my_df_columns_to_rows, how='inner', on="level_4")
        #print(final_computed_df.head())
        #adding one more column here
        return final_computed_df.drop(columns=['level_4'])
    except Exception as ex:
        logger.error("[DM_ADOBE_SEO_DASHBOARD_ERROR]: error while converting to desired format " + str(ex))
        print ("terminated")
        exit(-1)


def fetch_date_range(query):
    try:
        cur.execute(query)
        date_dim_df = cur.fetchall()
        logger.info("[DM_ADOBE_ANALYTICS_EXECUTIVE_INFO]: prepared date range for summary dashboard query")
        return date_dim_df
    except psycopg2.Error as ex:
        logger.error("[DM_ADOBE_ANALYTICS_EXECUTIVE_ERROR]: error while getting max date from attribution fact: " + str(ex))
        return None



if __name__ == '__main__':

    conf  = json.loads(open('../conf/itdsd_hadoop_dm_adobe_analytics_config_seo_payload.json').read())
    
    logger = lg.getlogger(conf.get("log_file_path"), conf.get("log_file_name"), conf.get("logger_name"))
    logger.info("[DM_ADOBE_ANALYTICS_SEO_DASHBOARD]: Starting the process at:" + str(datetime.now()))
    # move exiting files to archive
    fo.move_files(conf.get("output_file_path"), conf.get("archived_file_path"))
    cur = get_db_connection()
    geo_list=['AMER','EMEA','APAC']

    
    #start_date = sys.argv[1]#'2019-05-22'#
    #end_date = sys.argv[2]#'2019-05-29'#

    #flattened_dates_df = generate_date_item_ids(start_date,end_date)
    print("getting country dim") 
    #prepare list of countries by geo to be used for data extract from adobe lookup
    df_country = fetch_country_dim_from_db()
    print(df_country)

    list_country_segment = conf.get("list_country_segments")
    list_bu_segment = conf.get("list_bu_segments")
    print(list_country_segment)
    
    df_dates = fetch_date_range(conf.get("date_range_query"))
    if (df_dates[0][0]) != None:
        start_date = df_dates[0][0] #  max date in table
        end_date = df_dates[0][1]
    flattened_dates_df  = generate_date_item_ids(start_date,end_date)
    filename = conf.get("output_file_name") + '_' + str(start_date) + '_' + str(end_date) + conf.get("output_file_extn")
    logger.info("[DM_ADOBE_ANALYTICS_SEO_INFO]: output filename is:" + filename)
     # creating a 0 byte file touch file; will be useful if no data is to process. HAWQ process wont fail
    flattened_final_report1 = pd.DataFrame()
    flattened_final_report1.to_csv(conf.get("output_file_path") + filename, index=False, header=False, sep=',')
    print(start_date)
    print(end_date)
    #using the payload of the data
    final_payload = open("../conf/itdsd_hadoop_dm_adobe_analytics_final_seo_payload.json","r").read()
    #flattened_final_report =[]
    if start_date < end_date:
        try:
            #flattened_final_report_seo = prepare_final_report(start_date,end_date,final_payload,list_country_segment,list_bu_segment)
            flattened_final_report = process_data_by_month(final_payload,flattened_dates_df,list_country_segment,list_bu_segment,logger)
            print("final updated dataframe")
            flattened_final_report=flattened_final_report.replace('NaN',0)
            #print(flattened_final_report)
            flattened_final_report.columns = conf.get("flattened_final_report_output_file_title").split(",")
            #flattened_final_report_video_HEARTEBEAT.columns = conf.get("flattened_final_report_output_file_title").split(",")
            print(flattened_final_report)
            flattened_final_report = convert_df_to_desired_format(flattened_final_report)
            print(flattened_final_report)
            flattened_final_report.to_csv(conf.get("output_file_path") + filename, index=False , sep=',' ,header=conf.get("output_file_title").split(","))
            print("done")
        except Exception as ex:
            logger.error("[DM_ADOBE_ANALYTICS_SEO_ERROR]: error while writing final report to HDFS: " + str(ex))
            exit(-1)
    else:
        logger.error("[DM_ADOBE_ANALYTICS_SEO_ERROR]: date error start date is greater than equal to end date")
    logger.info("[DM_ADOBE_ANALYTICS_SEO_INFO]: Process ended at:" + str(datetime.now()))




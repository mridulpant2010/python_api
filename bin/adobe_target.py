# -*- coding: utf-8 -*-
import jwt
from jwt.contrib.algorithms.pycrypto import RSAAlgorithm

# jwt.unregister_algorithm('RS256')
# jwt.register_algorithm('RS256', RSAAlgorithm(RSAAlgorithm.SHA256))
import requests
import csv
import datetime
import time
import psycopg2
import json
import sys

import sys 
sys.path.append("/usr/local/share/applications/hdw/lib")
from environment_var_loader import *

def encode_utf8(var):
    if isinstance(var, basestring):
        return var.encode('utf8')
    else:
        return unicode(var).encode('utf8')


def create_file(filepath, data):
    try:
        file = open(filepath, "wb+")
        f = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        f.writerows(data)
        file.close()
    except Exception as e:
        print(e)
        print("File creation failed")


def get_dimensions_day(visitors, visit, impression, exp):
    if visit is not None and visitors is not None and impression is not None:
        if 'intervals' in visit and 'intervals' in visitors and 'intervals' in impression:
            v_intervals = visit.get('intervals', None)
            i_intervals = impression.get('intervals', None)
            vi_intervals = visitors.get('intervals', None)
            print("Length of intervals:" + str(len(v_intervals)) + "," + str(len(i_intervals)) + "," + str(
                len(i_intervals)))
            for j in range(len(v_intervals)):
                interval = encode_utf8(v_intervals[j].get('interval'))
                interval_date = interval.split("T")[0]
                visitors = encode_utf8(vi_intervals[j].get('totals').get('entries'))
                visits = encode_utf8(v_intervals[j].get('totals').get('entries'))
                impressions = encode_utf8(i_intervals[j].get('totals').get('entries'))
                row = [id, name, type, exp, interval, impressions, visits, visitors, interval_date,rightnow]
                rows.append(row)
        else:
            print(visit, impression, visitors)
    else:
        print("No data")


def query_hawq(sql):
    # Create the connection
    try:
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        cur.execute(sql)
        data = cur.fetchall()
        # conn.commit()
        cur.close()
        conn.close()
        return data
    except Exception as e:
        print(e)
        print("Faliure while connecting to hawq")
        sys.exit(-1)


###############################
#      MAIN     /con              #
###############################

try:
    rightnow = str(datetime.datetime.utcnow())
    rightnow = rightnow.split(".")[0]
    print(rightnow)
    config = json.loads(
        open("/usr/local/share/applications/hdw/conf/itdsd_hadoop_dm_adobe_target_config.json", "r").read())
    #conn_string = config.get("conn_string")
    conn_string="host='{}' dbname='{}' user='{}' password='{}'".format(hawq_host,hawq_database,hawq_username,hawq_password)
    # Read the private key from the file
    file = open("/usr/local/share/applications/hdw/conf/private.key", "r")
    private_key = file.read()

    # Convert time into epoch format for validity of access token
    t = datetime.datetime.utcnow() + datetime.timedelta(days=1)
    t = time.mktime(t.timetuple())
    t = str(t).split(".")[0]

    payload2 = {
        "exp": int(t),
    	"iss": "5B29123F5245AD520A490D45@AdobeOrg",
    	"sub": "865E6D065D9DA7D70A495C9E@techacct.adobe.com",
    	"https://ims-na1.adobelogin.com/s/ent_analytics_bulk_ingest_sdk": True,
    	"https://ims-na1.adobelogin.com/s/ent_marketing_sdk": True,
    	"aud": "https://ims-na1.adobelogin.com/c/027b9bdae1a4494399e1bb902a213b84"
    }

    client_id = config.get("client_id_jwt")
    client_secret = config.get("client_secret_jwt")

    try:
        encoded = jwt.encode(payload2, private_key, algorithm='RS256')
	#print(encoded)
        res = requests.post('https://ims-na1.adobelogin.com/ims/exchange/jwt/?client_id=' + client_id + '&client_secret=' + client_secret + '&jwt_token=' + encoded)
	print(res.status_code)
        res_json = res.json()
	print(res_json)
    except Exception as e:
        print(e)
        print("jwt exchange API failed")

    access_token = res_json.get("access_token")
    

    headers = {'authorization': 'Bearer ' + access_token,
               'cache-control': 'no-cache',
               'content-type': 'application/vnd.adobe.target.v1+json',
               'x-api-key': client_id
               }

    date_today = str(datetime.datetime.now().date())
    dw_load_date = query_hawq(config.get("max_metricdate_sql"))
    dw_load_date = dw_load_date[0][0] - datetime.timedelta(days=10)
    #dw_load_date="2019-09-30"
    #date_today="2019-12-31"
    print(str(dw_load_date), date_today)
    rows = []
    h = ["activity_id", "activity_name", "type_of_report", "experience", "interval", "impressions", "Visits",
         "visitors","interval_date", "dw_load_date"]
    # rows.append(h)
    sql = config.get("activity_sql")
    activities = query_hawq(sql)
    params = {
            "reportInterval": str(dw_load_date) + "T00:00:00/" + str(date_today) + "T23:59:59",
            "resolution": "day"
             }
    for i in range(len(activities)):
        print("Activity:" + str(i))
        act_id = activities[i][0]
        act_type = activities[i][1]

        report_url = 'https://mc.adobe.io/vmwareinc/target/activities/' + str(act_type) + '/' + str(
                act_id) + '/report/performance'
        try:
            print(report_url)
            response = requests.get(report_url, headers=headers, params=params)
            s = response.status_code
            print(response.status_code)
            res = response.json()
            #print(res)
        except:
            print("Report API request failed")

        if s == 503:
            retry = 1
            while retry < 5:
                print(retry)
                time.sleep(60)
                try:
                    response = requests.get(report_url, headers=headers, params=params)
                    print(response.status_code)
                    res = response.json()
                    s = response.status_code
                    if s == 503:
                        retry += 1
                    else:
                        retry = 5
                except:
                    print("Report API request failed while retrying after sleep")

        id = encode_utf8(res.get('activity').get('id'))
        type = encode_utf8(res.get('activity').get('type'))
        name = encode_utf8(res.get('activity').get('name'))
        stats = res.get('report').get('statistics')
        if stats != {} and stats.get('totals') != {}:
            if type == "abt":
                control = stats.get('control')
                visitors = control.get('totals').get('visitor')
                visit = control.get('totals').get('visit')
                impression = control.get('totals').get('impression')
                get_dimensions_day(visitors, visit, impression, "control")

                targeted = stats.get('targeted')
                visitors = targeted.get('totals').get('visitor')
                visit = targeted.get('totals').get('visit')
                impression = targeted.get('totals').get('impression')
                get_dimensions_day(visitors, visit, impression, "targeted")
		
		personalized = stats.get('totals')
		visitors = personalized.get('visitor')
		visit = personalized.get('visit')
		impression = personalized.get('impression')
		get_dimensions_day(visitors, visit, impression, "personalized")
            else:
                experiences = res.get('activity').get('experiences')
                rep_exp = stats.get('experiences')
                for i in range(len(rep_exp)):
                    xp = rep_exp[i]
                    exp_id = rep_exp[i].get('experienceLocalId')
                    for k in range(len(experiences)):
                        experience_id = experiences[k].get('experienceLocalId')
                        if exp_id == experience_id:
                            exp = encode_utf8(experiences[k].get('name'))
                            visitors = xp.get('totals').get('visitor')
                            visit = xp.get('totals').get('visit')
                            impression = xp.get('totals').get('impression')
                            get_dimensions_day(visitors, visit, impression, exp)

    create_file(config.get("target_filepath") + "adobe_target.csv", rows)   #"+dw_load_date+"_"+date_today+"

    print(str(datetime.datetime.utcnow()))

except Exception as e:
    print(e)
    print("Script failed")
    # create_file(config.get("target_filepath") + "adobe_target_failed" + str(dw_load_date) + "_" + str(date_today) + ".csv",rows)
    sys.exit(-1)

""" oauth module """
import time, selenium, json, requests

from requests_oauthlib import OAuth2Session

from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import TimeoutException

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import datetime,jwt


"""
oauth2.0 based validation
currently working only for adobe analytics - to be checked for LinkedIn

deprecating this ouath2 function with the jwt
"""


def prepare_jwt_payload(jwtPayloadJson):
    #take the exp time from datetime:
    exp_time =datetime.datetime.utcnow() + datetime.timedelta(seconds=30)
    #payload structure for the jwt
    #jwtPayloadJson=jwt_conf.get("jwtPayloadStructure")
    print(jwtPayloadJson)
    jwtPayloadJson['exp']=exp_time

    return jwtPayloadJson



def get_access_token(jwtPayloadJson,client_id_jwt,client_secret_jwt,url,logger):

    try:
        print(jwtPayloadJson)

        #reading the privatekey
        keyfile = open('/usr/local/share/applications/hdw/conf/private.key')
        private_key = keyfile.read()
        # Encode the jwt Token
        jwttoken = jwt.encode(jwtPayloadJson, private_key, algorithm='RS256')
        accessTokenRequestPayload = {'client_id': client_id_jwt
                                ,'client_secret': client_secret_jwt}
        accessTokenRequestPayload['jwt_token'] = jwttoken
        #print(accessTokenRequestPayload)
        result = requests.post(url, data = accessTokenRequestPayload)
        resultjson = json.loads(result.text)

        #file where the access token is present
        with open('token_jwt.json','w') as outfile:
            json.dump(resultjson,outfile)
        return resultjson['access_token']
    except Exception as ex:
        logger.error("[PYUTIL_JWT_ERROR]:Error while generating JWT token: "+str(ex))
        print(ex)

def oauth2(client_id, client_secret, username, password, authorization_base_url, token_url,
           scope, redirect_uri, ff_webdriver_path, element_id_username, element_id_pwd, element_id_signin, logger):
    try:
        # Initialize the Headless Browser to Open a the Web page Authorization URL and authenticate the username and password
        options = Options()
        options.headless = True

        # Create a Firefox Driver Session
        driver = webdriver.Firefox(firefox_options=options, executable_path=ff_webdriver_path)

        # Initialize Session to get the Authorization URL
        oauth = OAuth2Session(client_id, redirect_uri=redirect_uri, scope=scope)
        authorization_url, state = oauth.authorization_url(authorization_base_url)

        # Run selenium for validating user on the browser
        ## Open the Authorization URL in the Broswer
        driver.get(authorization_url)
        time.sleep(10)

        driver.find_element_by_id(element_id_username).send_keys(username)
        driver.execute_script('document.getElementById("' + element_id_pwd + '").removeAttribute("readonly")')
        driver.find_element_by_id(element_id_pwd).clear()
        driver.find_element_by_id(element_id_pwd).click()
        driver.find_element_by_id(element_id_pwd).send_keys(Keys.HOME)
        driver.find_element_by_id(element_id_pwd).send_keys(password)
        driver.find_element_by_id(element_id_signin).submit()
        logger.info("[PYUTIL_OAUTH2_INFO]: First Retry " )

        #resubmit page due to behavior exhibited by Adobe login
        driver.execute_script('document.getElementById("' + element_id_pwd + '").removeAttribute("readonly")')
        driver.find_element_by_id(element_id_pwd).clear()
        driver.find_element_by_id(element_id_pwd).click()
        driver.find_element_by_id(element_id_pwd).send_keys(Keys.HOME)
        driver.find_element_by_id(element_id_pwd).send_keys(password)
        logger.info("[PYUTIL_OAUTH2_INFO]:  second retry" )

        driver.execute_script('document.getElementById("' + element_id_pwd + '").removeAttribute("readonly")')
        driver.find_element_by_id(element_id_pwd).clear()
        driver.find_element_by_id(element_id_pwd).click()
        driver.find_element_by_id(element_id_pwd).send_keys(Keys.HOME)
        driver.find_element_by_id(element_id_pwd).send_keys(password)
        driver.find_element_by_id(element_id_signin).submit()
        driver.find_element_by_id(element_id_signin).submit()
        logger.info("[PYUTIL_OAUTH2_INFO]:  Third Time retry" )
        time.sleep(5)


        ## driver.current_url contains the 'code' value, to be used to generate access_token
        token = oauth.fetch_token(
            token_url,
            authorization_response=driver.current_url,
            client_secret=client_secret)

        access_token = token.get("access_token")
        #refresh_token = token.get("refresh_token")
        logger.info("[PYUTIL_OAUTH2_INFO]:  Access token  fetched using get")
        ## save tokens in a file for future use
        with open('tokens.json', 'w') as outfile:
            json.dump(token, outfile)
        outfile.close()
        driver.quit()
        return access_token
    except Exception as ex:
        logger.error("[PYUTIL_OAUTH2_ERROR]: Error in oauth2.0 function: " + str(ex))
        #print ex
        exit(-1)


def get_response(url,access_token, payload_data,adobe_client_id,logger):
        try:
                headers = {
                'x-api-key': adobe_client_id,
                'x-proxy-global-company-id': "vmware1",
                'Authorization': "Bearer " + access_token,
                'Accept': 'application/json',
                'Content-Type': 'application/json'
                }
                response = requests.request("POST", url, headers=headers, data=payload_data)
                return response
        except Exception as ex:
                logger.error("[PYUTIL_RESPONSE_ERROR]: Error in get_response funciton :" +str(ex))

"""
method to hit reporting API and give the data. Runs for input array of countrie and date item IDs
"""

def generate_report(report_api, payload, logger):
    # access reporting API
    #global adobe_conf
    adobe_conf = json.loads(open("/usr/local/share/applications/hdw/conf/itdsd_hadoop_dm_adobe_analytics_login_config.json", 'r').read())
    url = report_api
    adobe_client_id = adobe_conf.get("client_id")
    #print(adobe_conf)
    try:
        token_file = json.loads(open("token_jwt.json", 'r').read())
        access_token = token_file.get("access_token")
        #print(adobe_conf.get("jwtPayloadStructure"))
    except Exception as ex:
        if "token_jwt.json" in str(ex):
            print("token_jwt.json not found , hence generating one")
            logger.info("[PYUTIL_JWT_INFO]: token.json not found: " + str(ex) + ", going for new token.")
            #access_token = jwt_access_token
            print(adobe_conf.get("jwtPayloadStructure"))
            jwtPayloadJson= prepare_jwt_payload(adobe_conf.get("jwtPayloadStructure"))

            access_token=get_access_token(jwtPayloadJson,adobe_conf.get("client_id_jwt"),adobe_conf.get("client_secret_jwt"),adobe_conf.get("url"),logger)
        else:
            logger.error("[PYUTIL_JWT_ERROR]: cannot create new tokens.json: " + str(ex))
            exit(-1)

    params = {
        'limit': '1000',
        'page': '0'
    }

    try:
        response = get_response(url,access_token,payload,adobe_client_id,logger)
        # if Oauth token is not valid
        if response.status_code == 401:
            print "Oauth token is not valid with status code as: " + str(
                response.status_code) + ", going for new token."
            logger.info("[PYUTIL_JWT_INFO]: Oauth token is not valid with status code as:" + str(response.status_code) + ", going for new token.")
            print(adobe_conf.get("jwtPayloadStructure"))
            jwtPayloadJson= prepare_jwt_payload(adobe_conf.get("jwtPayloadStructure"))
            access_token=get_access_token(jwtPayloadJson,adobe_conf.get("client_id_jwt"),adobe_conf.get("client_secret_jwt"),adobe_conf.get("url"),logger)
            response = get_response(url,access_token,payload,adobe_client_id,logger)
        # specific to API.14 call
        elif response.status_code == 400 and response.json().get("error") == 'report_not_ready':
            print "Adobe Analytics 1.4 response not ready. retrying..."
        elif response.status_code != 200:
            #retry mechanism for the below 504 status code
            if response.status_code == 504:
                count=0
                while response.status_code == 504 and count <11:
                        logger.info("[PYUTIL_JWT_INFO]: Encountered 504 error running the jwt code again for the time : "+str(count))
                        jwtPayloadJson= prepare_jwt_payload(adobe_conf.get("jwtPayloadStructure"))
                        access_token=get_access_token(jwtPayloadJson,adobe_conf.get("client_id_jwt"),adobe_conf.get("client_secret_jwt"),adobe_conf.get("url"),logger)
                        response = get_response(url,access_token,payload,adobe_client_id,logger)
                        count+=1
            elif response.status_code == 429:
                count_another=0
                while response.status_code == 429 and count_another <5:
                        time.sleep(20)
                        logger.info("[PYUTIL_JWT_INFO] : Encountered 429 error running after 15 sec : "+str(count_another))
                        jwtPayloadJson= prepare_jwt_payload(adobe_conf.get("jwtPayloadStructure"))
                        access_token=get_access_token(jwtPayloadJson,adobe_conf.get("client_id_jwt"),adobe_conf.get("client_secret_jwt"),adobe_conf.get("url"),logger)
                        response = get_response(url,access_token,payload,adobe_client_id,logger)
                        count_another+=1


            else:
                logger.error("[PYUTIL_JWT_ERROR]: Error while obtaining API response, response code:" + str(
                        response.status_code) + " , response text is:" +
                                response.text)
                exit(-1)
        return response
    except Exception as ex:
        logger.error("[PYUTIL_JWT_ERROR]: Exception while running the Adobe API: " + ex.message)
        print ("Exception while running the Adobe API: " + ex.message)
        exit(-1)

#THIS METHOD IS CURRENTLY NOT BEING USED
def generate_report_oauth(report_api, payload, logger):
    # access reporting API
    adobe_conf = json.loads(open("/usr/local/share/applications/hdw/conf/itdsd_hadoop_dm_adobe_analytics_login_config.json", 'r').read())
    url = report_api
    try:
        token_file = json.loads(open("tokens.json", 'r').read())
        access_token = token_file.get("access_token")
    except Exception as ex:
        if "tokens.json" in str(ex):
            print("tokens.json not found , hence generating one")
            logger.info("[PYUTIL_OAUTH2_INFO]: token.json not found: " + str(ex) + ", going for new token.")

            access_token = oauth2(adobe_conf.get("client_id"), adobe_conf.get("client_secret"), adobe_conf.get("username"),
                                        adobe_conf.get("password"),
                                        adobe_conf.get("authorization_base_url"), adobe_conf.get("token_url"), adobe_conf.get("scope"),
                                        adobe_conf.get("redirect_uri"),
                                        adobe_conf.get("ff_webdriver_path"), "adobeid_username", "adobeid_password",
                                        "adobeid_signin",
                                        logger)


        else:
            logger.error("[PYUTIL_OAUTH2_INFO]: cannot create new tokens.json: " + str(ex))
            exit(-1)

    params = {
        'limit': '1000',
        'page': '0'
    }

    try:
        headers = {
            'x-api-key': adobe_conf.get("client_id"),
            'x-proxy-global-company-id': "vmware1",
            'Authorization': "Bearer " + access_token,
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        # if Oauth token is not valid
        if response.status_code == 401:
            print "Oauth token is not valid with status code as: " + str(
                response.status_code) + ", going for new token."
            logger.info("[PYUTIL_OAUTH2_INFO]: Oauth token is not valid with status code as:" + str(
                response.status_code) + ", going for new token.")

            access_token = oauth2(adobe_conf.get("client_id"), adobe_conf.get("client_secret"), adobe_conf.get("username"),
                                        adobe_conf.get("password"),
                                        adobe_conf.get("authorization_base_url"), adobe_conf.get("token_url"), adobe_conf.get("scope"),
                                        adobe_conf.get("redirect_uri"),
                                        adobe_conf.get("ff_webdriver_path"), "adobeid_username", "adobeid_password",
                                        "adobeid_signin",
                                        logger)



            headers = {
                'x-api-key': adobe_conf.get("client_id"),
                'x-proxy-global-company-id': "vmware1",
                'Authorization': "Bearer " + access_token,
                'Accept': 'application/json',
                'Content-Type': 'application/json'
            }
            response = requests.request("POST", url, headers=headers, data=payload)
        # specific to API.14 call
        elif response.status_code == 400 and response.json().get("error") == 'report_not_ready':
            print "Adobe Analytics 1.4 response not ready. retrying..."
        elif response.status_code != 200:
            logger.error("[PYUTIL_OAUTH2_ERROR]: Error while obtaining API response, response code:" + str(
                response.status_code) + " , response text is:" +
                         response.text)
            # print ("[PYUTIL_OAUTH2_ERROR]: Error while obtaining API response, response code:" + str(
            #     response.status_code) + " , response text is:" +
            #              response.text)
            exit(-1)
        return response
    except Exception as ex:
        logger.error("[PYUTIL_OAUTH2_ERROR]: Exception while running the Adobe API: " + ex.message)
        print ("Exception while running the Adobe API: " + ex.message)
        exit(-1)

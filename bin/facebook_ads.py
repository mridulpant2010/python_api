from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.adobjects.adaccountuser import AdAccountUser
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium import webdriver
from requests_oauthlib import OAuth2Session
from requests_oauthlib.compliance_fixes import facebook_compliance_fix
import json,os,time
from datetime import datetime
from pyutil import logger as lg
import pandas as pd


def get_authorization():

    authorization_base_url = conf.get("authorization_base_url") 
    print(authorization_base_url)
    token_url = conf.get("token_url")
    client_id = conf.get("client_id")
    client_secret = conf.get("client_secret")
    redirect_uri = conf.get("redirect_uri")
    facebook = OAuth2Session(client_id, redirect_uri=redirect_uri)
    facebook = facebook_compliance_fix(facebook)    
    authorization_url, state = facebook.authorization_url(authorization_base_url)
    authorization_url=authorization_url+'&scope=ads_management%20ads_read%20attribution_read'
    #doing this part because we have to add scope
    #authorization_url=authorization_url+'&scope=r_liteprofile%20r_emailaddress%20w_member_social%20r_ads_reporting%20w_organization_social%20r_organization_social%20r_ads%20rw_ads'
    print(authorization_url)

    try:
        options = Options()
        options.headless = False #make it true to run in the background
        
        driver_path= conf.get("driver_path")
        driver = webdriver.Firefox(firefox_options=options, executable_path=driver_path)#'C:\\Users\\MridulPant\\Downloads\\geckodriver.exe')
        #just log in to the facebook in the firefox.
        driver.get(authorization_url)
        
        #implement retry-mechanism here 
        username=conf.get("username")
        password= conf.get("password")
        driver.find_element_by_id('email').send_keys(username)
        driver.find_element_by_id('pass').send_keys(password)
        time.sleep(5)
        print("done")
        driver.find_element_by_id('loginbutton').submit()
        #driver.find_element_by_xpath('/html/body/div[1]/main/div[2]/form/div[3]/button').submit()
        #/html/body/div[1]/main/div[2]/form/div[3]/button
        #time.sleep(5)
        #driver.find_element_by_xpath('//*[@id="oauth__auth-form__submit-btn"]').submit()
        
        time.sleep(5)
        #import pdb;pdb.set_trace()
        redirect_response=driver.current_url.strip()
        print("redirecting the response code")
        print(redirect_response)
        token = facebook.fetch_token(token_url,authorization_response=redirect_response,client_secret=client_secret)#,include_client_id=True)

        access_token = token.get("access_token")
        #print(access_token)

        with open('../conf/facebook_token.json', 'w') as outfile:
            json.dump(token, outfile)
        outfile.close()
        print("file created")
        
        return access_token
    except Exception as ex:
        print("exception inside the get_authorization function "+str(ex))
    '''
    finally:
        driver.quit()
    '''

def get_bearer_token():
    try:
        #import pdb;pdb.set_trace()
        token_filepath = conf.get("token_filepath")
        if os.path.isfile(token_filepath):
            #linkedin_token.json
            token_file = json.loads(open("../conf/facebook_token.json").read())
            epoch_expiration_time=token_file.get("expires_at")
            token_expiration_date=time.strftime("%Y-%m-%d", time.gmtime(epoch_expiration_time))
            current_date = datetime.now().strftime("%Y-%m-%d")
            if current_date < token_expiration_date:
                access_token=token_file.get("access_token")
            else:
                access_token = get_authorization()
        else:
            print("token doesn't exists and we are going to generate it. ")
            access_token = get_authorization()
        return access_token
    except Exception as ex:
        print("exception inside the get_bearer_token function "+str(ex))
    


#get all the ad-account id

#get all the accounts available:
def get_ad_account():

    #import pdb;pdb.set_trace()
    try:
        me=AdAccountUser(fbid='me')
        ad_accounts=me.get_ad_accounts()
        ad_account_list=[]
        for each_ad_account in range(len(ad_accounts)):
            account_id=dict(ad_accounts[each_ad_account]).get("id")
            ad_account_list.append(account_id)

        return ad_account_list
    except Exception as ex:
        print(ex)


def form_creative_dataset(list_ac,ad_id):
    #import pdb;pdb.set_trace()
    list_ad_creatives_df=[]
    try:
        for creative_el in range(len(list_ac)):
            new_dict={}
            #import pdb; pdb.set_trace()
            creative_element= list_ac[creative_el]
            creative_element=dict(creative_element)
            account_id=creative_element.get("account_id")
            creative_id= creative_element.get("id")
            creative_name=creative_element.get("name")
            if creative_element.get("object_story_spec") is not None:
                object_story=dict(creative_element.get("object_story_spec"))
                if  object_story is not None :    
                    #object_story_spec=dict(object_story.get('link_data'))
                    if object_story.get('link_data') is not None:
                        object_story_spec=dict(object_story.get('link_data'))
                
                    elif object_story.get("link_data")is None and object_story.get("video_data") is None:
                        object_story_spec=dict()
                    
                    elif dict(dict(object_story.get("video_data")).get("call_to_action")) is not None:
                        object_story=dict(dict(object_story.get("video_data")).get("call_to_action"))
                        object_story_spec=dict(object_story.get('value'))
                else:
                    object_story_spec=object_story.get('link_data',{})
            else:
                object_story_spec={}
            child_attachments=object_story_spec.get("child_attachments")
            if child_attachments is None:
                url_link=object_story_spec.get("link")
            elif object_story_spec is None:
                url_link=None
            else:
                link=[childa.get("link") for childa in child_attachments]
                #just taking one element at a time,
                url_link=set(link).pop()
            print(account_id,creative_id,creative_name,url_link)
            new_dict['account_id']=account_id
            new_dict['creative_id']=creative_id
            new_dict['ad_id']=ad_id
            new_dict['creative_name']=creative_name
            new_dict['url_link']=url_link
            list_ad_creatives_df.append(new_dict)
        return list_ad_creatives_df
    except Exception as ex:
        print(ex)


if __name__ == "__main__":

    #read the conf structure:
    conf = json.loads(open("../conf/itdsd_hadoop_dm_facebook_config.json").read())
    logger = lg.getlogger(conf.get("log_file_path"), conf.get("log_file_name"), conf.get("logger_name"))
    logger.info("[DM_LINKEDIN]: Starting the process at:" + str(datetime.now()))

    #access_token = 'EAAU33OKKqZBEBAASb4Da2XeIvIch3CN7huqfWQ2HVpitxIVwCBo7FvOAvQeXdmx8bUGkCceRafQKHw96iU2AN34sE4C1n3Ii5WyZBX4fieuGlzzb2IgO66nTn5mDw1z1WgZB0NM5duxjd1zriBUwYZBqPCZB3qBqOLMnjS8Jn2YpNcZB9d3aYHbgTaYvqkYdkZD'
    access_token = get_bearer_token()
    print(access_token)

    #initializing the facebook ads
    FacebookAdsApi.init(conf.get("client_id"),conf.get("client_secret"),access_token=access_token)


    #start_date =sys.argv[1]
    #end_date=sys.argv[2]


    params={
        
        'fields':[AdsInsights.Field.account_currency,AdsInsights.Field.account_id,AdsInsights.Field.account_name,AdsInsights.Field.ad_id,AdsInsights.Field.ad_name,AdsInsights.Field.adset_name,AdsInsights.Field.adset_id,AdsInsights.Field.campaign_name,AdsInsights.Field.campaign_id,AdsInsights.Field.unique_clicks,AdsInsights.Field.ctr,AdsInsights.Field.impressions,AdsInsights.Field.spend,AdsInsights.Field.clicks,AdsInsights.Field.date_start,AdsInsights.Field.date_stop],
        'level':'ad',
        'time_range':{
                'since':'2019-02-01',
                'until':'2020-07-23'
        },
        #'export_format':'csv',
        #'export_name':'facebook.csv',
        'time_increment':1
    }


    params2={
        'fields':[AdCreative.Field.name,AdCreative.Field.adlabels,AdCreative.Field.object_story_spec,AdCreative.Field.account_id],
        'time_increment':1,
        'time_range':{
                'since':'2019-02-01',
                'until':'2020-07-23'
                }

        }


    ad_account_list=get_ad_account()
    print(ad_account_list)
    #getting ad_creatives information
    try:
        final_df=pd.DataFrame()
        for account_id in ad_account_list:
            my_account = AdAccount(str(account_id))
            ads=my_account.get_ads()
            ll=[]
            print("starting the new account",str(account_id))
            for a in range(len(ads)):
                ad = ads[a]
                ad_id = ad.get("id")
                ads_ac = Ad(ad_id)
                dct=form_creative_dataset(ads_ac.get_ad_creatives(fields=[AdCreative.Field.name,AdCreative.Field.adlabels,AdCreative.Field.object_story_spec,AdCreative.Field.account_id]),ad_id)
                ll.extend(dct)
            time.sleep(10)    
            df_final= pd.DataFrame(ll)
            print("ad_creatives_{0}".format(account_id))
            df_final.to_csv('../output/ad_creatives_{0}.csv'.format(account_id),index=False)
            print("--------------------df final-----------------------")
            #final_df=pd.concat([final_df,df_final])
        #final_df.to_csv("../output/ad_creatives.csv",index=False)

        #getting ad-insights information:

        for account_id in ad_account_list:
            list_ad_insights=[]
            my_account = AdAccount(str(account_id))
            ads=my_account.get_ads()
            for a in range(len(ads)):
                ad=ads[a]
                ad_id = ad.get("id")
                ads_ac = Ad(ad_id)
                list_ad_insights.extend(ads_ac.get_insights(params=params))
            print("ad_insights_{0}".format(account_id))
            df = pd.DataFrame(list_ad_insights)
            df.to_csv("../output/ad_insights_{0}.csv".format(str(account_id)),index=False)

    except Exception as ex:
        print(ex)


    logger.info("[DM_LINKEDIN]: Ending the process at:" + str(datetime.now()))
    
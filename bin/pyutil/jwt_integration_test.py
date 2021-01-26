import datetime,jwt,json,requests
#from pyutil import logger as lg

def prepare_jwt_payload():
    #take the exp time from datetime:
    exp_time =datetime.datetime.utcnow() + datetime.timedelta(seconds=30)
    #payload structure for the jwt
    jwtPayloadJson=jwt_conf.get("jwtPayloadStructure")
    jwtPayloadJson['exp']=exp_time

    jwttoken = jwt.encode(jwtPayloadJson, private_key, algorithm='RS256')

    accessTokenRequestPayload = {'client_id': str(jwt_conf.get("client_id"))
                                , 'client_secret': str(jwt_conf.get("client_secret"))}

    accessTokenRequestPayload['jwt_token'] = jwttoken
    
    return accessTokenRequestPayload 


def get_access_token(jwtPayloadJson):

    try:
        
        print "starting..."
        # Encode the jwt Token
	print jwt_conf.get("url")
	print jwtPayloadJson
        #print(accessTokenRequestPayload)
        result = requests.post(jwt_conf.get("url"), data = jwtPayloadJson)
        resultjson = json.loads(result.text)

        print(resultjson['access_token'])

        #file where the access token is present
        with open('../token_jwt.json','w') as outfile:
            json.dump(resultjson,outfile)
    
    except Exception as ex:
        print(ex)
    #print(resultjson["access_token"])

if __name__ == "__main__":

    jwt_conf = json.loads(open("../../conf/jwt_auth_conf.json","r").read())

    #private key which is require for the signature, please change the path with the respective private_key.
    keyfile = open('/home/hdw-etl/private.key','r')
    private_key = keyfile.read()

    jwtPayloadJson= prepare_jwt_payload()
    get_access_token(jwtPayloadJson)

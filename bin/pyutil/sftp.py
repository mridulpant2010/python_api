""" connect to sftp """

import pysftp
from pyutil import logger as lg

def connect_with_sftp(ftp_server, ftp_user, ftp_passwd, logger):
    logger.info("[PYUTIL_SFTP_MODULE_INFO]: attempting to connect to sftp host: "+ ftp_server+" with user:"+ftp_user)
    try:
        con = pysftp.Connection(host= ftp_server, username=ftp_user, password=ftp_passwd)
        logger.info("[PYUTIL_SFTP_MODULE_INFO]: connected to sftp host: "+ ftp_server+" with user:"+ftp_user+ " successfully")
        return con
    except Exception as ex:
        logger.error("[PYUTIL_SFTP_MODULE_ERROR]: Error while connecting with sftp server: " + str(ex))
        exit(-1)
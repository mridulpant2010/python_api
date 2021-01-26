""" connect with HAWQ SSE DB """
import psycopg2


def getconnection(host, user, db, pwd, logger):
    try:
        hawqAddress = host
        hawqUser = user
        hawqDatabase = db
        password = pwd
        conn = psycopg2.connect(
            "dbname='{db}' user='{u}' password='{p}'host='{h}'".format(db=hawqDatabase, u=hawqUser, h=hawqAddress,
                                                                       p=password))
        logger.info(
            "[PYUTIL_HAWQ_CONNECTION_INFO]:Connection established with host: " + hawqAddress + " ,user: " + hawqUser + " and db: " + hawqDatabase)
        return conn
    except Exception as ex:
        logger.info('[PYUTIL_HAWQ_CONNECTION_ERROR]:problem in obtaining connection...%s', str(ex))


"""
execute a hawq function 
"""


def execute_hawq_function(cur, fnc_name, logger):
    try:
        cur.execute(fnc_name)

        # if function exited with non zero exit code (failed)
        if (cur.fetchone()[0] != 0):
            logger.error(
                "[PYUTIL_EXEC_FNC_ERROR]: error while executing function " + fnc_name + ". Check ref.edw_dml_audit_log if you are making use of it.")
            exit(-1)
        else:
            logger.info("[PYUTIL_EXEC_FNC_INFO]: Function "+ fnc_name + " successfully.")
    except psycopg2.Error as ex:
        logger.error(
            "[PYUTIL_EXEC_FNC_ERROR]: exception while executing function: " + fnc_name + ", exception is: " + str(ex))
        exit(-1)

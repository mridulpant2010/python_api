import logging


"""
obtain logger for regular INFO and ERROR logging
"""
def getlogger(logpath, logfilename, loggername):
    logger = logging.getLogger(loggername)
    logger.setLevel(logging.DEBUG)

    ch = logging.FileHandler(logpath + logfilename)
    ch.setLevel(logging.DEBUG)
    # create formatter
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    # add formatter to ch
    ch.setFormatter(formatter)
    # add ch to logger
    logger.addHandler(ch)

    return logger
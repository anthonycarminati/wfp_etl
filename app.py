# DEFINE IMPORTS
import csv, subprocess, os, re, logging
from ftplib import FTP
import ConfigParser
import psycopg2 as ps

# CONFIG FILE PARSER
config = ConfigParser.ConfigParser()
config.readfp(open('/opt/wfp/wfp_etl/etl_config.conf'))

# SETUP GLOBAL CONFIG DICT
g = {
    # FTP INFO
    'FTP_ADDRESS': config.get('sterling', 'FTP_ADDRESS'),
    'FTP_USER': config.get('sterling', 'FTP_USER'),
    'FTP_PASSWORD': config.get('sterling', 'FTP_PASSWORD'),
    # DATABASE INFO
    'POSTGRES_HOST': config.get('warehouse', 'POSTGRES_HOST'),
    'POSTGRES_USER': config.get('warehouse', 'POSTGRES_USER'),
    'POSTGRES_PWD': config.get('warehouse', 'POSTGRES_PWD'),
    'POSTGRES_DB': config.get('warehouse', 'POSTGRES_DB'),
    # DATA PATH INFO
    'DATA_DROP_PATH': '/opt/wfp/wfp_etl/_data/sterling/drop/',
    # 'DATA_CONVERTED_PATH': '/opt/wfp/wfp_etl/_data/drop/',
    'DATA_ERROR_PATH': '/opt/wfp/wfp_etl/_data/sterling/errors/',
    # 'DATA_ETL_TMP_PATH': '/shared/_tmp/telescope_tmp.txt',
    # LOG FILE
    'LOG_FILE_PATH': '/opt/wfp/wfp_etl/_logs/etl_logs.log',
}

# SETUP LOGGING
logger = logging.getLogger('wfp_etl')
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler(g['LOG_FILE_PATH'])
ch = logging.StreamHandler()
fh.setLevel(logging.DEBUG)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(levelname)s] [%(asctime)s]: %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(ch)

# SETUP DB CONNECTION
conn = ps.connect(host=g['POSTGRES_HOST'],
                  port='5432',
                  user=g['POSTGRES_USER'],
                  password=g['POSTGRES_PWD'],
                  database=g['POSTGRES_DB'])
cur = conn.cursor()

# CONNECT TO FTP AND GET FILE LIST
logger.info('Checking FTP site for new files.')
ftp = FTP(g['FTP_ADDRESS'])
ftp.login(g['FTP_USER'], g['FTP_PASSWORD'])
ftp_files = ftp.nlst()

# SETUP LIST OF FILES TO EXCLUDE FROM DOWNLOADS
excl_download = list()

for file in os.listdir(g['DATA_DROP_PATH']):
    excl_download.append(file)

# DOWNLOAD FILES
for file in ftp_files:
    # P&L Reports
    if 'PLReport' in file and file not in excl_download:
        file_name = file
        file_date = file[16:24]

        # DOWNLOAD P&L REPORT FROM FTP AND CREATE AUDITING RECORD
        try:
            # DOWNLOAD P&L REPORT
            ftp.voidcmd("NOOP")
            logger.info('Downloading {0} to DATA_DROP_PATH.'.format(file))
            ftp.retrbinary('RETR {0}'.format(file), open('{0}{1}'.format(g['DATA_DROP_PATH'], file), 'w+').write)
            logger.info('{0} downloaded successfully!'.format(file))

            ## CREATE AUDITING RECORD FOR P&L REPORT
            # try:
            #     logger.info('NEED TO CREATE AUDITING RECORD FOR {0}.'.format(file))
            # except Exception, e:
            #     logger.error('{0}. Could not create etl_auditing record for {1}'.format(e, file))

        except Exception, e:
            logger.error('{0}.'.format(e))
# DEFINE IMPORTS
import csv, subprocess, os, re, logging, shutil
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
    'DATA_CONVERTED_PATH': '/opt/wfp/wfp_etl/_data/sterling/converted/',
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
# ADD CODE TO APPEND file_name FROM etl_sterling TO excl_download LIST

# DOWNLOAD FILES
for file in ftp_files:
    # DAILY REPORTS
    if '_Daily' in file and file not in excl_download:
        file_name = file
        file_date = file[16:24]

        # DOWNLOAD DAILY REPORT FROM FTP AND CREATE AUDITING RECORD
        try:
            # DOWNLOAD DAILY REPORT
            ftp.voidcmd("NOOP")
            logger.info('Downloading {0} to DATA_DROP_PATH.'.format(file))
            ftp.retrbinary('RETR {0}'.format(file), open('{0}{1}'.format(g['DATA_DROP_PATH'], file), 'w+').write)
            logger.info('{0} downloaded successfully!'.format(file))

            # CREATE AUDITING RECORD FOR DAILY REPORT
            # try:
            #     logger.info('NEED TO CREATE AUDITING RECORD FOR {0}.'.format(file))
            # except Exception, e:
            #     logger.error('{0}. Could not create etl_auditing record for {1}'.format(e, file))

        except Exception, e:
            logger.error('{0}. {1} could not be downloaded'.format(e, file))

# CLEAN UP FILE FROM DROP FOLDER AND PLACE IN CONVERTED FOLDER FOR UPLOAD
for file in os.listdir(g['DATA_DROP_PATH']):
    # DAILY REPORTS
    if '_Daily' in file:
        # pass
        # CALCULATE NUMBER OF ROWS
        # num_rows = sum(1 for row in csv.reader(open('{0}{1}'.format(g['DATA_DROP_PATH'], file), mode='r')))
        # TAKE ALL RECORDS EXCEPT FOR LAST ONE AND WRITE TO DATA_CONVERTED_PATH FOLDER
        # REMOVE OLD FILE FROM DATA_DROP_PATH
        try:
            with open('{0}{1}'.format(g['DATA_DROP_PATH'], file), 'rb') as file_obj:
                logger.info('Cleaning {0}.'.format(file))
                file_obj.seek(0, os.SEEK_END)
                pos = file_obj.tell() - 1
                while pos > 0 and file_obj.read(1) != "\n":
                    pos -= 1
                    file_obj.seek(pos, os.SEEK_SET)
                if pos > 0:
                    file_obj.seek(pos, os.SEEK_SET)
                    file_obj.truncate()

                shutil.move('{0}{1}'.format(g['DATA_DROP_PATH'], file), '{0}{1}'.format(g['DATA_CONVERTED_PATH'], file))

        except Exception, e:
            logger.error('{0}. Could not clean {1}'.format(e, file))

# PUSH ALL DOWNLOADS TO DATABASE
for file_str in os.listdir(g['DATA_CONVERTED_PATH']):
    # P&L REPORTS
    if '_Daily' in file_str:
        try:
            sql_cmd = u'\"\\COPY stg_daily_trades(trader,sequence_no,account,side,symbol,quantity,price,destination,contra,trade_datetime,bo_account,cusip,liq,order_id,exec_broker,ecn_fee,order_datetime,specialist,commission,bb_trade,sec_fee,batch_id,client_order_id,prime,cover_quantity,userr,settle_date,principal,net_amount,allocation_id,allocation_role,is_clearable,nscc_fee,nasdaq_fee,clearing_fee,nyse_etf_fee,amex_etf_fee,listing_exchange,native_liq,order_received_id,bo_group_id) FROM \'{q_file_path}\' WITH DELIMITER \',\' CSV HEADER ESCAPE AS \'\\\' \"'.format(q_file_path=g['DATA_CONVERTED_PATH']+file_str)
            pgsql_cmd = u'sudo psql {pg_user} -h {pg_host} -d {pg_db} -p 5432 -c {sql_cmd}'.format(pg_user=g['POSTGRES_USER'], pg_host=g['POSTGRES_HOST'], pg_db=g['POSTGRES_DB'], sql_cmd=sql_cmd)
            print pgsql_cmd
            pgsql_status = subprocess.call(pgsql_cmd, shell=True)
        except Exception, e:
            logger.error('{0}. {1} could not be pushed to database'.format(e, file_str))

# DEFINE IMPORTS
import os
import logging
from ftplib import FTP
import ConfigParser
import psycopg2 as ps
import pandas as pd

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
    'DATA_FINAL_PATH': '/opt/wfp/wfp_etl/_data/sterling/final/',
    'DATA_ERROR_PATH': '/opt/wfp/wfp_etl/_data/sterling/errors/',
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
exclude_file = list()
for file in os.listdir(g['DATA_DROP_PATH']):
    exclude_file.append(file)

# RETRIEVE LIST OF FILES ALREADY PUSHED TO DATABASE
sql_cmd = """SELECT file_name FROM etl_daily_trades;"""
cur.execute(sql_cmd)
for file in cur:
    exclude_file.append(file[0])  # CURSOR RETURNS DATA IN TUPLE

# DOWNLOAD FILES
for file in ftp_files:
    # DAILY REPORTS
    if '_Daily' in file and file not in exclude_file:
        file_name = file
        file_date = file[16:24]

        # DOWNLOAD DAILY REPORT FROM FTP AND CREATE AUDITING RECORD
        try:
            # DOWNLOAD DAILY REPORT
            ftp.voidcmd("NOOP")
            logger.info('Downloading {0} to DATA_DROP_PATH.'.format(file))
            ftp.retrbinary('RETR {0}'.format(file), open('{0}{1}'.format(g['DATA_DROP_PATH'], file), 'w+').write)
            logger.info('{0} downloaded successfully!'.format(file))

            # CREATE AUDITING RECORD HERE
            #############################
            #############################

        except Exception, e:
            logger.error('{0}. {1} could not be downloaded'.format(e, file))

    # OPEN POSITION REPORTS
    if '_PosAvgReports' in file and file not in exclude_file:
        pass

# HELPER FUNCTIONS - MOVE TO HELPER FUNCTION MODULE IN THE FUTURE
def func_side_desc(row):
    if row['Side'] == 'B':
        return 'Buy'
    else:
        return 'Sell'

def func_calculated_quantity(row):
    if row['Side'] == 'B':
        return row['Qty'] * 1
    else:
        return row['Qty'] * -1

def func_calculated_principal(row):
    if row['Side'] == 'B':
        return row['Principal'] * 1
    else:
        return row['Principal'] * -1

def func_ticket_fee(row):
    return 1

def func_total_fee(row):
    return 1

def func_away_ticket(row):
    return 1

def func_total_cost(row):
    return 1

def func_calculated_net(row):
    return 1


# CLEAN UP FILES FROM DROP FOLDER AND PLACE IN FINAL FOLDER FO R UPLOAD
for file in os.listdir(g['DATA_DROP_PATH']):
    if file not in os.listdir(g['DATA_FINAL_PATH']):
        # DAILY REPORTS
        if '_Daily' in file:
            try:
                # READ, CLEAN, AND WRITE CONVERTED DATA
                data_in = pd.read_csv('{0}{1}'.format(g['DATA_DROP_PATH'], file))
                data_out = data_in[data_in.Trader != '*']

                data_out['calculated_quantity'] = data_out.apply(func_calculated_quantity, axis=1)
                data_out['calculated_principal'] = data_out.apply(func_calculated_principal, axis=1)
                data_out['ticket_fee'] = data_out.apply(func_ticket_fee, axis=1)
                data_out['total_fee'] = data_out.apply(func_total_fee, axis=1)
                data_out['away_ticket'] = data_out.apply(func_away_ticket, axis=1)
                data_out['total_cost'] = data_out.apply(func_total_cost, axis=1)
                data_out['calculated_net'] = data_out.apply(func_calculated_net, axis=1)

                data_out.to_csv('{0}{1}'.format(g['DATA_FINAL_PATH'], file), index=False)
                logger.info('Successfully converted {0}'.format(file))

                # REMOVE FILE FROM DROP ZONE
                os.remove('{0}{1}'.format(g['DATA_DROP_PATH'], file))
            except Exception, e:
                logger.error('{0}. Could not clean {1}'.format(e, file))

        # OPEN POSITION REPORTS
        if '_PosAvgReports' in file:
            pass

# STAGE LOAD - PUSH FILES TO DATABASE FROM DATA_FINAL_PATH
for file in os.listdir(g['DATA_FINAL_PATH']):
    # DAILY REPORTS
    if '_Daily' in file:
        try:
            # COMPOSE AND EXECUTE COPY COMMAND
            with open('{0}{1}'.format(g['DATA_FINAL_PATH'], file), 'rb') as copy_file:
                sql_cmd = """COPY stg_daily_trades(trader,sequence_no,account,side,symbol,quantity,price,destination,contra,trade_datetime,bo_account,cusip,liq,order_id,exec_broker,ecn_fee,order_datetime,specialist,commission,bb_trade,sec_fee,batch_id,client_order_id,prime,cover_quantity,userr,settle_date,principal,net_amount,allocation_id,allocation_role,is_clearable,nscc_fee,nasdaq_fee,clearing_fee,nyse_etf_fee,amex_etf_fee,listing_exchange,native_liq,order_received_id,bo_group_id) FROM STDIN WITH CSV HEADER DELIMITER AS ',';"""
                cur.copy_expert(sql_cmd, copy_file)
                conn.commit()

            # LOGGING
            logger.info('Successfully pushed {0} to database.'.format(file))

            # CALCULATE METRICS FOR AUDITING
            file_size = os.path.getsize('{0}{1}'.format(g['DATA_FINAL_PATH'], file))
            num_rows = sum(1 for line in open('{0}{1}'.format(g['DATA_FINAL_PATH'], file))) - 1

            # COMPOSE AND EXECUTE AUDITING RECORD
            sql_cmd = """INSERT INTO etl_daily_trades(file_name, file_size, num_rows) VALUES(%(file_name)s, %(file_size)s, %(num_rows)s);"""
            cur.execute(sql_cmd, {'file_name': file, 'file_size': file_size, 'num_rows': num_rows})

            # LOGGING
            logger.info('Successfully wrote auditing record for {0}'.format(file))

            # REMOVE FILE FROM CONVERTED FOLDER
            os.remove('{0}{1}'.format(g['DATA_FINAL_PATH'], file))
        except Exception, e:
            logger.error('{0}. {1} could not be pushed to database'.format(e, file))

    # OPEN POSITION REPORTS
    if '_PosAvgReports' in file:
        pass

# KICK OFF STORED PROCEDURE FOR STAGE TO FINAL LOAD
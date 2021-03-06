# DEFINE IMPORTS
import os
import logging
from ftplib import FTP
import pandas as pd
import shutil
from helpers import *
from config import g

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
conn = ps.connect(host=g['POSTGRES_HOST'],
                  port='5432',
                  user=g['POSTGRES_USER'],
                  password=g['POSTGRES_PWD'],
                  database=g['POSTGRES_DB'])
cur = conn.cursor()
sql_cmd = """SELECT file_name FROM etl_daily_trades;"""
cur.execute(sql_cmd)
for file in cur:
    exclude_file.append(file[0])  # CURSOR RETURNS DATA IN TUPLE
conn = ps.connect(host=g['POSTGRES_HOST'],
                  port='5432',
                  user=g['POSTGRES_USER'],
                  password=g['POSTGRES_PWD'],
                  database=g['POSTGRES_DB'])
cur = conn.cursor()
sql_cmd = """SELECT file_name FROM etl_daily_open_positions;"""
cur.execute(sql_cmd)
for file in cur:
    exclude_file.append(file[0])  # CURSOR RETURNS DATA IN TUPLE
conn = ps.connect(host=g['POSTGRES_HOST'],
                  port='5432',
                  user=g['POSTGRES_USER'],
                  password=g['POSTGRES_PWD'],
                  database=g['POSTGRES_DB'])
cur = conn.cursor()
sql_cmd = """SELECT file_name FROM etl_daily_pl_report;"""
cur.execute(sql_cmd)
for file in cur:
    exclude_file.append(file[0])  # CURSOR RETURNS DATA IN TUPLE

def etl():
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

            except Exception, e:
                logger.error('{0}. {1} could not be downloaded'.format(e, file))

        # OPEN POSITION REPORTS
        if '_PosAvgReport' in file and file not in exclude_file:
            file_name = file
            file_date = file[16:24]

            # DOWNLOAD DAILY REPORT FROM FTP AND CREATE AUDITING RECORD
            try:
                # DOWNLOAD DAILY REPORT
                ftp.voidcmd("NOOP")
                logger.info('Downloading {0} to DATA_DROP_PATH.'.format(file))
                ftp.retrbinary('RETR {0}'.format(file), open('{0}{1}'.format(g['DATA_DROP_PATH'], file), 'w+').write)
                logger.info('{0} downloaded successfully!'.format(file))

            except Exception, e:
                logger.error('{0}. {1} could not be downloaded'.format(e, file))

        # OPEN POSITION REPORTS
        if '_PLReport' in file and file not in exclude_file:
            file_name = file
            file_date = file[16:24]

            # DOWNLOAD DAILY REPORT FROM FTP AND CREATE AUDITING RECORD
            try:
                # DOWNLOAD DAILY REPORT
                ftp.voidcmd("NOOP")
                logger.info('Downloading {0} to DATA_DROP_PATH.'.format(file))
                ftp.retrbinary('RETR {0}'.format(file), open('{0}{1}'.format(g['DATA_DROP_PATH'], file), 'w+').write)
                logger.info('{0} downloaded successfully!'.format(file))

            except Exception, e:
                logger.error('{0}. {1} could not be downloaded'.format(e, file))

    # CLEAN UP FILES FROM DROP FOLDER AND PLACE IN FINAL FOLDER FOR UPLOAD
    for file in os.listdir(g['DATA_DROP_PATH']):
        if file not in exclude_file:
            # DAILY REPORTS
            if '_Daily' in file:
                num_rows = sum(1 for line in open('{0}{1}'.format(g['DATA_DROP_PATH'], file)))
                if num_rows > 2:
                    try:
                        # READ AND CLEAN DATA
                        data_in = pd.read_csv('{0}{1}'.format(g['DATA_DROP_PATH'], file), sep=',', na_values='NULL')
                        data_out = data_in[data_in.Trader != '*']

                        # ADD DERIVED COLUMNS
                        data_out['side_desc'] = data_out.apply(func_side_desc, axis=1)
                        data_out['calculated_quantity'] = data_out.apply(func_calculated_quantity, axis=1)
                        data_out['calculated_principal'] = data_out.apply(func_calculated_principal, axis=1)
                        data_out['ticket_fee'] = data_out.apply(func_ticket_fee, axis=1)
                        data_out['total_fee'] = data_out.apply(func_total_fee, axis=1)
                        data_out['away_ticket'] = data_out.apply(func_away_ticket, axis=1)
                        data_out['total_cost'] = data_out.apply(func_total_cost, axis=1)
                        data_out['calculated_net'] = data_out.apply(func_calculated_net, axis=1)

                        # WRITE NEW FILE TO FINAL FOLDER
                        data_out.to_csv('{0}{1}'.format(g['DATA_FINAL_PATH'], file), index=False, sep='|')
                        logger.info('Successfully converted {0}'.format(file))

                        # REMOVE FILE FROM DROP ZONE
                        os.remove('{0}{1}'.format(g['DATA_DROP_PATH'], file))
                    except Exception, e:
                        logger.error('{0}. Could not pre-process {1}'.format(e, file))
                        shutil.move('{0}{1}'.format(g['DATA_DROP_PATH'], file), '{0}{1}'.format(g['DATA_ERROR_PATH'], file))
                        logger.error('{0}. {1} moved to errors folder'.format(e, file))

            # OPEN POSITION REPORTS
            if '_PosAvgReport' in file:
                try:
                    # READ AND CLEAN DATA
                    data_in = pd.read_csv('{0}{1}'.format(g['DATA_DROP_PATH'], file), sep=',', na_values='NULL')
                    data_out = data_in

                    # ADD DERIVED COLUMNS
                    data_out['pos_value'] = data_out.apply(func_pos_value, axis=1)

                    # WRITE NEW FILE TO FINAL FOLDER
                    data_out.to_csv('{0}{1}'.format(g['DATA_FINAL_PATH'], file), index=False, sep='|')
                    logger.info('Successfully converted {0}'.format(file))

                    # REMOVE FILE FROM DROP ZONE
                    os.remove('{0}{1}'.format(g['DATA_DROP_PATH'], file))
                except Exception, e:
                    logger.error('{0}. Could not pre-process {1}'.format(e, file))
                    shutil.move('{0}{1}'.format(g['DATA_DROP_PATH'], file), '{0}{1}'.format(g['DATA_ERROR_PATH'], file))
                    logger.error('{0}. {1} moved to errors folder'.format(e, file))

            # P&L REPORTS
            if '_PLReport' in file:
                try:
                    # READ AND CLEAN DATA
                    data_in = pd.read_csv('{0}{1}'.format(g['DATA_DROP_PATH'], file), sep=',', na_values='NULL')
                    data_out = data_in

                    # ADD DERIVED COLUMNS


                    # WRITE NEW FILE TO FINAL FOLDER
                    data_out.to_csv('{0}{1}'.format(g['DATA_FINAL_PATH'], file), index=False, sep='|')
                    logger.info('Successfully converted {0}'.format(file))

                    # REMOVE FILE FROM DROP ZONE
                    os.remove('{0}{1}'.format(g['DATA_DROP_PATH'], file))
                except Exception, e:
                    logger.error('{0}. Could not pre-process {1}'.format(e, file))
                    shutil.move('{0}{1}'.format(g['DATA_DROP_PATH'], file), '{0}{1}'.format(g['DATA_ERROR_PATH'], file))
                    logger.error('{0}. {1} moved to errors folder'.format(e, file))

    # STAGE LOAD - PUSH FILES TO DATABASE FROM DATA_FINAL_PATH
    for file in os.listdir(g['DATA_FINAL_PATH']):

        # DAILY REPORTS
        if '_Daily' in file:
            try:
                # COMPOSE AND EXECUTE COPY COMMAND
                with open('{0}{1}'.format(g['DATA_FINAL_PATH'], file), 'rb') as copy_file:
                    sql_cmd = """COPY stg_daily_trades(trader,sequence_no,account,side,symbol,quantity,price,destination,contra,trade_datetime,bo_account,cusip,liq,order_id,exec_broker,ecn_fee,order_datetime,specialist,commission,bb_trade,sec_fee,batch_id,client_order_id,prime,cover_quantity,userr,settle_date,principal,net_amount,allocation_id,allocation_role,is_clearable,nscc_fee,nasdaq_fee,clearing_fee,nyse_etf_fee,amex_etf_fee,listing_exchange,native_liq,order_received_id,bo_group_id,side_desc,calculated_quantity,calculated_principal,ticket_fee,total_fee,away_ticket,total_cost,calculated_net) FROM STDIN WITH CSV HEADER DELIMITER AS '|'; UPDATE stg_daily_trades SET file_name = '{0}' , file_date = '{1}' WHERE file_name IS NULL AND file_date IS NULL;""".format(file, file[13:21])
                    bulk_copy(sql_cmd, copy_file)

                # LOGGING
                logger.info('Successfully pushed {0} to database.'.format(file))

                # CALCULATE METRICS FOR AUDITING
                file_size = os.path.getsize('{0}{1}'.format(g['DATA_FINAL_PATH'], file))
                num_rows = sum(1 for line in open('{0}{1}'.format(g['DATA_FINAL_PATH'], file))) - 1

                # COMPOSE AND EXECUTE AUDITING RECORD
                sql_cmd = """INSERT INTO etl_daily_trades(file_name, file_size, num_rows) VALUES(%(file_name)s, %(file_size)s, %(num_rows)s);"""
                var_dict = {'file_name': file, 'file_size': file_size, 'num_rows': num_rows}
                etl_update(sql_cmd, var_dict)

                # LOGGING
                logger.info('Successfully wrote auditing record for {0}'.format(file))

                # REMOVE FILE FROM CONVERTED FOLDER
                os.remove('{0}{1}'.format(g['DATA_FINAL_PATH'], file))

                # EXECUTE STORED PROCEDURE FOR STAGE TO FINAL LOAD
                # sql_cmd = """EXECUTE STORED_PROCEDURE_NAME;""".format(file)
                # cur.copy_expert(sql_cmd, copy_file)
                # conn.commit()

            except Exception, e:
                conn.rollback()
                logger.error('{0}. {1} could not be pushed to database'.format(e, file))

        # OPEN POSITION REPORTS
        if '_PosAvgReport' in file:
            try:
                # COMPOSE AND EXECUTE COPY COMMAND
                with open('{0}{1}'.format(g['DATA_FINAL_PATH'], file), 'rb') as copy_file:
                    sql_cmd = """COPY stg_daily_open_positions(account,symbol,pos_qty,pos_avg_price,pos_value) FROM STDIN WITH CSV HEADER DELIMITER AS '|'; UPDATE stg_daily_open_positions SET file_name = '{0}', file_date = '{1}' WHERE file_name IS NULL AND file_date IS NULL;""".format(file, file[20:28])
                    bulk_copy(sql_cmd, copy_file)

                # LOGGING
                logger.info('Successfully pushed {0} to database.'.format(file))

                # CALCULATE METRICS FOR AUDITING
                file_size = os.path.getsize('{0}{1}'.format(g['DATA_FINAL_PATH'], file))
                num_rows = sum(1 for line in open('{0}{1}'.format(g['DATA_FINAL_PATH'], file))) - 1

                # COMPOSE AND EXECUTE AUDITING RECORD
                sql_cmd = """INSERT INTO etl_daily_open_positions(file_name, file_size, num_rows) VALUES(%(file_name)s, %(file_size)s, %(num_rows)s);"""
                var_dict = {'file_name': file, 'file_size': file_size, 'num_rows': num_rows}
                etl_update(sql_cmd, var_dict)

                # LOGGING
                logger.info('Successfully wrote auditing record for {0}'.format(file))

                # REMOVE FILE FROM CONVERTED FOLDER
                os.remove('{0}{1}'.format(g['DATA_FINAL_PATH'], file))

                # EXECUTE STORED PROCEDURE FOR STAGE TO FINAL LOAD
                # sql_cmd = """EXECUTE STORED_PROCEDURE_NAME;""".format(file)
                # cur.copy_expert(sql_cmd, copy_file)
                # conn.commit()

            except Exception, e:
                conn.rollback()
                logger.error('{0}. {1} could not be pushed to database'.format(e, file))

        # P&L REPORTS
        if '_PLReport' in file:
            try:
                # COMPOSE AND EXECUTE COPY COMMAND
                with open('{0}{1}'.format(g['DATA_FINAL_PATH'], file), 'rb') as copy_file:
                    sql_cmd = """COPY stg_daily_pl_report(account,symbol,realized,unrealized,trades,volume,date,ecn_fee,sec_Fee,commission,nasdaq_fee,nscc_Fee,clearing_fee,orders_yielding_exec,position,closing_price,nyse_fee,amex_fee,nasdaq_etf) FROM STDIN WITH CSV HEADER DELIMITER AS '|'; UPDATE stg_daily_pl_report SET file_name = '{0}', file_date = '{1}' WHERE file_name IS NULL AND file_date IS NULL;""".format(file, file[16:24])
                    bulk_copy(sql_cmd, copy_file)

                # LOGGING
                logger.info('Successfully pushed {0} to database.'.format(file))

                # CALCULATE METRICS FOR AUDITING
                file_size = os.path.getsize('{0}{1}'.format(g['DATA_FINAL_PATH'], file))
                num_rows = sum(1 for line in open('{0}{1}'.format(g['DATA_FINAL_PATH'], file))) - 1

                # COMPOSE AND EXECUTE AUDITING RECORD
                sql_cmd = """INSERT INTO etl_daily_pl_report(file_name, file_size, num_rows) VALUES(%(file_name)s, %(file_size)s, %(num_rows)s);"""
                var_dict = {'file_name': file, 'file_size': file_size, 'num_rows': num_rows}
                etl_update(sql_cmd, var_dict)

                # LOGGING
                logger.info('Successfully wrote auditing record for {0}'.format(file))

                # REMOVE FILE FROM CONVERTED FOLDER
                os.remove('{0}{1}'.format(g['DATA_FINAL_PATH'], file))

                # EXECUTE STORED PROCEDURE FOR STAGE TO FINAL LOAD
                conn = ps.connect(host=g['POSTGRES_HOST'],
                      port='5432',
                      user=g['POSTGRES_USER'],
                      password=g['POSTGRES_PWD'],
                      database=g['POSTGRES_DB'])
                cur = conn.cursor()
                sql_cmd = """SELECT stg_to_final_pl_report();"""
                cur.execute(sql_cmd)
                cur.execute("COMMIT")
                logger.info('Successfully ran stage to final logic.')


            except Exception, e:
                conn.rollback()
                logger.error('{0}. {1} could not be pushed to database'.format(e, file))

if __name__ == '__main__':
    etl()

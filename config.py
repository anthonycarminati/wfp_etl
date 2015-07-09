import ConfigParser

config = ConfigParser.ConfigParser()
config.readfp(open('/opt/wfp/wfp_etl/etl_config.conf'))

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
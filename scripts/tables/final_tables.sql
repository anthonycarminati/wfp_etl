CREATE TABLE fact_daily_trades(
  trader VARCHAR(50)
  ,sequence_no VARCHAR(10)
  ,account VARCHAR(50)
  ,side VARCHAR(10)
  ,symbol VARCHAR(25)
  ,quantity DECIMAL --INTEGER
  ,price DECIMAL
  ,destination VARCHAR(25)
  ,contra VARCHAR(10)
  ,trade_datetime TIMESTAMP
  ,bo_account VARCHAR(25)
  ,cusip VARCHAR(25)
  ,liq VARCHAR(10)
  ,order_id VARCHAR(25)
  ,exec_broker VARCHAR(10)
  ,ecn_fee DECIMAL --INTEGER
  ,order_datetime TIMESTAMP
  ,specialist VARCHAR(255)
  ,commission DECIMAL
  ,bb_trade VARCHAR(255)
  ,sec_fee DECIMAL
  ,batch_id VARCHAR(255)
  ,client_order_id VARCHAR(255)
  ,prime VARCHAR(255)
  ,cover_quantity DECIMAL --INTEGER
  ,userr VARCHAR(255)
  ,settle_date DATE
  ,principal DECIMAL --INTEGER
  ,net_amount DECIMAL
  ,allocation_id VARCHAR(255)
  ,allocation_role VARCHAR(255)
  ,is_clearable BOOLEAN
  ,nscc_fee DECIMAL
  ,nasdaq_fee DECIMAL
  ,clearing_fee DECIMAL
  ,nyse_etf_fee DECIMAL
  ,amex_etf_fee DECIMAL
  ,listing_exchange VARCHAR(255)
  ,native_liq VARCHAR(10)
  ,order_received_id VARCHAR(25)
  ,bo_group_id VARCHAR(4)
  ,side_desc VARCHAR(10)
  ,calculated_quantity DECIMAL
  ,calculated_principal DECIMAL
  ,ticket_fee DECIMAL
  ,total_fee DECIMAL
  ,away_ticket DECIMAL
  ,total_cost DECIMAL
  ,calculated_net DECIMAL
  ,file_name VARCHAR(50)
  ,file_date VARCHAR(8)
);
GRANT SELECT ON fact_daily_trades TO wfp_etl;

CREATE TABLE fact_daily_open_positions(
  account VARCHAR(50)
  ,symbol VARCHAR(25)
  ,pos_qty DECIMAL
  ,pos_avg_price DECIMAL
  ,pos_value DECIMAL
  ,file_name VARCHAR(50)
  ,file_date VARCHAR(8)
);
GRANT SELECT ON fact_daily_open_positions TO wfp_etl;

CREATE TABLE fact_daily_pl_report(
  account VARCHAR(50)
  ,symbol VARCHAR(10)
  ,realized DECIMAL
  ,unrealized DECIMAL
  ,trades DECIMAL
  ,volume DECIMAL
  ,date VARCHAR(10)
  ,ecn_fee DECIMAL
  ,sec_Fee DECIMAL
  ,commission DECIMAL
  ,nasdaq_fee DECIMAL
  ,nscc_Fee DECIMAL
  ,clearing_fee DECIMAL
  ,orders_yielding_exec DECIMAL
  ,position DECIMAL
  ,closing_price DECIMAL
  ,nyse_fee DECIMAL
  ,amex_fee DECIMAL
  ,nasdaq_etf DECIMAL
  ,file_name VARCHAR(50)
  ,file_date VARCHAR(8)
);
GRANT SELECT ON fact_daily_pl_report TO wfp_etl;
CREATE TABLE stg_daily_trades(
  trader VARCHAR(255)
  ,sequence_no VARCHAR(255)
  ,account VARCHAR(255)
  ,side VARCHAR(255)
  ,symbol VARCHAR(255)
  ,quantity VARCHAR(255)
  ,price VARCHAR(255)
  ,destination VARCHAR(255)
  ,contra VARCHAR(255)
  ,trade_datetime VARCHAR(255)
  ,bo_account VARCHAR(255)
  ,cusip VARCHAR(255)
  ,liq VARCHAR(255)
  ,order_id VARCHAR(255)
  ,exec_broker VARCHAR(255)
  ,ecn_fee VARCHAR(255)
  ,order_datetime VARCHAR(255)
  ,specialist VARCHAR(255)
  ,commission VARCHAR(255)
  ,bb_trade VARCHAR(255)
  ,sec_fee VARCHAR(255)
  ,batch_id VARCHAR(255)
  ,client_order_id VARCHAR(255)
  ,prime VARCHAR(255)
  ,cover_quantity VARCHAR(255)
  ,userr VARCHAR(255)
  ,settle_date VARCHAR(255)
  ,principal VARCHAR(255)
  ,net_amount VARCHAR(255)
  ,allocation_id VARCHAR(255)
  ,allocation_role VARCHAR(255)
  ,is_clearable VARCHAR(255)
  ,nscc_fee VARCHAR(255)
  ,nasdaq_fee VARCHAR(255)
  ,clearing_fee VARCHAR(255)
  ,nyse_etf_fee VARCHAR(255)
  ,amex_etf_fee VARCHAR(255)
  ,listing_exchange VARCHAR(255)
  ,native_liq VARCHAR(255)
  ,order_received_id VARCHAR(255)
  ,bo_group_id VARCHAR(255)
  ,side_desc VARCHAR(255) --figured out in python
  ,calculated_quantity VARCHAR(255) --calculated in python
  ,calculated_principal VARCHAR(255) --calculated in python
  ,ticket_fee VARCHAR(255) --calculated in python
  ,total_fee VARCHAR(255) --calculated in python
  ,away_ticket VARCHAR(255) --calculated in python
  ,total_cost VARCHAR(255) --calculated in python
  ,calculated_net VARCHAR(255) --calculated in python
  ,file_name	VARCHAR(255)
  ,file_date	VARCHAR(255)
);
GRANT ALL ON stg_daily_trades to wfp_etl;

CREATE TABLE stg_daily_open_positions(
  account VARCHAR(255)
  ,symbol VARCHAR(255)
  ,pos_qty VARCHAR(255)
  ,pos_avg_price VARCHAR(255)
  ,pos_value VARCHAR(255) --calculated in python
  ,file_name VARCHAR(255)
  ,file_date VARCHAR(255)
);
GRANT ALL ON stg_daily_open_positions to wfp_etl;




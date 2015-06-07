CREATE TABLE fact_daily_trades(
  trader VARCHAR(50)
  ,sequence_no VARCHAR(10)
  ,account VARCHAR(50)
  ,side VARCHAR(10)
  ,symbol VARCHAR(25)
  ,quantity INTEGER
  ,price DECIMAL
  ,destination VARCHAR(25)
  ,contra VARCHAR(10)
  ,trade_datetime TIMESTAMP
  ,bo_account VARCHAR(25)
  ,cusip VARCHAR(25)
  ,liq VARCHAR(10)
  ,order_id VARCHAR(25)
  ,exec_broker VARCHAR(10)
  ,ecn_fee INTEGER
  ,order_datetime TIMESTAMP
  ,specialist VARCHAR(255)
  ,commission DECIMAL
  ,bb_trade VARCHAR(255)
  ,sec_fee DECIMAL
  ,batch_id VARCHAR(255)
  ,client_order_id VARCHAR(255)
  ,prime VARCHAR(255)
  ,cover_quantity INTEGER
  ,userr VARCHAR(255)
  ,settle_date DATE
  ,principal INTEGER
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
);


trader
,sequence_no
,account
,side
,symbol
,quantity
,price
,destination
,contra
,trade_datetime
,bo_account
,cusip
,liq
,order_id
,exec_broker
,ecn_fee
,order_datetime
,specialist
,commission
,bb_trade
,sec_fee
,batch_id
,client_order_id
,prime
,cover_quantity
,userr
,settle_date
,principal
,net_amount
,allocation_id
,allocation_role
,is_clearable
,nscc_fee
,nasdaq_fee
,clearing_fee
,nyse_etf_fee
,amex_etf_fee
,listing_exchange
,native_liq
,order_received_id
,bo_group_id
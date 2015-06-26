CREATE OR REPLACE FUNCTION stg_to_final_daily_trades() RETURNS VOID AS '

INSERT INTO fact_daily_trades(
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
)
SELECT
  CAST(trader AS VARCHAR(50))
  ,CAST(sequence_no AS VARCHAR(10))
  ,CAST(account AS VARCHAR(50))
  ,CAST(side AS VARCHAR(10))
  ,CAST(symbol AS VARCHAR(25))
  ,CAST(quantity AS DECIMAL) --INTEGER)
  ,CAST(price AS DECIMAL)
  ,CAST(destination AS VARCHAR(25))
  ,CAST(contra AS VARCHAR(10))
  ,CAST(trade_datetime AS TIMESTAMP)
  ,CAST(bo_account AS VARCHAR(25))
  ,CAST(cusip AS VARCHAR(25))
  ,CAST(liq AS VARCHAR(10))
  ,CAST(order_id AS VARCHAR(25))
  ,CAST(exec_broker AS VARCHAR(10))
  ,CAST(ecn_fee AS DECIMAL) --INTEGER)
  ,CAST(order_datetime AS TIMESTAMP)
  ,CAST(specialist AS VARCHAR(255))
  ,CAST(commission AS DECIMAL)
  ,CAST(bb_trade AS VARCHAR(255))
  ,CAST(sec_fee AS DECIMAL)
  ,CAST(batch_id AS VARCHAR(255))
  ,CAST(client_order_id AS VARCHAR(255))
  ,CAST(prime AS VARCHAR(255))
  ,CAST(cover_quantity AS DECIMAL) --INTEGER)
  ,CAST(userr AS VARCHAR(255))
  ,CAST(settle_date AS DATE)
  ,CAST(principal AS DECIMAL) --INTEGER)
  ,CAST(net_amount AS DECIMAL)
  ,CAST(allocation_id AS VARCHAR(255))
  ,CAST(allocation_role AS VARCHAR(255))
  ,CAST(is_clearable AS BOOLEAN)
  ,CAST(nscc_fee AS DECIMAL)
  ,CAST(nasdaq_fee AS DECIMAL)
  ,CAST(clearing_fee AS DECIMAL)
  ,CAST(nyse_etf_fee AS DECIMAL)
  ,CAST(amex_etf_fee AS DECIMAL)
  ,CAST(listing_exchange AS VARCHAR(255))
  ,CAST(native_liq AS VARCHAR(10))
  ,CAST(order_received_id AS VARCHAR(25))
  ,CAST(bo_group_id AS VARCHAR(4))
FROM stg_daily_trades;

TRUNCATE TABLE stg_daily_trades;

' LANGUAGE SQL;
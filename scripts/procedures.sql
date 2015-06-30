CREATE OR REPLACE FUNCTION stg_to_temp_daily_trades() RETURNS VOID AS '

SELECT
  CAST(trader AS VARCHAR(50))
  ,CAST(sequence_no AS VARCHAR(10))
  ,CAST(account AS VARCHAR(50))
  ,CAST(side AS VARCHAR(10))
  ,CAST(symbol AS VARCHAR(25))
  ,CAST(quantity AS NUMERIC)
  ,CAST(price AS NUMERIC)
  ,CAST(destination AS VARCHAR(25))
  ,CAST(contra AS VARCHAR(10))
  ,CAST(trade_datetime AS TIMESTAMP)
  ,CAST(bo_account AS VARCHAR(25))
  ,CAST(cusip AS VARCHAR(25))
  ,CAST(liq AS VARCHAR(10))
  ,CAST(order_id AS VARCHAR(25))
  ,CAST(exec_broker AS VARCHAR(10))
  ,CAST(ecn_fee AS NUMERIC)
  ,CAST(order_datetime AS TIMESTAMP)
  ,CAST(specialist AS VARCHAR(255))
  ,CAST(commission AS NUMERIC)
  ,CAST(bb_trade AS VARCHAR(255))
  ,CAST(sec_fee AS NUMERIC)
  ,CAST(batch_id AS VARCHAR(255))
  ,CAST(client_order_id AS VARCHAR(255))
  ,CAST(prime AS VARCHAR(255))
  ,CAST(cover_quantity AS NUMERIC)
  ,CAST(userr AS VARCHAR(255))
  ,CAST(settle_date AS DATE)
  ,CAST(principal AS NUMERIC)
  ,CAST(net_amount AS NUMERIC)
  ,CAST(allocation_id AS VARCHAR(255))
  ,CAST(allocation_role AS VARCHAR(255))
  ,CAST(is_clearable AS BOOLEAN)
  ,CAST(nscc_fee AS NUMERIC)
  ,CAST(nasdaq_fee AS NUMERIC)
  ,CAST(clearing_fee AS NUMERIC)
  ,CAST(nyse_etf_fee AS NUMERIC)
  ,CAST(amex_etf_fee AS NUMERIC)
  ,CAST(listing_exchange AS VARCHAR(255))
  ,CAST(native_liq AS VARCHAR(10))
  ,CAST(order_received_id AS VARCHAR(25))
  ,CAST(bo_group_id AS VARCHAR(4))
  ,CAST(CASE WHEN side = 'B' THEN 'Buy' ELSE 'Sell' END AS varchar(10)) AS side_desc
  ,CASE WHEN side = 'B' THEN CAST(quantity AS NUMERIC) ELSE -1 * CAST(quantity AS NUMERIC) END AS calculated_quantity
  ,CASE WHEN side = 'B' THEN CAST(principal AS NUMERIC) ELSE -1 * CAST(principal AS NUMERIC) END AS calculated_principal
  ,CASE WHEN prime IS NULL THEN .0011 * CAST(quantity AS NUMERIC) ELSE CAST(0 AS NUMERIC) END AS ticket_fee --ticket fees
  ,CAST(ecn_fee AS NUMERIC) + CAST(sec_fee AS NUMERIC) + CAST(ticket_fee AS NUMERIC) AS total_fee --all fees
  ,CASE WHEN CAST(commission AS NUMERIC) = 0 THEN CAST(0 AS NUMERIC) ELSE CAST(15 AS NUMERIC) END AS away_ticket --away ticket
  ,CAST(total_fee AS NUMERIC) + CAST(away_ticket AS NUMERIC) AS total_cost --total cost
  ,CAST(calculated_principal AS NUMERIC) - CAST(total_cost AS NUMERIC) AS calculated_net --actual net
  ,CAST(order_datetime::date AS DATE) AS order_date
--INTO temp_daily_trades
FROM stg_daily_trades
LIMIT 1;

' LANGUAGE SQL;


--=======================================================================================================
--=======================================================================================================
--=======================================================================================================

CREATE OR REPLACE FUNCTION temp_to_final_daily_trades() RETURNS VOID AS '

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
  ,CAST(quantity AS NUMERIC) --INTEGER)
  ,CAST(price AS NUMERIC)
  ,CAST(destination AS VARCHAR(25))
  ,CAST(contra AS VARCHAR(10))
  ,CAST(trade_datetime AS TIMESTAMP)
  ,CAST(bo_account AS VARCHAR(25))
  ,CAST(cusip AS VARCHAR(25))
  ,CAST(liq AS VARCHAR(10))
  ,CAST(order_id AS VARCHAR(25))
  ,CAST(exec_broker AS VARCHAR(10))
  ,CAST(ecn_fee AS NUMERIC) --INTEGER)
  ,CAST(order_datetime AS TIMESTAMP)
  ,CAST(specialist AS VARCHAR(255))
  ,CAST(commission AS NUMERIC)
  ,CAST(bb_trade AS VARCHAR(255))
  ,CAST(sec_fee AS NUMERIC)
  ,CAST(batch_id AS VARCHAR(255))
  ,CAST(client_order_id AS VARCHAR(255))
  ,CAST(prime AS VARCHAR(255))
  ,CAST(cover_quantity AS NUMERIC) --INTEGER)
  ,CAST(userr AS VARCHAR(255))
  ,CAST(settle_date AS DATE)
  ,CAST(principal AS NUMERIC) --INTEGER)
  ,CAST(net_amount AS NUMERIC)
  ,CAST(allocation_id AS VARCHAR(255))
  ,CAST(allocation_role AS VARCHAR(255))
  ,CAST(is_clearable AS BOOLEAN)
  ,CAST(nscc_fee AS NUMERIC)
  ,CAST(nasdaq_fee AS NUMERIC)
  ,CAST(clearing_fee AS NUMERIC)
  ,CAST(nyse_etf_fee AS NUMERIC)
  ,CAST(amex_etf_fee AS NUMERIC)
  ,CAST(listing_exchange AS VARCHAR(255))
  ,CAST(native_liq AS VARCHAR(10))
  ,CAST(order_received_id AS VARCHAR(25))
  ,CAST(bo_group_id AS VARCHAR(4))
FROM temp_daily_trades;

TRUNCATE TABLE stg_daily_trades;

' LANGUAGE SQL;
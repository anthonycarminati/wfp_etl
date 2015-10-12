-- Function: stg_to_final_daily_pl_report()

-- DROP FUNCTION stg_to_final_daily_pl_report();

CREATE OR REPLACE FUNCTION stg_to_final_daily_pl_report()
  RETURNS void AS
$BODY$

INSERT INTO fact_daily_pl_report(
  account
  ,symbol
  ,realized
  ,unrealized
  ,trades
  ,volume
  ,date
  ,ecn_fee
  ,sec_Fee
  ,commission
  ,nasdaq_fee
  ,nscc_Fee
  ,clearing_fee
  ,orders_yielding_exec
  ,position
  ,closing_price
  ,nyse_fee
  ,amex_fee
  ,nasdaq_etf
  ,file_name
  ,file_date
)
SELECT
  CAST(account AS VARCHAR(50))
  ,CAST(symbol AS VARCHAR(10))
  ,CAST(realized AS DECIMAL)
  ,CAST(unrealized AS DECIMAL)
  ,CAST(trades AS DECIMAL)
  ,CAST(volume AS DECIMAL)
  ,CAST(date AS VARCHAR(10))
  ,CAST(ecn_fee AS DECIMAL)
  ,CAST(sec_Fee AS DECIMAL)
  ,CAST(commission AS DECIMAL)
  ,CAST(nasdaq_fee AS DECIMAL)
  ,CAST(nscc_Fee AS DECIMAL)
  ,CAST(clearing_fee AS DECIMAL)
  ,CAST(orders_yielding_exec AS DECIMAL)
  ,CAST(position AS DECIMAL)
  ,CAST(closing_price AS DECIMAL)
  ,CAST(nyse_fee AS DECIMAL)
  ,CAST(amex_fee AS DECIMAL)
  ,CAST(nasdaq_etf AS DECIMAL)
  ,CAST(file_name AS VARCHAR(50))
  ,CAST(file_date AS VARCHAR(8))
FROM stg_daily_pl_report;

$BODY$
  LANGUAGE sql VOLATILE
  COST 100;
ALTER FUNCTION stg_to_final_daily_pl_report()
  OWNER TO wfp_admin;

--##########################################################################################
--##########################################################################################

-- Function: truncate_stg_daily_pl_report()

-- DROP FUNCTION truncate_stg_daily_pl_report();

CREATE OR REPLACE FUNCTION truncate_stg_daily_pl_report()
  RETURNS void AS
$BODY$

TRUNCATE TABLE stg_daily_pl_report;

$BODY$
  LANGUAGE sql VOLATILE
  COST 100;
ALTER FUNCTION truncate_stg_daily_pl_report()
  OWNER TO wfp_admin;

-- Function: stg_to_final_daily_open_positions()

-- DROP FUNCTION stg_to_final_daily_open_positions();

CREATE OR REPLACE FUNCTION stg_to_final_daily_open_positions()
  RETURNS void AS
$BODY$

INSERT INTO fact_daily_open_positions(
  account
  ,symbol
  ,pos_qty
  ,pos_avg_price
  ,pos_value
  ,file_name
  ,file_date
)
SELECT
  CAST(account AS VARCHAR(50))
  ,CAST(symbol AS VARCHAR(25))
  ,CAST(pos_qty AS DECIMAL)
  ,CAST(pos_avg_price AS DECIMAL)
  ,CAST(pos_value AS DECIMAL)
  ,CAST(file_name AS VARCHAR(50))
  ,CAST(file_date AS VARCHAR(8))
FROM stg_daily_open_positions;

$BODY$
  LANGUAGE sql VOLATILE
  COST 100;
ALTER FUNCTION stg_to_final_daily_open_positions()
  OWNER TO wfp_admin;

--##########################################################################################
--##########################################################################################

-- Function: truncate_stg_daily_open_positions()

-- DROP FUNCTION truncate_stg_daily_open_positions();

CREATE OR REPLACE FUNCTION truncate_stg_daily_open_positions()
  RETURNS void AS
$BODY$

TRUNCATE TABLE stg_daily_open_positions;

$BODY$
  LANGUAGE sql VOLATILE
  COST 100;
ALTER FUNCTION truncate_stg_daily_open_positions()
  OWNER TO wfp_admin;

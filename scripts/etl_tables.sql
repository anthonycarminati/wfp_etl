CREATE TABLE etl_daily_trades(
  file_name VARCHAR(100)
  ,file_size INT
  ,num_rows INT
  ,load_to_stage_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  ,load_to_final_time TIMESTAMP
  ,CONSTRAINT pk_file_name PRIMARY KEY (file_name)
);
GRANT ALL ON etl_daily_trades TO wfp_etl;

CREATE TABLE etl_daily_open_positions(
  file_name VARCHAR(100)
  ,file_size INT
  ,num_rows INT
  ,load_to_stage_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  ,load_to_final_time TIMESTAMP
  ,CONSTRAINT pk_file_name PRIMARY KEY (file_name)
);
GRANT ALL ON etl_daily_open_positions TO wfp_etl;

CREATE TABLE etl_daily_pl_report(
  file_name VARCHAR(100)
  ,file_size INT
  ,num_rows INT
  ,load_to_stage_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  ,load_to_final_time TIMESTAMP
  ,CONSTRAINT pk_file_name PRIMARY KEY (file_name)
);
GRANT ALL ON etl_daily_pl_report TO wfp_etl;
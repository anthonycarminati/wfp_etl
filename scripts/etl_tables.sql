CREATE TABLE etl_daily_trades(
  file_name VARCHAR(20)
  ,file_size INT
  ,num_rows INT
  ,load_to_stage_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  ,load_to_final_time TIMESTAMP
  ,CONSTRAINT pk_file_name PRIMARY KEY (file_name)
);
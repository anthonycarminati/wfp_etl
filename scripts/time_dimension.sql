SELECT
	datum AS date,
	EXTRACT(YEAR FROM datum) AS year,
	EXTRACT(MONTH FROM datum) AS month,
	-- Localized month name
	to_char(datum, 'TMMonth') AS month_name,
	EXTRACT(DAY FROM datum) AS day,
	EXTRACT(doy FROM datum) AS day_of_year,
	-- Localized weekday
	to_char(datum, 'TMDay') AS weekday_name,
	-- ISO calendar week
	EXTRACT(week FROM datum) AS calendar_week,
	to_char(datum, 'dd. mm. yyyy') AS formatted_date,
	'Q' || to_char(datum, 'Q') AS quartal,
	to_char(datum, 'yyyy/"Q"Q') AS year_quartal,
	to_char(datum, 'yyyy/mm') AS year_month,
	-- ISO calendar year and week
	to_char(datum, 'iyyy/IW') AS year_calendar_week,
	-- Weekend
	CASE WHEN EXTRACT(isodow FROM datum) IN (6, 7) THEN 'Weekend' ELSE 'Weekday' END AS Weekend,
	-- Fixed holidays
  CASE WHEN to_char(datum, 'MMDD') IN ('0101', '0704', '1225', '1226') THEN 'Holiday' ELSE 'No holiday' END AS AmericanHoliday,
	-- Some periods of the year, adjust for your organisation and country
	--CASE WHEN to_char(datum, 'MMDD') BETWEEN '1115' AND '1225' THEN 'Christmas season'
	--     WHEN to_char(datum, 'MMDD') > '1225' OR to_char(datum, 'MMDD') <= '0106' THEN 'Winter break'
  --	ELSE 'Normal' END AS Per
	-- ISO start and end of the week of this date
	datum + (1 - EXTRACT(isodow FROM datum))::INTEGER AS calendar_week_start,
	datum + (7 - EXTRACT(isodow FROM datum))::INTEGER AS calendar_week_end,
	-- Start and end of the month of this date
	datum + (1 - EXTRACT(DAY FROM datum))::INTEGER AS month_start,
	(datum + (1 - EXTRACT(DAY FROM datum))::INTEGER + '1 month'::INTERVAL)::DATE - '1 day'::INTERVAL AS month_end
FROM (
	-- There are 3 leap years in this range, so calculate 365 * 10 + 3 records
	SELECT '2000-01-01'::DATE + SEQUENCE.DAY AS datum
	FROM generate_series(0,3652) AS SEQUENCE(DAY)
	GROUP BY SEQUENCE.DAY
     ) DQ
ORDER BY 1;
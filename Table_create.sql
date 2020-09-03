-- Creating Date dimention
DROP TABLE if exists d_date;
CREATE TABLE d_date
(
  date_dim_id              INT NOT NULL,
  date_actual              DATE NOT NULL,
  day_of_month             INT NOT NULL,
  month_actual             INT NOT NULL,
  year_actual              INT NOT NULL,
  mmyyyy                   CHAR(6) NOT NULL,
  mmddyyyy                 CHAR(10) NOT NULL,
  weekend_indr             BOOLEAN NOT NULL
);

ALTER TABLE public.d_date ADD CONSTRAINT d_date_date_dim_id_pk PRIMARY KEY (date_dim_id);
CREATE INDEX d_date_date_actual_idx
  ON d_date(date_actual);
COMMIT;
INSERT INTO d_date
SELECT TO_CHAR(datum, 'yyyymmdd')::INT AS date_dim_id,
       datum AS date_actual,
       EXTRACT(DAY FROM datum) AS day_of_month,
       EXTRACT(MONTH FROM datum) AS month_actual,
       EXTRACT(ISOYEAR FROM datum) AS year_actual,
       TO_CHAR(datum, 'mmyyyy') AS mmyyyy,
       TO_CHAR(datum, 'mmddyyyy') AS mmddyyyy,
       CASE
           WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE
           ELSE FALSE
           END AS weekend_indr
FROM (SELECT '2020-01-01'::DATE + SEQUENCE.DAY AS datum
      FROM GENERATE_SERIES(0, 29219) AS SEQUENCE (DAY)
      GROUP BY SEQUENCE.DAY) DQ
ORDER BY 1;
COMMIT;

-- create staging area for sources; weather stations:

CREATE TABLE public.stage_sources (
	stage_id serial NOT NULL,
	id text NULL,
	"name" text NULL,
	frost_long numeric(4,2) NULL,
	frost_lat numeric(4,2) NULL,
	CONSTRAINT stage_sources_pkey PRIMARY KEY (stage_id)
);

-- create staging area for ruter_stops: 

CREATE TABLE public.stage_stops (
	stop_id text NULL,
	stop_name text NULL,
	stop_lat float8 NULL,
	stop_lon float8 NULL
);

-- create table where weather stations in range of ruter coordinates to reduce 'search area' for closest weather station: 

CREATE TABLE weather_station
AS 
SELECT id, name, frost_long, frost_lat from stage_sources where (frost_lat between 59.42 and 60.55) and (frost_long between 10.10  and 11.87);


-- create table for closest weather station for each ruter stop:
create table closest_weather_station(
	id serial primary key, 
	ruter_stop text, 
	weather_stat_id text
);









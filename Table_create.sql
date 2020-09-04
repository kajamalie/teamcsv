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


-- create table for closest weather station for each ruter stop:
create table closest_weather_station(
	id serial primary key, 
	ruter_stop text, 
	weather_stat_id text
);

-- create staging area for observations:

CREATE TABLE public.stage_observations2 (
	observation_id serial NOT NULL,
	weather_stat_id text NULL,
	"time" time NULL,
	"date" text NULL,
	temperature float8 NULL,
	percipitation float8 NULL,
	CONSTRAINT stage_observations2_pkey PRIMARY KEY (observation_id)
);

--For starschema: 
--Create and populate timedimention: 
CREATE table star_times (
    id int4 NOT NULL,
    time time,
    hour int2,
    military_hour int2,
    minute int4,
    second int4,
    minute_of_day int4,
    second_of_day int4,
    quarter_hour varchar,
    am_pm varchar,
    day_night varchar,
    day_night_abbrev varchar,
    time_period varchar,
    time_period_abbrev varchar
)
WITH (OIDS=FALSE);

INSERT INTO star_times
SELECT
  to_char(datum, 'HH24MISS')::integer AS id,
  datum::time AS time,
  to_char(datum, 'HH12')::integer AS hour,
  to_char(datum, 'HH24')::integer AS military_hour,
  extract(minute FROM datum)::integer AS minute,
  extract(second FROM datum) AS second,
  to_char(datum, 'SSSS')::integer / 60 AS minute_of_day,
  to_char(datum, 'SSSS')::integer AS second_of_day,
  to_char(datum - (extract(minute FROM datum)::integer % 15 || 'minutes')::interval, 'hh24:mi') ||
  ' ï¿½ ' ||
  to_char(datum - (extract(minute FROM datum)::integer % 15 || 'minutes')::interval + '14 minutes'::interval, 'hh24:mi')
    AS quarter_hour,
  to_char(datum, 'AM') AS am_pm,
  CASE WHEN to_char(datum, 'hh24:mi') BETWEEN '08:00' AND '19:59' THEN 'Day (8AM-8PM)' ELSE 'Night (8PM-8AM)' END
  AS day_night,
  CASE WHEN to_char(datum, 'hh24:mi') BETWEEN '08:00' AND '19:59' THEN 'Day' ELSE 'Night' END
  AS day_night_abbrev,
  CASE
  WHEN to_char(datum, 'hh24:mi') BETWEEN '00:00' AND '03:59' THEN 'Late Night (Midnight-4AM)'
  WHEN to_char(datum, 'hh24:mi') BETWEEN '04:00' AND '07:59' THEN 'Early Morning (4AM-8AM)'
  WHEN to_char(datum, 'hh24:mi') BETWEEN '08:00' AND '11:59' THEN 'Morning (8AM-Noon)'
  WHEN to_char(datum, 'hh24:mi') BETWEEN '12:00' AND '15:59' THEN 'Afternoon (Noon-4PM)'
  WHEN to_char(datum, 'hh24:mi') BETWEEN '16:00' AND '19:59' THEN 'Evening (4PM-8PM)'
  WHEN to_char(datum, 'hh24:mi') BETWEEN '20:00' AND '23:59' THEN 'Night (8PM-Midnight)'
  END AS time_period,
  CASE
  WHEN to_char(datum, 'hh24:mi') BETWEEN '00:00' AND '03:59' THEN 'Late Night'
  WHEN to_char(datum, 'hh24:mi') BETWEEN '04:00' AND '07:59' THEN 'Early Morning'
  WHEN to_char(datum, 'hh24:mi') BETWEEN '08:00' AND '11:59' THEN 'Morning'
  WHEN to_char(datum, 'hh24:mi') BETWEEN '12:00' AND '15:59' THEN 'Afternoon'
  WHEN to_char(datum, 'hh24:mi') BETWEEN '16:00' AND '19:59' THEN 'Evening'
  WHEN to_char(datum, 'hh24:mi') BETWEEN '20:00' AND '23:59' THEN 'Night'
  END AS time_period_abbrev
FROM generate_series('2000-01-01 00:00:00'::timestamp, '2000-01-01 23:59:59'::timestamp, '1 hour') datum;



-- create place dim: bus_stop   
SELECT stop_id, stop_name
INTO csv_data.bus_stop
FROM public.stage_stops ss;  

ALTER TABLE csv_data.bus_stop 
   ADD column s_stop_id serial primary key;


-- create place dim: weather_station
SELECT id as source_id, name as source_name
INTO csv_data.weather_station
FROM public.weather_station;

ALTER TABLE csv_data.weather_station 
   ADD column s_stop_id serial primary key;
 

 -- create fact table:  
create table csv_data.fact ( 
  	   fact_id serial primary key,
       s_stop_id int references  csv_data.bus_stop(s_stop_id),
       s_weather_id int references csv_data.weather_station(s_weather_id),
       time_id time references csv_data.star_times(time), 
       date_id varchar,
       temperature varchar, 
       precipitation varchar
      ); 
 
    --populere fact table:
    
   with sources as (select ws.ruter_stop, ws.weather_stat_id, so."date", so.observation_id, so.percipitation, so.temperature, so."time"
              from public.closest_weather_station ws
              join public.stage_observations2 so using(weather_stat_id)
              join csv_data.weather_station ws2 on so.weather_stat_id = ws2.source_id) 
        insert into csv_data.fact (s_stop_id, s_weather_id, time_id, date_id, temperature, precipitation)
               select bs.s_stop_id, ws2.s_weather_id, s."time", s."date", s.temperature, s.percipitation 
               from sources s
               join csv_data.bus_stop bs on s.ruter_stop = bs.stop_name 
               join csv_data.weather_station ws2 on s.weather_stat_id = ws2.source_id 
               join csv_data.star_times st on s."time" = st."time" ;
                --join csv_data.star_date sd on s."date" = sd.date_actual; 
                








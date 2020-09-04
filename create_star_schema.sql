--For starschema: 

--Create and populate timedimention: 

drop table csv_data.date_time; 

CREATE TABLE csv_data.date_time
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

ALTER TABLE csv_data.date_time ADD CONSTRAINT d_date_date_dim_id_pk PRIMARY KEY (date_actual);
CREATE index csv_data.date_time_date_actual_idx
  ON d_date(date_actual);
COMMIT;

INSERT into date_time
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







--create time-dim by hour: 
create table csv_data.time_hour(
       date_hour varchar); 
      
 drop table csv_data.time_hour;
      


insert into csv_data.time_hour
       values 
('T00:00:00.000Z'), ('T01:00:00.000Z'), ('T02:00:00.000Z'),
('T03:00:00.000Z'), ('T04:00:00.000Z'), ('T05:00:00.000Z'),
('T06:00:00.000Z'), ('T07:00:00.000Z'), ('T08:00:00.000Z'),
('T09:00:00.000Z'), ('T10:00:00.000Z'), ('T11:00:00.000Z'),
('T12:00:00.000Z'), ('T13:00:00.000Z'), ('T14:00:00.000Z'),
('T15:00:00.000Z'), ('T16:00:00.000Z'), ('T17:00:00.000Z'),
('T18:00:00.000Z'), ('T19:00:00.000Z'),('T20:00:00.000Z'),
('T21:00:00.000Z'), ('T22:00:00.000Z'), ('T23:00:00.000Z'),
('T24:00:00.000Z');
      
alter table csv_data.time_hour
      add primary key(date_hour);

-- hvis vi vil gjøre den om til en dag:
      
insert into csv_data.time_hour
       values 
('2020-09-02T00:00:00.000Z'), ('2020-09-02T01:00:00.000Z'), ('2020-09-02T02:00:00.000Z'),
('2020-09-02T03:00:00.000Z'), ('2020-09-02T04:00:00.000Z'), ('2020-09-02T05:00:00.000Z'),
('2020-09-02T06:00:00.000Z'), ('2020-09-02T07:00:00.000Z'), ('2020-09-02T08:00:00.000Z'),
('2020-09-02T09:00:00.000Z'), ('2020-09-02T10:00:00.000Z'), ('2020-09-02T11:00:00.000Z'),
('2020-09-02T12:00:00.000Z'), ('2020-09-02T13:00:00.000Z'), ('2020-09-02T14:00:00.000Z'),
('2020-09-02T15:00:00.000Z'), ('2020-09-02T16:00:00.000Z'), ('2020-09-02T17:00:00.000Z'),
('2020-09-02T18:00:00.000Z'), ('2020-09-02T19:00:00.000Z'),('2020-09-02T20:00:00.000Z'),
('2020-09-02T21:00:00.000Z'), ('2020-09-02T22:00:00.000Z'), ('2020-09-02T23:00:00.000Z'),
('2020-09-02T24:00:00.000Z');





--Nyde tidstabeller: 
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
TRUNCATE TABLE star.times;
-- Unknown member
INSERT INTO star.times VALUES (
    -1, --id
    '0:0:0', -- time
    0, -- hour
    0, -- military_hour
    0, -- minute
    0, -- second
    0, -- minute_of_day
    0, -- second_of_day
    'Unknown', -- quarter_hour
    'Unknown', -- am_pm
    'Unknown', -- day_night
    'Unk', -- day_night_abbrev
    'Unknown', -- time_period
    'Unk' -- time_period_abbrev
);
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
  ' – ' ||
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

ALTER TABLE  csv_data.star_times 
   ADD column s_times_id serial primary key;
-- Dimension: Date
-- PK: k_date (YYYYMMDD)
-- Variable: all different time formats :-)
DROP TABLE if exists star.date_s;
CREATE TABLE star_date
(
  k_date              INT NOT NULL,
  date_actual              DATE NOT NULL,
  epoch                    BIGINT NOT NULL,
  day_suffix               VARCHAR(4) NOT NULL,
  day_name                 VARCHAR(9) NOT NULL,
  day_of_week              INT NOT NULL,
  day_of_month             INT NOT NULL,
  day_of_quarter           INT NOT NULL,
  day_of_year              INT NOT NULL,
  week_of_month            INT NOT NULL,
  week_of_year             INT NOT NULL,
  week_of_year_iso         CHAR(10) NOT NULL,
  month_actual             INT NOT NULL,
  month_name               VARCHAR(9) NOT NULL,
  month_name_abbreviated   CHAR(3) NOT NULL,
  quarter_actual           INT NOT NULL,
  quarter_name             VARCHAR(9) NOT NULL,
  year_actual              INT NOT NULL,
  first_day_of_week        DATE NOT NULL,
  last_day_of_week         DATE NOT NULL,
  first_day_of_month       DATE NOT NULL,
  last_day_of_month        DATE NOT NULL,
  first_day_of_quarter     DATE NOT NULL,
  last_day_of_quarter      DATE NOT NULL,
  first_day_of_year        DATE NOT NULL,
  last_day_of_year         DATE NOT NULL,
  mmyyyy                   CHAR(6) NOT NULL,
  mmddyyyy                 CHAR(10) NOT NULL,
  weekend_indr             BOOLEAN NOT NULL
);
ALTER TABLE star_date ADD CONSTRAINT date_s_k_date_pk PRIMARY KEY (k_date);
CREATE INDEX date_s_date_actual_idx
  ON star.date_s(date_actual);
COMMIT;
INSERT INTO star_date
SELECT TO_CHAR(datum, 'yyyymmdd')::INT AS k_date,
       datum AS date_actual,
       EXTRACT(EPOCH FROM datum) AS epoch,
       TO_CHAR(datum, 'fmDDth') AS day_suffix,
       TO_CHAR(datum, 'Day') AS day_name,
       EXTRACT(ISODOW FROM datum) AS day_of_week,
       EXTRACT(DAY FROM datum) AS day_of_month,
       datum - DATE_TRUNC('quarter', datum)::DATE + 1 AS day_of_quarter,
       EXTRACT(DOY FROM datum) AS day_of_year,
       TO_CHAR(datum, 'W')::INT AS week_of_month,
       EXTRACT(WEEK FROM datum) AS week_of_year,
       EXTRACT(ISOYEAR FROM datum) || TO_CHAR(datum, '"-W"IW-') || EXTRACT(ISODOW FROM datum) AS week_of_year_iso,
       EXTRACT(MONTH FROM datum) AS month_actual,
       TO_CHAR(datum, 'Month') AS month_name,
       TO_CHAR(datum, 'Mon') AS month_name_abbreviated,
       EXTRACT(QUARTER FROM datum) AS quarter_actual,
       CASE
           WHEN EXTRACT(QUARTER FROM datum) = 1 THEN 'First'
           WHEN EXTRACT(QUARTER FROM datum) = 2 THEN 'Second'
           WHEN EXTRACT(QUARTER FROM datum) = 3 THEN 'Third'
           WHEN EXTRACT(QUARTER FROM datum) = 4 THEN 'Fourth'
           END AS quarter_name,
       EXTRACT(ISOYEAR FROM datum) AS year_actual,
       datum + (1 - EXTRACT(ISODOW FROM datum))::INT AS first_day_of_week,
       datum + (7 - EXTRACT(ISODOW FROM datum))::INT AS last_day_of_week,
       datum + (1 - EXTRACT(DAY FROM datum))::INT AS first_day_of_month,
       (DATE_TRUNC('MONTH', datum) + INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month,
       DATE_TRUNC('quarter', datum)::DATE AS first_day_of_quarter,
       (DATE_TRUNC('quarter', datum) + INTERVAL '3 MONTH - 1 day')::DATE AS last_day_of_quarter,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-01-01', 'YYYY-MM-DD') AS first_day_of_year,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-12-31', 'YYYY-MM-DD') AS last_day_of_year,
       TO_CHAR(datum, 'mmyyyy') AS mmyyyy,
       TO_CHAR(datum, 'mmddyyyy') AS mmddyyyy,
       CASE
           WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE
           ELSE FALSE
           END AS weekend_indr
FROM (SELECT '2000-01-01'::DATE + SEQUENCE.DAY AS datum
      FROM GENERATE_SERIES(0, 29219) AS SEQUENCE (DAY)
      GROUP BY SEQUENCE.DAY) DQ
ORDER BY 1;


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

select * from  csv_data.weather_station ws ;

--drop table csv_data.weather_station;



ALTER TABLE  csv_data.weather_station
   ADD column s_weather_id serial primary key;
  
    

 -- create fact table: 
   
create table csv_data.fact ( 
  	   fact_id serial primary key,
       s_stop_id int references  csv_data.bus_stop(s_stop_id),
       s_weather_id int references csv_data.weather_station(s_weather_id),
       time_hour int references csv_data.star_times(s_times_id), 
       date_actual int references csv_data.star_date(k_date),
       temperature varchar, 
       precipitation varchar
      ); 


   

    --start på join for å populere fact_table:

  --med startimes         
with sourses as (select so.weather_stat_id, so.timereference, so.temperature, so.percipitation, ws.s_weather_id, csw.ruter_stop, SO.timereference 
                 from public.stage_observations so 
                 join csv_data.weather_station ws on so.weather_stat_id = ws.source_id
                 join public.closest_weather_station csw on ws.source_id = csw.weather_stat_id)    
insert into csv_data.fact (s_stop_id, s_weather_id, temperature, precipitation)
            select bs.s_stop_id,  ws.s_weather_id, s.temperature, s.percipitation 
            from sourses s
            join csv_data.bus_stop bs on s.ruter_stop = bs.stop_name 
            join csv_data.weather_station ws using(s_weather_id)
            --join csv_data.star_times st st on references (s_times_id)
            --join csv_data.star_date sd using(k_date); 
           
            
            --til når vi skal få inn tider:
             st."hour", sd.date_actual,
           time_hour, date_actual,
           
           
           select * from csv_data.fact f ;
          select * from public.stage_observations so ;
         select * from csv_data.weather_station ws ;
        
        select * from csv_data.star_date sd ;
       select * from csv_data.star_times st ;
      
      
      
      
   with sources as (select so.weather_stat_id, so.timereference, so.temperature, so.percipitation, ws.s_weather_id, csw.ruter_stop, SO.timereference 
                 from public.stage_observations so 
                 join csv_data.weather_station ws on so.weather_stat_id = ws.source_id
                 join public.closest_weather_station csw on ws.source_id = csw.weather_stat_id)
        insert into csv_data.fact (s_stop_id, s_weather_id, time_hour, date_actual, temperature, precipitation)
               from sources s
               join 
                 ;
                
                
                
   select ws.ruter_stop, ws.weather_stat_id, so."date", so.observation_id, so.percipitation, so.temperature, so."time", so.weather_stat_id 
              from public.closest_weather_station ws
              join public.stage_observations2 so using(weather_stat_id); 
          
           

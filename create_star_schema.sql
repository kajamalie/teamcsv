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
       date_hour varchar references csv_data.time_hour(date_hour), 
       date_actual date references csv_data.date_time(date_actual),
       temperature varchar, 
       precipitation varchar
      ); 
     
     


    --start på join for å populere fact_table:
    
     
     
     
with sourses as (select so.weather_stat_id, so.timereference, so.temperature, so.percipitation, ws.s_weather_id, csw.ruter_stop, SO.timereference 
                 from public.stage_observations so 
                 join csv_data.weather_station ws on so.weather_stat_id = ws.source_id
                 join public.closest_weather_station csw on ws.source_id = csw.weather_stat_id)    
insert into csv_data.fact (s_stop_id, s_weather_id, date_hour, date_actual, temperature, precipitation)
            select bs.s_stop_id,  ws.s_weather_id, so.temperature, so.percipitation 
            from sourses s
            join csv_data.bus_stop bs on s.ruter_stop = bs.stop_name 
            join csv_data.weather_station ws using(s_weather_id)
            join csv_data.time_hour th on th.date_hour = s.timereference
            join csv_data.date_time dt using(date_actual); 
           
       --uten tider(denne er kjørt nå):   
           
with sourses as (select so.weather_stat_id, so.temperature, so.percipitation, ws.s_weather_id, csw.ruter_stop 
                 from public.stage_observations so 
                 join csv_data.weather_station ws on so.weather_stat_id = ws.source_id
                 join public.closest_weather_station csw on ws.source_id = csw.weather_stat_id)    
insert into csv_data.fact (s_stop_id, s_weather_id, temperature, precipitation)
            select bs.s_stop_id,  ws.s_weather_id, s.temperature, s.percipitation 
            from sourses s
            join csv_data.bus_stop bs on s.ruter_stop = bs.stop_name 
            join csv_data.weather_station ws using(s_weather_id)
            --join csv_data.time_hour th on th.date_hour = s.timereference
            --join csv_data.date_time dt using(date_actual); 
           
  



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
SELECT id, ruter_stop 
INTO csv_data.weather_station
FROM public.closest_weather_station; 

ALTER TABLE csv_data.weather_station 
   ADD column s_weather_id serial primary key;
  
  public.
 -- create fact table: 
   
create table csv_data.fact ( 
  	   fact_id serial primary key,
       s_stop_id int references  csv_data.bus_stop(s_stop_id),
       s_weather_id int references csv_data.weather_station(s_weather_id),
       time_id varchar references csv_data.time_hour(date_hour), 
       date_actual date references csv_data.date_time(date_actual)
      ); 
     
     
   
     


    --start på join for å populere fact_table:
  select ss.stop_id, csw.ruter_stop, ws.id as weather_id, bs.stop_name, ws2.s_weather_id from public.stage_stops ss
         join public.closest_weather_station csw on ss.stop_name = csw.ruter_stop 
         join public.weather_station ws on csw.weather_stat_id = ws.id 
         join csv_data.bus_stop bs on ss.stop_id = bs.stop_id 
         join csv_data.weather_station ws2 on csw.id = ws2.s_weather_id ; 
         
        






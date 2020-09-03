--For starschema: 

--Create and populate timedimention: 

CREATE TABLE csv_data.d_date
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

ALTER TABLE csv_data.d_date ADD CONSTRAINT d_date_date_dim_id_pk PRIMARY KEY (date_dim_id);
CREATE INDEX d_date_date_actual_idx
  ON d_date(date_actual);
COMMIT;

INSERT INTO csv_data.d_date
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
       date_hour varchar
       

-- create place dim: 
 create table csv_data.buss_stop (
      s_stop_id serial primary key, 
      stop_id varchar, 
      stop_name varchar,
      stop_lat float, 
      stop_lon float
 )
 
 
insert into csv_data.buss_stop (stop_id, stop_name, stop_lat,  stop_lon)
       values (select * from public.stage_stops);
       
--if we have a table directly from staging area: 
 
SELECT stop_id, stop_name, stop_lat, stop_lon
INTO csv_data.buss_stop
FROM public.stage_stops ss;  
WHERE condition; 


ALTER TABLE csv_data.buss_stop 
   ADD column s_stop_id serial primary key;

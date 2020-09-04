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








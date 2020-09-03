# -*- coding: utf-8 -*-
"""
Created on Mon Aug 31 14:44:12 2020

@author: Kaja Amalie
"""



#%%

import pandas
import os 
import numpy as np
import psycopg2# Connect to your postgres DB
# Connect to an existing database
conn = psycopg2.connect(host = "ds-etl-academy.cgbivchwjzle.eu-west-1.rds.amazonaws.com",
                        dbname = "team_csv",
                        user = "student_kaja",
                        password = "osama",
                        port = 5432
                       )
cur = conn.cursor()

conn.commit()

#%%
#stenge av database

cur.close()
conn.close()


#%%

#lage engine 

import pandas as pd
from sqlalchemy.types import Integer
from sqlalchemy import create_engine
hostname = "ds-etl-academy.cgbivchwjzle.eu-west-1.rds.amazonaws.com"
user = "student_kaja"
password = "osama"
dbname = "team_csv"
constring = f"postgresql+psycopg2://{user}:{password}@{hostname}/{dbname}"
engine = create_engine(constring, echo=False)  




connection = engine.raw_connection()


#%%

#(connecting directly to API)
# create table buss_stops:
    
import requests
import os
from zipfile import ZipFile 
import pandas as pd

def get_data_stops():
    url = 'https://storage.googleapis.com/marduk-production/outbound/gtfs/rb_rut-aggregated-gtfs.zip'
    myfile = requests.get(url)
    current_folder = os.getcwd()
    open(current_folder + '/routes.zip', 'wb').write(myfile.content)
    stops = pd.DataFrame()
    with ZipFile('routes.zip') as myzip:
        with myzip.open('stops.txt') as myfile:
            stops = pd.read_csv(myfile)
            columns = ['stop_id','stop_name','stop_lat','stop_lon']
            stops_data = stops[columns].copy()
           #stops.to_csv('stops.csv') #denne lager CSV 
    return(stops_data)



data = get_data_stops()
data.to_sql('stage_stops', con = engine, index=False, schema='public', if_exists='append')


#%%
#create table in SQL routes 
def get_data_routes():
    url = 'https://storage.googleapis.com/marduk-production/outbound/gtfs/rb_rut-aggregated-gtfs.zip'
    myfile = requests.get(url)
    current_folder = os.getcwd()
    open(current_folder + '/routes.zip', 'wb').write(myfile.content)
    stops = pd.DataFrame()
    with ZipFile('routes.zip') as myzip:
        with myzip.open('routes.txt') as myfile:
            routs = pd.read_csv(myfile)
            columns = ['route_id', 'route_short_name','route_long_name']  #'route_id','route_short_name','route_long_name', 'route_type']
            routs_data = routs[columns].copy()
           #routes.to_csv('routes.csv') #denne lager CSV 
    return(routs_data)

data2 = get_data_routes()
data2.to_sql('stage_routes', con = engine, index=False, schema='public', if_exists='append')



#%%
#Create table in SQL shapes 

shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence,shape_dist_traveled

def get_data_shapes():
    url = 'https://storage.googleapis.com/marduk-production/outbound/gtfs/rb_rut-aggregated-gtfs.zip'
    myfile = requests.get(url)
    current_folder = os.getcwd()
    open(current_folder + '/routes.zip', 'wb').write(myfile.content)
    stops = pd.DataFrame()
    with ZipFile('routes.zip') as myzip:
        with myzip.open('shapes.txt') as myfile:
            shapes = pd.read_csv(myfile)
            columns = ['shape_id', 'shape_pt_lat','shape_pt_lon']  #'route_id','route_short_name','route_long_name', 'route_type']
            shapes_data = shapes[columns].copy()
           #shapes.to_csv('shapes.csv') #denne lager CSV 
    return(shapes_data)

data3 = get_data_shapes()
data3.to_sql('stage_shapes', con = engine, index=False, schema='public', if_exists='append')


#%%
# set inn verdier gradvis
my_big_dataframe = data3
batchsize = 1000
for i in range(0, len(my_big_dataframe), batchsize):
    smaller_dataframe = my_big_dataframe[i:i+batchsize]
    smaller_dataframe.to_sql('stage_shapes_2', con = engine, index=False, schema='public', if_exists='append')
    
    
#%%
#Create table in SQL routs 

shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence,shape_dist_traveled

def get_data_stop_times():
    url = 'https://storage.googleapis.com/marduk-production/outbound/gtfs/rb_rut-aggregated-gtfs.zip'
    myfile = requests.get(url)
    current_folder = os.getcwd()
    open(current_folder + '/routes.zip', 'wb').write(myfile.content)
    sto = pd.DataFrame()
    with ZipFile('routes.zip') as myzip:
        with myzip.open('stop_times.txt') as myfile:
            stop_times = pd.read_csv(myfile)
            columns = ['trip_id', 'stop_id']  #'route_id','route_short_name','route_long_name', 'route_type']
            stop_times_id_data = stop_times[columns].copy()
            #stop_times.to_csv('stop_times.csv') #denne lager CSV 
    return(stop_times_id_data)



data4 = get_data_stop_times()
data4.to_sql('stage_stop_times', con = engine, index=False, schema='public', if_exists='append')



#%%
#Create table in SQL trips 

def get_data_trips():
    url = 'https://storage.googleapis.com/marduk-production/outbound/gtfs/rb_rut-aggregated-gtfs.zip'
    myfile = requests.get(url)
    current_folder = os.getcwd()
    open(current_folder + '/routes.zip', 'wb').write(myfile.content)
    sto = pd.DataFrame()
    with ZipFile('routes.zip') as myzip:
        with myzip.open('trips.txt') as myfile:
            trips = pd.read_csv(myfile)
            columns = ['route_id', 'trip_id']  #'route_id','route_short_name','route_long_name', 'route_type']
            trips_data = trips[columns].copy()
            #stop_times.to_csv('stop_times.csv') #denne lager CSV 
    return(trips_data)

data5 = get_data_trips()
data5.to_sql('stage_trips', con = engine, index=False, schema='public', if_exists='append')




#%%
my_big_dataframe = data5
batchsize = 1000
for i in range(0, len(my_big_dataframe), batchsize):
    smaller_dataframe = my_big_dataframe[i:i+batchsize]
    smaller_dataframe.to_sql('stage_trips', con = engine, index=False, schema='public', if_exists='append')
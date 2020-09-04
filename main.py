#%%
#Kajas codes that creates tables from EnTur(Ruter)

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
# In this cell we extract info on all sources (weather stations) available in frost API & insert data into staging area for sources.
import requests
import psycopg2


#connect to postgress
conn = psycopg2.connect(
    host="ds-etl-academy.cgbivchwjzle.eu-west-1.rds.amazonaws.com, dbname",
    dbname = "team_csv", 
    user = "student_helene", 
    password="Gandalf!", 
    port=5432)


# extract from sources site on FROST
client_id = '5a763f3f-2f05-4a3f-8ed1-bf6880cdbbb9'
frost_root = 'https://frost.met.no/sources/v0.jsonld'
r = requests.get(frost_root, auth=(client_id,''))
frost_dictionary = r.json()

# Check if the request worked, print out any errors
if r.status_code == 200:
    data = frost_dictionary['data']
    print('Data retrieved from frost.met.no!')
else:
    print('Error! Returned status code %s' % r.status_code)
    print('Message: %s' % frost_dictionary['error']['message'])
    print('Reason: %s' % frost_dictionary['error']['reason'])
 
# Loops through sources, prints to staging area in Postgress   
cur = conn.cursor()
query = ("INSERT INTO stage_sources (id, name, frost_long, frost_lat) VALUES (%s, %s, %s, %s)")
symbols = set('[]')
length = len(frost_dictionary['data'])

for i in range(length):
    clean_id = ''
    for character in [(frost_dictionary['data'][i]['id'])]:
        if character in symbols:
            pass
        else:
            clean_id += character     
    clean_name = ''        
    if 'name' not in frost_dictionary['data'][i]:
        print(frost_dictionary['data'][i])
    else:
        for character in [(frost_dictionary['data'][i]['name'])]:
            if character in symbols: 
                pass
            else: 
                clean_name += character      
    clean_coord = ''    
    if 'geometry' not in frost_dictionary['data'][i]:
        print(frost_dictionary['data'][i])
    else: 
        coordinates = [(frost_dictionary['data'][i]['geometry']['coordinates'])]
        longc = coordinates[0][0]
        latc = coordinates[0][1]
    values = (clean_id, clean_name, longc, latc) 
    try:
        cur.execute(query, values)
        conn.commit()
    except Exception as e: 
                print(e)
    
 
   
cur.close()
conn.close()

#%%

# find the range of coordinates ruter has stations within. 
import psycopg2


conn = psycopg2.connect(
    host="ds-etl-academy.cgbivchwjzle.eu-west-1.rds.amazonaws.com, dbname",
    dbname = "team_csv", 
    user = "student_helene", 
    password="Gandalf!", 
    port=5432)

cur = conn.cursor()

cur.execute("select min(stop_lat) from stage_stops")
minlat = curr.fetchall()
conn.commit()

cur.execute("select max(stop_lat) from stage_stops")
maxlat = curr.fetchall()
conn.commit()

cur.execute("select min(stop_lon) from stage_stops")
minlong = curr.fetchall()
conn.commit()

cur.execute("select max(stop_lon) from stage_stops")
maxlong = curr.fetchall()
conn.commit()

cur.close()
conn.close()


#%%

# here we create a table in our database with all weather stations within coordinates of where router is operating:

import psycopg2
conn = psycopg2.connect(
    host="ds-etl-academy.cgbivchwjzle.eu-west-1.rds.amazonaws.com, dbname",
    dbname = "team_csv", 
    user = "student_helene", 
    password="Gandalf!", 
    port=5432)

cur = conn.cursor()

query = ("CREATE TABLE public.weather_station AS 
            SELECT id, 
            name, 
            frost_long, 
            frost_lat 
        from stage_sources 
        where (frost_lat between 59.42 and 60.55) and (frost_long between 10.10  and 11.87)")

cur.execute(query)
conn.commit()
    
cur.close()
conn.close()   


#%%
# Here we connect ruter stops to its closest weather station using coordinates, and place them in a table for future reference:
import psycopg2
conn = psycopg2.connect(
    host="ds-etl-academy.cgbivchwjzle.eu-west-1.rds.amazonaws.com, dbname",
    dbname = "team_csv", 
    user = "student_helene", 
    password="Gandalf!", 
    port=5432)

cur = conn.cursor()

cur.execute('select frost_long from weather_station') 
frost_long = (cur.fetchall())
conn.commit()

cur.execute('select frost_lat from weather_station')
frost_lat = (cur.fetchall())
conn.commit()

cur.execute('select id from weather_station')
frost_stationname  = (cur.fetchall())
conn.commit()

cur.execute('select stop_lon from stage_stops;')
entur_long= (cur.fetchall())
conn.commit()

cur.execute('select stop_lat from stage_stops;')
entur_lat= (cur.fetchall())
conn.commit()

cur.execute('select stop_name from stage_stops;')
entur_stopname = (cur.fetchall())
conn.commit()


query = ("INSERT INTO closest_weather_station (ruter_stop, weather_stat_id) VALUES (%s, %s)")


import numpy


for i in range(len(entur_long)):
    e_long = float('.'.join(str(ele) for ele in entur_long[i]))
    e_lat = float('.'.join(str(ele) for ele in entur_lat[i]))
    b = numpy.array((e_long, e_lat))
    distance_list = []
    for j in range(len(frost_long)):
        f_long = float('.'.join(str(ele) for ele in frost_long[j]))
        f_lat = float('.'.join(str(ele) for ele in frost_lat[j]))
        a = numpy.array((f_long ,f_lat))
        distance_list.append((numpy.linalg.norm(b-a)))
    print(min(distance_list))
    index = distance_list.index(min(distance_list))
    ws_id = frost_stationname[index]
    print(ws_id)
    values = entur_stopname[i], ws_id
    cur.execute(query, values)
    conn.commit()
    
cur.close()
conn.close()   




#%%
# Here we extract all observations from a given date (sdate variable) upto today, here set to 1.9.2020, from frost API 

import requests
import psycopg2


client_id = '5a763f3f-2f05-4a3f-8ed1-bf6880cdbbb9'
frost_root = 'https://frost.met.no/observations/v0.jsonld'


conn = psycopg2.connect(host = "ds-etl-academy.cgbivchwjzle.eu-west-1.rds.amazonaws.com",
dbname = "team_csv",
user = "student_helene",
password = "Gandalf!",
port = 5432)

cur = conn.cursor()  

sourceid_list= []
cur.execute("SELECT DISTINCT(weather_stat_id) from closest_weather_station;") 
s_id = (cur.fetchall())
s_id2 = [item for t in s_id for item in t]



#creating time function to give in format of iso 2020-09-02T08:00:00.000Z
from datetime import date, timedelta

# set wanted start date on the format (yyyy,m,d) f.eks: 2020,9,1
sdate = date(2020,9,1)   # start date
edate = date.today()

delta = edate - sdate       # as timedelta
date_list = []
for i in range(delta.days + 1):
    day = sdate + timedelta(days=i)
    date_list.append((str(day)))
        
time_list = ['00:00:00.000', '01:00:00.000', '02:00:00.000',
             '03:00:00.000', '04:00:00.000', '05:00:00.000',
             '06:00:00.000', '07:00:00.000', '08:00:00.000',
             '09:00:00.000', '10:00:00.000', '11:00:00.000',
             '12:00:00.000', '13:00:00.000', '14:00:00.000',
             '15:00:00.000', '16:00:00.000', '17:00:00.000',
             '18:00:00.000', '19:00:00.000', '20:00:00.000',
             '21:00:00.000', '22:00:00.000', '23:00:00.000',
             ]
time_ref_list = []
for day in date_list:
    for time in time_list:
        timereference = f'{day}T{time}Z'
        time_ref_list.append([timereference, day, time])




for i in range(len(s_id2)):
    source = s_id2[i]
    print(source)
    for j in range(len(time_ref_list)):
        reftime = time_ref_list[j][0]
        parameters = {
                'sources': source,
                'elements': 'air_temperature,sum(precipitation_amount PT1H)',
                'referencetime': reftime
                }
        elements = parameters['elements'].split(',')
        r = requests.get(frost_root, parameters, auth=(client_id,''))
        observation = r.json()
        date_ = time_ref_list[j][1]
        time_ = time_ref_list[j][2]
        
        if r.status_code == 200:
            print('yes')
            vals = {}
            for e in elements:
               val = None
               for o in observation['data'][0]['observations']:
                    t = o['elementId']
                    if t == e:
                        val = o['value']
                        break
               vals[e] = val
            query = ("INSERT INTO stage_observations2 (weather_stat_id, time, date, temperature, percipitation) VALUES (%s, %s, %s, %s, %s)")        
            values = source, time_ , date_, vals['air_temperature'], vals['sum(precipitation_amount PT1H)']
            try: 
                cur.execute(query, values)  
                conn.commit() 
               # print(values)
            except:
                print('Here it messed up:')
                print(values)
        else:
            print('Error! Returned status code %s' % r.status_code)
            
cur.close()
conn.close()




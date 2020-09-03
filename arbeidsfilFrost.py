import requests
import pandas as pd


client_id = '5a763f3f-2f05-4a3f-8ed1-bf6880cdbbb9'
frost_root = 'https://frost.met.no/observations/v0.jsonld'
parameters = {
    'sources': 'SN18700,SN90450',
    'elements': 'air_temperature,sum(precipitation_amount PT1H),mean(surface_air_pressure PT1H),mean(wind_speed PT1H)',
    'referencetime': '2020-01-01/2020-01-02',
}


#sjekker at det fungerer
if r.status_code == 200:
    data = frost_dictionary['data']
    print('Data retrieved from frost.met.no!')
else:
    print('Error! Returned status code %s' % r.status_code)
    print('Message: %s' % frost_dictionary['error']['message'])
    print('Reason: %s' % frost_dictionary['error']['reason'])
frost_data_dict = {}


# Issue an HTTP GET request
r = requests.get(frost_root, parameters, auth=(client_id,''))
# Extract JSON data
frost_dictionary = r.json()




#Henter data fra dBeaver. 
import psycopg2

conn = psycopg2.connect(host = "ds-etl-academy.cgbivchwjzle.eu-west-1.rds.amazonaws.com",
dbname = "team_csv",
user = "student_lene",
password = "osama",
port = 5432)

cur = conn.cursor()


#Get_sources, gi liste med alle source_id
#Itererer gjennom listen med kilder. 
#Kommer som tuple. Ha de til liste. 

def get_sources():
    sourceid_list= []
    cur.execute("SELECT DISTINCT(weather_stat_id) from closest_weather_station;") 
    s_id = (cur.fetchall())
    s_id2 = [item for t in s_id for item in t]
    return s_id2
 
cur.close()
conn.close()


time_list = ['2020-09-02T08:00:00.000Z', '2020-09-02T08:01:00.000Z', '2020-09-02T08:02:00.000Z',
             '2020-09-02T08:03:00.000Z', '2020-09-02T08:04:00.000Z', '2020-09-02T08:05:00.000Z',
             '2020-09-02T08:06:00.000Z', '2020-09-02T08:07:00.000Z', '2020-09-02T08:08:00.000Z',
             '2020-09-02T08:09:00.000Z', '2020-09-02T08:10:00.000Z', '2020-09-02T08:11:00.000Z',
             '2020-09-02T08:12:00.000Z', '2020-09-02T08:13:00.000Z', '2020-09-02T08:14:00.000Z',
             '2020-09-02T08:15:00.000Z', '2020-09-02T08:16:00.000Z', '2020-09-02T08:17:00.000Z',
             '2020-09-02T08:18:00.000Z', '2020-09-02T08:19:00.000Z', '2020-09-02T08:20:00.000Z',
             '2020-09-02T08:21:00.000Z', '2020-09-02T08:22:00.000Z', '2020-09-02T08:23:00.000Z',
             '2020-09-02T08:24:00.000Z']
    
def get_referencetime(date_from='2020-01-09', date_until='2020-09-02'):
    
    
    2020-09-03T07:30:00.000Z
    2020-09-02T08:00:00.000Z
           
                      
    
    
    
#%% Dersom vi rekker: 
def load_observations():
    sources = get_sources()
    elements = 'air_temperature, sum(precipitation_amount PT1H),mean(surface_air_pressure PT1H),mean(wind_speed PT1H)
    referencetime = get_referencetime() #sjekk hva kaia lager
    for s in sources:
        for day in referencetime:
            raw_data = requests.get(with url+ day) 
            print(raw_data)
            # refine data for insertion into table
    
#%%


client_id = '5a763f3f-2f05-4a3f-8ed1-bf6880cdbbb9'
frost_root = 'https://frost.met.no/observations/v0.jsonld'


import requests
import psycopg2

conn = psycopg2.connect(host = "ds-etl-academy.cgbivchwjzle.eu-west-1.rds.amazonaws.com",
dbname = "team_csv",
user = "student_lene",
password = "osama",
port = 5432)

cur = conn.cursor()  



def get_sources():
    sourceid_list= []
    cur.execute("SELECT DISTINCT(weather_stat_id) from closest_weather_station;") 
    s_id = (cur.fetchall())
    s_id2 = [item for t in s_id for item in t]
    return s_id2


time_list = ['2020-09-02T00:00:00.000Z', "2020-09-02T01:00:00.000Z", "2020-09-02T02:00:00.000Z",
             "2020-09-02T03:00:00.000Z", "2020-09-02T04:00:00.000Z", "2020-09-02T05:00:00.000Z",
             "2020-09-02T06:00:00.000Z", "2020-09-02T07:00:00.000Z", "2020-09-02T08:00:00.000Z",
             "2020-09-02T09:00:00.000Z", "2020-09-02T10:00:00.000Z", "2020-09-02T11:00:00.000Z",
             "2020-09-02T12:00:00.000Z", "2020-09-02T13:00:00.000Z", "2020-09-02T14:00:00.000Z",
             "2020-09-02T15:00:00.000Z", "2020-09-02T16:00:00.000Z", "2020-09-02T17:00:00.000Z",
             "2020-09-02T18:00:00.000Z", "2020-09-02T19:00:00.000Z", "2020-09-02T20:00:00.000Z",
             "2020-09-02T21:00:00.000Z", "2020-09-02T22:00:00.000Z", "2020-09-02T23:00:00.000Z",
             "2020-09-02T24:00:00.000Z"]
    


    
    for i in range(1):
        source = s_id2[0]
        for j in range(1):
            reftime = time_list[0]
            parameters = {
                'sources': source,
                'elements': 'air_temperature,sum(precipitation_amount PT1H)',
                'referencetime': reftime
                }
            elements = parameters['elements'].split(',')
            r = requests.get(frost_root, parameters, auth=(client_id,''))
            observation = r.json()
            
            vals = {}
            for e in elements:
                val = None
                for o in observation['data'][0]['observations']:
                    t = o['elementId']
                    if t == e:
                        val = o['value']
                        break
                vals[e] = val
            
                
    
            #air_temp =  observation['data'][0]['observations'][0]['value']   
            #percipitation =  observation['data'][0]['observations'][1]['value']


         
        
query = ("INSERT INTO stage_observations (weather_stat_id, timereference, temperature, percipitation) VALUES (%s, %s, %s, %s)")        
values = source, reftime, vals['air_temperature'], vals['sum(precipitation_amount PT1H)']
cur.execute(query, values)       
    



#To do: 
            
#get_sources, gi liste med alle source_id, 


#get referencetime, liste med dager. 

    
#Get parametersfunksjon- må gi alt jeg trenger for å kjøre en request for å trekke ut data.
#sourceid, dato som input  for å bruke i load_obervations.    
    
#parameters og r request bør være i funksjonen.         

#sitter igjen med json. 
    

    
    






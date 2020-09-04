# teamcsv

Forklaring på Frost-observasjoner:

Opprinnelig plan: 
Å hente ut observasjoner fra 01.01.2020 frem til nåværende tidspunkt. Til å definere godt vær hadde vi valgt ut følgende: 
-  temperatur (air_temperature)
-  nedbør (sum(precipitation_amount PT1H)
-  air pressure ( mean(surface_air_pressure PT1H)
-  mean (wind_speed PT1H)
Resultatet: 
- Vi måtte innskrenke ganske kraftig. Sluttresultatet inneholder observasjoner for hver time siden 1.september. I tillegg innskrenket antall elementer for å definere vært fint vær. Fint vær beregnes nå ut fra nedbør og temperatur. 

I loopen som kjører igjennom observasjoner har vi definert parametere. 
Source (s_id2) identifiserer målestasjon og hentes fra databasen vår i postgres. Elementer hentes fra json filen til observasjoner fra Frost og tid fra overnevnte time_list. 
Dersom en source kun har verdi på en av elementene, settes den manglende verdien til None. Sources som verken har temperatur eller nedbørsmåling utelukkes. 

En del observasjoner har ikke måling på temperatur. Planen vår var å erstatte de som manglet måling med gjennomsnittet av verdiene til de som hadde måling. Grunnet tidsmangel ble ikke dette gjort. 




Linje 1-45 
Oppsett til EnTur API & database connection via postgress. 


Linje 46- 75
Connecting til EnTurAPI & extract info om alle buss-stopp.
Plasser i staging area (table: public.stage_stops) i database. 
Extracts: stop_id, stom_name, coordinates long, coordinates lat.


Linje 76- 96
Connecting to EnTurAPI & extract info om ruter. 
(denne ble ikke kjørt i prosjektet da data var for stor)

Linje 97- 154
Connecting to EnTurAPI & extract info om shapes/ruter. 
(denne ble ikke kjørt i prosjektet da data var for stor)

Linje 155-174
Connecting to EnTurAPI & extract info om trips. 
(denne ble ikke kjørt i prosjektet da data var for stor)

Linje 175-244
Connecting to FrostAPI & extract info on all sources/weather stations. 
Plasserer i staging area(table:public.stage_source) i database.
Extracts: source_id, source_name,  coordinates long, coordinates lat.

Linje 246-280
Finds the range of coordinates where ruter has stations in. 
Note: this will be transferred to the next cell in the WHERE cluse.

Linje 281- 309
Create subset from stage_source of weather stationn inside ruters coordinates.


Linje 310- 376
Finds the weather station which is closest for each of the ruter-stops & inserts this information to the database. 

Linje 377- 478
 Here we extract all observations from a given date (sdate variable) upto today, here set to 1.9.2020, from frost API. With the given weather stations and. 




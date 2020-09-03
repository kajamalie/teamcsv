# teamcsv

Forklaring på Frost-observasjoner:

Opprinnelig plan: 
Å hente ut observasjoner fra 01.01.2020 frem til nåværende tidspunkt. Til å definere godt vær hadde vi valgt ut følgende: 
-  temperatur (air_temperature)
-  nedbør (sum(precipitation_amount PT1H)
-  air pressure ( mean(surface_air_pressure PT1H)
-  mean (wind_speed PT1H)
Resultatet: 
- Vi måtte innskrenke ganske kraftig. Sluttresultatet inneholder observasjoner for hver time for èn dag- 02.09.2020. Disse verdiene har vi laget manuelt i variabelen time_list. I tillegg innskrenket antall elementer for å definere vært fint vær. Fint vær beregnes nå ut fra nedbør og temperatur. 

I loopen som kjører igjennom observasjoner har vi definert parametere. 
Source (s_id2) identifiserer målestasjon og hentes fra databasen vår i postgres. Elementer hentes fra json filen til observasjoner fra Frost og tid fra overnevnte time_list. 
Dersom en source kun har verdi på en av elementene, settes den manglende verdien til None. Sources som verken har temperatur eller nedbørsmåling utelukkes. 

En del observasjoner har ikke måling på temperatur. Planen vår var å erstatte de som manglet måling med gjennomsnittet av verdiene til de som hadde måling. Grunnet tidsmangel ble ikke dette gjort. 
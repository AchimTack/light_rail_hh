# -*- coding: utf-8 -*-
"""
Created on Wed Oct 18 19:36:05 2017

@author: Achim
"""

# imports
import pandas as pd
from sqlalchemy import create_engine

# variables
passengers_raw = r'../../data/raw/Passagierzahlen.csv'
stations_raw = r'../../data/raw/station_coordinates.csv'
engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgis')

### process passenger data
# load raw data to df
df1 = pd.read_csv(stations_raw, sep='\t')
df1['lat'] = df1['lat'].str.replace(',','.')
df1['lon'] = df1['lon'].str.replace(',','.')
df1['lat'] = df1['lat'].apply(pd.to_numeric)
df1['lon'] = df1['lon'].apply(pd.to_numeric)
print df1.head(n=5)

#upload df to local postgres db
df1.to_sql('light_rail_stations', engine, schema='its_hackathon', if_exists='replace')


### process passenger data
# load raw data to df
df2 = pd.read_csv(passengers_raw, sep=';', encoding='ISO-8859-1')
df2['dtmIstAbfahrtDatum'] = df2['dtmIstAbfahrtDatum'].str.replace('.','-')
df2['Einsteiger'] = df2['Einsteiger'].str.replace(',','.')
df2['Aussteiger'] = df2['Aussteiger'].str.replace(',','.')s
df2 = df2[['dtmIstAbfahrtDatum', 'Zugnr', 'Station', 'Einsteiger', 'Aussteiger']].copy()

df2.columns = ['dep_time', 'train_id', 'station', 'boarders', 'deboarders']
df2['boarders'] = df2['boarders'].apply(pd.to_numeric)
df2['deboarders'] = df2['deboarders'].apply(pd.to_numeric)

# convert string-timestamp to datetime
df2['dep_time'] = pd.to_datetime(df2['dep_time'],dayfirst=True)
df2['dep_time'] = df2['dep_time'].values.astype('<M8[m]')
df2.set_index(pd.DatetimeIndex(df2['dep_time']))
print df2.head(n=5)

#upload df to local postgres db
df2.to_sql('light_rail_passengers', engine, schema='its_hackathon', if_exists='replace')


print 'all done'


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


### process passenger data
# load raw data to df
df = pd.read_csv(stations_raw, sep='\t')
df['lat'] = df['lat'].str.replace(',','.')
df['lon'] = df['lon'].str.replace(',','.')

df['lat'] = df['lat'].apply(pd.to_numeric)
df['lon'] = df['lon'].apply(pd.to_numeric)

print df.head(n=5)

#upload df to local postgres db
engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgis')
df.to_sql('light_rail_stations', engine, if_exists='replace')


### process passenger data
# load raw data to df
df = pd.read_csv(passengers_raw, sep=';', encoding='ISO-8859-1')
df['dtmIstAbfahrtDatum'] = df['dtmIstAbfahrtDatum'].str.replace('.','-')
df['Einsteiger'] = df['Einsteiger'].str.replace(',','.')
df['Aussteiger'] = df['Aussteiger'].str.replace(',','.')

df = df[['dtmIstAbfahrtDatum', 'Zugnr', 'Station', 'Einsteiger', 'Aussteiger']].copy()
df.columns = ['dep_time', 'train_id', 'station', 'boarders', 'deboarders']

df['boarders'] = df['boarders'].apply(pd.to_numeric)
df['deboarders'] = df['deboarders'].apply(pd.to_numeric)

# convert string-timestamp to datetime
df['dep_time'] = pd.to_datetime(df['dep_time'],dayfirst=True)
df['dep_time'] = df['dep_time'].values.astype('<M8[m]')
df.set_index(pd.DatetimeIndex(df['dep_time']))

print df.head(n=5)

#upload df to local postgres db
engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgis')
df.to_sql('light_rail_passengers', engine, if_exists='replace')

del df

print 'all done'


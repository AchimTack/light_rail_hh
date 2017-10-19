# -*- coding: utf-8 -*-
"""
Created on Wed Oct 18 19:36:05 2017

@author: Achim
"""

# imports
import pandas as pd
from sqlalchemy import create_engine

# variables

# load raw data to df
df = pd.read_csv(raw_data, sep=';', encoding='ISO-8859-1')
df['dtmIstAbfahrtDatum'] = df['dtmIstAbfahrtDatum'].str.replace('.','-')
df['Einsteiger'] = df['Einsteiger'].str.replace(',','.')
df['Aussteiger'] = df['Aussteiger'].str.replace(',','.')

df = df[['dtmIstAbfahrtDatum', 'Zugnr', 'Station', 'Einsteiger', 'Aussteiger']].copy()
df.columns = ['dep_time', 'train_id', 'station', 'boarders', 'deboarders']

# convert string-timestamp to datetime
df['dep_time'] = pd.to_datetime(df['dep_time'],dayfirst=True)
df['dep_time'] = df['dep_time'].values.astype('<M8[m]')
df.set_index(pd.DatetimeIndex(df['dep_time']))

# aggregate over time & station
df_agg = df.groupby(['dep_time','station']).sum()

print df.head(n=5)

print df_agg.head(n=5)

engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgis')
engine.execute("drop table if exists light_rail_hh")
df.to_sql('light_rail_hh', engine)

print 'all done'


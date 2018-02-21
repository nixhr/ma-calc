#!/usr/bin/python3

import ephem
from datetime import datetime
from pandas.io import sql
from sqlalchemy import create_engine
import mysql.connector
from mysql.connector import Error
from csv import reader
from pprint import pprint
from collections import defaultdict
import numpy as np
import pandas as pd
from pandas.io import sql
import time
import math
from math import sqrt
import json
import pdb

# ------------------ config start ---------------------#
MTM_TempFac=200         # Larger the number --> Positive t2m has LARGER influence on melting falling snow (default=200)
MTM_TriangleFac=700     # Larger the number --> Positive t2m AND positive zeroChgt have SMALLER influence on melting (default=500)
# ------------------- config end ----------------------#

csvdir="input_csv"

location="Zagreb"
lat='45.80'
lon='16.00'
height=100
altlat=45.80
altlon=16.00
altheight=100

dflist = []
df = {}

# csv sources list
# fields: 
# postproc table field name
# file suffix
# relevant csv column

sources = [["matrixstats","cldave","cld",0], \
          ["matrixstats","precave","prec",0], \
          ["matrixstats","precpct","prec",2], \
          ["matrixstats","upthrpct","up",2], \
          ["matrixstats","rdrmax","rdr",0], \
          ["matrixstats","capeave","capep1",0], \
          ["extract","altt2m","altt2m",0], \
          ["extract","capep1","capep1",0], \
          ["extract","cld","cld",0], \
          ["extract","d2m","d2m",0], \
          ["extract","gust","gust",0], \
          ["extract","h0","h0",0], \
          ["extract","h2m","h2m",0], \
          ["extract","mdlhgt","mdlhgt",0], \
          ["extract","mlcape","mlcape",0], \
          ["extract","mslp","mslp",0], \
          ["extract","prec","prec",0], \
          ["extract","t2m","t2m",0], \
          ["extract","t850","t850",0], \
          ["extract","wd","wd",0], \
          ["extract","wspd","wspd",0]] 
#sources = [["cldave","cld",0]]

for i in range(len(sources)):
  filename=str(csvdir) + "/" + sources[i][0] + "_" + str(location) + "_" + sources[i][2]
  varname=str(sources[i][1])
  field=int(sources[i][3])
  df[varname] = pd.read_csv(filename, header=None, usecols=[field], names=[varname], dtype=np.float64)
  dflist.append(df[varname])
  rf = pd.concat(dflist, axis=1)


#import dates
dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d_%H:%M')
df['date'] = pd.read_csv(csvdir + '/dates', header=None, parse_dates=[0], names=['date'],date_parser=dateparse)
dflist.append(df['date'])
rf = pd.concat(dflist, axis=1)

#import weekdays
df['weekday'] = pd.read_csv(csvdir + '/weekdays', header=None, usecols=[0], names=['weekday'])
dflist.append(df['weekday'])
rf = pd.concat(dflist, axis=1)

# temporary
rf['location']="Zagreb"
rf['lat']='45.80'
rf['lon']='16.00'
rf['height']=100
rf['altlat']=45.80
rf['altlon']=16.00
rf['altheight']=100

#add cloumn weather
rf['weather'] = np.nan
rf['precpctfinal'] = np.nan
rf['snowpct'] = np.nan
rf['rtspct_ratio'] = np.nan
rf['rainpct'] = np.nan
rf['tstormpct'] = np.nan
rf['precpctdisp'] = np.nan
rf['snowpctdisp'] = np.nan
rf['tstormpctdisp'] = np.nan
rf['h2mdisp'] = np.nan
rf['tstorm'] = str("-")
rf['fog'] = str("-")
#rf['nightsym']=np.nan
rf['wind'] = np.nan
rf['winddir'] = np.nan
rf['wdir'] = np.nan
rf['rtsratiotmp'] = np.nan
rf['rtsratio'] = np.nan
rf['hour'] = np.nan
rf['ymd'] = np.nan
rf['daynight'] = np.nan

rf['winterdone'] = np.nan
rf['fogdone'] = np.nan
rf['winddone'] = np.nan
rf['rtsratiodone'] = np.nan


start_time = time.time()

# B) Calculate final precipitation probability
rf.loc[(rf['precpctfinal'].isnull()), 'precpctfinal'] = np.clip((rf['precpct'] + (np.clip((rf['rdrmax'] - 20),0,None)/2) + np.clip((rf['cldave'] - 60),0,None)/4),0,100).apply(lambda x: round(x,0))

# C) Calculate snow probability
rf.loc[(rf['snowpct'].isnull()), 'snowpct'] = np.clip(rf['precpctfinal']*(1 - (np.clip(((np.clip(rf['h0'],0,None).apply(lambda x: round(x,0)) + MTM_TempFac * rf['t2m'])/2),0,None).apply(lambda x: round(x,3))) / MTM_TriangleFac),0,100).apply(lambda x: round(x,0))
rf.loc[(rf['rainpct'].isnull()), 'rainpct'] = np.clip((rf['precpctfinal']-rf['snowpct']),0,100).apply(lambda x: round(x,0))

# D) Calculate tstorm probability

rf.loc[(rf['tstormpct'].isnull()) & (rf['rdrmax'] >= 35), 'tstormpct'] = (((rf['upthrpct'])**(0.5))*14+rf['rdrmax']-40+(rf['capeave'])**(0.5)*2-15).apply(lambda x: round(x,0))
rf.loc[(rf['tstormpct'].isnull()) & (rf['rdrmax'] < 35), 'tstormpct'] = ((rf['precpctfinal']/100)*((rf['capeave'])**(0.5))*2-15).apply(lambda x: round(x,0))

# E) Limit precipitation, snow and tstorm probabilities into range 1-90 %

rf.loc[(rf['precpctfinal'] < 1), 'precpctdisp'] = '<1%'
rf.loc[(rf['precpctfinal'] > 90), 'precpctdisp'] = '>90%'
rf.loc[(rf['precpctdisp'].isnull()), 'precpctdisp'] = rf['precpctfinal'].astype(str) + '%'

rf.loc[(rf['snowpct'] < 1), 'snowpctdisp'] = '<1%'
rf.loc[(rf['snowpct'] > 90), 'snowpctdisp'] = '>90%'
rf.loc[(rf['snowpctdisp'].isnull()), 'snowpctdisp'] = rf['snowpct'].astype(str) + '%'

rf.loc[(rf['tstormpct'] < 1), 'tstormpctdisp'] = '<1%'
rf.loc[(rf['tstormpct'] > 90), 'tstormpctdisp'] = '>90%'
rf.loc[(rf['tstormpctdisp'].isnull()), 'tstormpctdisp'] = rf['tstormpct'].astype(str) + '%'

# F) Clouds and rain
rf.loc[(rf['precave'] > 4) & (rf['precpct'] > 20) & (rf['cldave'] < 50) & (rf['weather'].isnull()), 'weather'] = '7.png'
rf.loc[(rf['precave'] > 4) & (rf['precpct'] > 20) & (rf['cldave'] < 85) & (rf['weather'].isnull()), 'weather'] = '16.png'
rf.loc[(rf['precave'] > 1) & (rf['precpct'] > 20) & (rf['cldave'] < 50) & (rf['weather'].isnull()), 'weather'] = '6.png'
rf.loc[(rf['precave'] > 1) & (rf['precpct'] > 20) & (rf['cldave'] < 85) & (rf['weather'].isnull()), 'weather'] = '15.png'
rf.loc[(rf['precave'] > 0) & (rf['precpct'] > 20) & (rf['cldave'] < 50) & (rf['weather'].isnull()), 'weather'] = '5.png'
rf.loc[(rf['precave'] > 0) & (rf['precpct'] > 20) & (rf['cldave'] < 85) & (rf['weather'].isnull()), 'weather'] = '14.png'
rf.loc[(rf['precave'] > 4) & (rf['precpct'] > 20) & (rf['weather'].isnull()) , 'weather'] = '25.png'
rf.loc[(rf['precave'] > 1) & (rf['precpct'] > 20) & (rf['weather'].isnull()) , 'weather'] = '24.png'
rf.loc[(rf['precave'] > 0) & (rf['precpct'] > 20) & (rf['weather'].isnull()) , 'weather'] = '23.png'
rf.loc[(rf['cldave'] > 85) & (rf['weather'].isnull()), 'weather'] = '102.png'
rf.loc[(rf['cldave'] > 50) & (rf['weather'].isnull()), 'weather'] = '4.png'
rf.loc[(rf['cldave'] > 15) & (rf['weather'].isnull()), 'weather'] = '3.png'
rf.loc[(rf['cldave'] > 0) & (rf['weather'].isnull()), 'weather'] = '2.png'
rf.loc[rf['weather'].isnull(), 'weather'] = "1.png"

# G) Additional T-Storm flag
rf.loc[(rf['rdrmax'] > 55) & (rf['upthrpct'] > 20) & (rf['tstormpct'] > 60), 'tstorm'] = '202.png'        # grmljavinsko nevrijeme
rf.loc[(rf['rdrmax'] > 38) & (rf['upthrpct'] > 5) & (rf['tstormpct'] > 30) & (rf['tstorm'] != '202.png'), 'tstorm'] = '201.png'         # grmljavina

# H) Winter weather

rf.loc[(rf['winterdone'].isnull()) & (rf['precave'] > 2.5 )  &  (rf['precpctfinal']  > 20.0)  &  ((rf['rainpct']+0.001)/(rf['snowpct']+0.001) < 1.0 )  &  (rf['cldave']  < 50.0 )  &  (rf['t2m'] < 5.0 )  &  (rf['d2m'] < 2.0), ['winterdone', 'weather']] = ['1', '10.png']     # promjenjivo oblačno, jak snijeg
rf.loc[(rf['winterdone'].isnull()) & (rf['precave'] > 0.5 )  &  (rf['precpctfinal']  > 20.0)  &  ((rf['rainpct']+0.001)/(rf['snowpct']+0.001) < 1.0 )  &  (rf['cldave']  < 50.0 )  &  (rf['t2m'] < 5.0 )  &  (rf['d2m'] < 2.0), ['winterdone', 'weather']] = ['1', '9.png' ]     # promjenjivo oblačno, umjeren snijeg
rf.loc[(rf['winterdone'].isnull()) & (rf['precave'] > 0.0 )  &  (rf['precpctfinal']  > 20.0)  &  ((rf['rainpct']+0.001)/(rf['snowpct']+0.001) < 1.0 )  &  (rf['cldave']  < 50.0 )  &  (rf['t2m'] < 5.0 )  &  (rf['d2m'] < 2.0), ['winterdone', 'weather']] = ['1', '8.png' ]     # promjenjivo oblačno, slab snijeg
rf.loc[(rf['winterdone'].isnull()) & (rf['precave'] > 2.5 )  &  (rf['precpctfinal']  > 20.0)  &  ((rf['rainpct']+0.001)/(rf['snowpct']+0.001) < 1.0 )  &  (rf['cldave']  < 85.0 )  &  (rf['t2m'] < 5.0 )  &  (rf['d2m'] < 2.0), ['winterdone', 'weather']] = ['1', '19.png']     # pretežno oblačno, jak snijeg
rf.loc[(rf['winterdone'].isnull()) & (rf['precave'] > 0.5 )  &  (rf['precpctfinal']  > 20.0)  &  ((rf['rainpct']+0.001)/(rf['snowpct']+0.001) < 1.0 )  &  (rf['cldave']  < 85.0 )  &  (rf['t2m'] < 5.0 )  &  (rf['d2m'] < 2.0), ['winterdone', 'weather']] = ['1', '18.png']     # pretežno oblačno, umjeren snijeg
rf.loc[(rf['winterdone'].isnull()) & (rf['precave'] > 0.0 )  &  (rf['precpctfinal']  > 20.0)  &  ((rf['rainpct']+0.001)/(rf['snowpct']+0.001) < 1.0 )  &  (rf['cldave']  < 85.0 )  &  (rf['t2m'] < 5.0 )  &  (rf['d2m'] < 2.0), ['winterdone', 'weather']] = ['1', '17.png']     # pretežno oblačno, slab snijeg
rf.loc[(rf['winterdone'].isnull()) & (rf['precave'] > 2.5 )  &  (rf['precpctfinal']  > 20.0)  &  ((rf['rainpct']+0.001)/(rf['snowpct']+0.001) < 1.0 )  &  (rf['t2m']     <  5.0 )  &  (rf['d2m'] < 2.0),                        ['winterdone', 'weather']] = ['1', '28.png']     # jak snijeg 
rf.loc[(rf['winterdone'].isnull()) & (rf['precave'] > 0.5 )  &  (rf['precpctfinal']  > 20.0)  &  ((rf['rainpct']+0.001)/(rf['snowpct']+0.001) < 1.0 )  &  (rf['t2m']     <  5.0 )  &  (rf['d2m'] < 2.0),                        ['winterdone', 'weather']] = ['1', '27.png']     # umjeren snijeg 
rf.loc[(rf['winterdone'].isnull()) & (rf['precave'] > 0.0 )  &  (rf['precpctfinal']  > 20.0)  &  ((rf['rainpct']+0.001)/(rf['snowpct']+0.001) < 1.0 )  &  (rf['t2m']     <  5.0 )  &  (rf['d2m'] < 2.0),                        ['winterdone', 'weather']] = ['1', '26.png']     # slab snijeg 
rf.loc[(rf['winterdone'].isnull()) & (rf['precave'] > 4.0 )  &  (rf['precpctfinal']  > 20.0)  &  ((rf['rainpct']+0.001)/(rf['snowpct']+0.001) < 5.0 )  &  (rf['cldave']  < 50.0 )  &  (rf['t2m'] < 6.0 )  &  (rf['d2m'] < 3.0), ['winterdone', 'weather']] = ['1', '13.png']     # promjenjivo oblačno, jaka susnježica
rf.loc[(rf['winterdone'].isnull()) & (rf['precave'] > 1.0 )  &  (rf['precpctfinal']  > 20.0)  &  ((rf['rainpct']+0.001)/(rf['snowpct']+0.001) < 5.0 )  &  (rf['cldave']  < 50.0 )  &  (rf['t2m'] < 6.0 )  &  (rf['d2m'] < 3.0), ['winterdone', 'weather']] = ['1', '12.png']     # promjenjivo oblačno, umjerena susnježica
rf.loc[(rf['winterdone'].isnull()) & (rf['precave'] > 0.0 )  &  (rf['precpctfinal']  > 20.0)  &  ((rf['rainpct']+0.001)/(rf['snowpct']+0.001) < 5.0 )  &  (rf['cldave']  < 50.0 )  &  (rf['t2m'] < 6.0 )  &  (rf['d2m'] < 3.0), ['winterdone', 'weather']] = ['1', '11.png']     # promjenjivo oblačno, slaba susnježica
rf.loc[(rf['winterdone'].isnull()) & (rf['precave'] > 4.0 )  &  (rf['precpctfinal']  > 20.0)  &  ((rf['rainpct']+0.001)/(rf['snowpct']+0.001) < 5.0 )  &  (rf['cldave']  < 85.0 )  &  (rf['t2m'] < 6.0 )  &  (rf['d2m'] < 3.0), ['winterdone', 'weather']] = ['1', '22.png']     # pretežno oblačno, jaka susnježica
rf.loc[(rf['winterdone'].isnull()) & (rf['precave'] > 1.0 )  &  (rf['precpctfinal']  > 20.0)  &  ((rf['rainpct']+0.001)/(rf['snowpct']+0.001) < 5.0 )  &  (rf['cldave']  < 85.0 )  &  (rf['t2m'] < 6.0 )  &  (rf['d2m'] < 3.0), ['winterdone', 'weather']] = ['1', '21.png']     # pretežno oblačno, umjerena susnježica
rf.loc[(rf['winterdone'].isnull()) & (rf['precave'] > 0.0 )  &  (rf['precpctfinal']  > 20.0)  &  ((rf['rainpct']+0.001)/(rf['snowpct']+0.001) < 5.0 )  &  (rf['cldave']  < 85.0 )  &  (rf['t2m'] < 6.0 )  &  (rf['d2m'] < 3.0), ['winterdone', 'weather']] = ['1', '20.png']     # pretežno oblačno, slaba susnježica
rf.loc[(rf['winterdone'].isnull()) & (rf['precave'] > 4.0 )  &  (rf['precpctfinal']  > 20.0)  &  ((rf['rainpct']+0.001)/(rf['snowpct']+0.001) < 5.0 )  &  (rf['t2m']     <  6.0 )  &  (rf['d2m'] < 3.0),                        ['winterdone', 'weather']] = ['1', '31.png']     # jaka susnježica 
rf.loc[(rf['winterdone'].isnull()) & (rf['precave'] > 1.0 )  &  (rf['precpctfinal']  > 20.0)  &  ((rf['rainpct']+0.001)/(rf['snowpct']+0.001) < 5.0 )  &  (rf['t2m']     <  6.0 )  &  (rf['d2m'] < 3.0),                        ['winterdone', 'weather']] = ['1', '30.png']     # umjerena susnježica 
rf.loc[(rf['winterdone'].isnull()) & (rf['precave'] > 0.0 )  &  (rf['precpctfinal']  > 20.0)  &  ((rf['rainpct']+0.001)/(rf['snowpct']+0.001) < 5.0 )  &  (rf['t2m']     <  6.0 )  &  (rf['d2m'] < 3.0),                        ['winterdone', 'weather']] = ['1', '29.png']     # slaba susnježica

rf.rtspct_ratio=(rf['rainpct']+0.001)/(rf['snowpct']+0.001)

# I) Additional fog flag
# fog="-" po defaultu vec popunjen
rf.loc[(rf['fogdone'].isnull()) & (rf['precave']  < 0.2) & (rf['h2m'] > 99.0) & (rf['mslp'] > 1010) & (rf['wspd'] < 2.5) & (rf['cldave'] < 101.0) & (rf['tstorm'] != '-'), ['fogdone', 'weather', 'fog']] = ['1', '102.png', '302.png']     # jaka magla, updateamo fog i weather samo ako nije tstorm 
rf.loc[(rf['fogdone'].isnull()) & (rf['precave']  < 0.6) & (rf['h2m'] > 95.0) & (rf['mslp'] > 1005) & (rf['wspd'] < 4.0) & (rf['cldave'] < 101.0) & (rf['tstorm'] != '-'), ['fogdone', 'fog']] =  ['1', '301.png']       # slaba magla , updateamo fog samo ako nije tstorm

# J) Night symbols (this should be programmed to take in account ACTUAL sun position, but...)

ephemloc        = ephem.Observer()
ephemloc.lat    = lat
ephemloc.lon    = lon
ephemloc.elevation = height

def daytime(datetime):
  ephemloc.date=datetime
  next_sunrise    = ephemloc.next_rising(ephem.Sun()).datetime()
  next_sunset     = ephemloc.next_setting(ephem.Sun()).datetime()
  if next_sunset < next_sunrise:
    return 'day'
  else:
    return 'night'

rf.daynight=rf.date.apply(lambda x: daytime(x))

def modweather(image):
#  if daynight == 'day':
#    return image
#  else:
  if image == '1.png' :  return  '32.png' 
  if image == '2.png' :  return  '33.png' 
  if image == '3.png' :  return  '34.png' 
  if image == '4.png' :  return  '35.png' 
  if image == '5.png' :  return  '37.png' 
  if image == '6.png' :  return  '38.png' 
  if image == '7.png' :  return  '39.png' 
  if image == '8.png' :  return  '40.png' 
  if image == '9.png' :  return  '41.png' 
  if image == '10.png':  return  '42.png' 
  if image == '11.png':  return  '43.png' 
  if image == '12.png':  return  '44.png' 
  if image == '13.png':  return  '45.png' 
  if image == '14.png':  return  '46.png' 
  if image == '15.png':  return  '47.png' 
  if image == '16.png':  return  '48.png' 
  if image == '17.png':  return  '49.png' 
  if image == '18.png':  return  '50.png' 
  if image == '19.png':  return  '51.png' 
  if image == '20.png':  return  '52.png' 
  if image == '21.png':  return  '53.png' 
  if image == '22.png':  return  '54.png' 
  return image


rf.loc[rf['daynight'] == 'night', 'weather'] = rf['weather'].apply(lambda x: modweather(x))
# K) Static WIND corrections
# komentirano

# L) Wind Codes

# olujan vjetar
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 337.5 ) & (rf['wspd'] > 15 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'N' ,'100.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 292.5 ) & (rf['wspd'] > 15 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'NW','99.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 247.5 ) & (rf['wspd'] > 15 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'W' ,'98.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 202.5 ) & (rf['wspd'] > 15 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'SW','97.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 157.5 ) & (rf['wspd'] > 15 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'S' ,'96.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 112.5 ) & (rf['wspd'] > 15 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'SE','95.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 67.5  ) & (rf['wspd'] > 15 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'E' ,'94.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 22.5  ) & (rf['wspd'] > 15 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'NE','101.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  < 22.5  ) & (rf['wspd'] > 15 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'N' ,'100.png']

# jak vjetar

rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 337.5 ) & (rf['wspd'] > 10 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'N' ,'92.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 292.5 ) & (rf['wspd'] > 10 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'NW','91.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 247.5 ) & (rf['wspd'] > 10 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'W' ,'90.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 202.5 ) & (rf['wspd'] > 10 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'SW','89.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 157.5 ) & (rf['wspd'] > 10 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'S' ,'88.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 112.5 ) & (rf['wspd'] > 10 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'SE','87.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 67.5  ) & (rf['wspd'] > 10 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'E' ,'86.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 22.5  ) & (rf['wspd'] > 10 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'NE','93.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  < 22.5  ) & (rf['wspd'] > 10 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'N' ,'92.png']

# umjeren vjetar

rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 337.5 ) & (rf['wspd'] > 4 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'N' ,'84.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 292.5 ) & (rf['wspd'] > 4 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'NW','83.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 247.5 ) & (rf['wspd'] > 4 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'W' ,'82.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 202.5 ) & (rf['wspd'] > 4 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'SW','81.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 157.5 ) & (rf['wspd'] > 4 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'S' ,'80.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 112.5 ) & (rf['wspd'] > 4 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'SE','79.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 67.5  ) & (rf['wspd'] > 4 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'E' ,'78.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 22.5  ) & (rf['wspd'] > 4 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'NE','85.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  < 22.5  ) & (rf['wspd'] > 4 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'N' ,'84.png']

# slab vjetar

rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 337.5 ) & (rf['wspd'] >= 1 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'N' ,'77.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 292.5 ) & (rf['wspd'] >= 1 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'NW','71.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 247.5 ) & (rf['wspd'] >= 1 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'W' ,'70.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 202.5 ) & (rf['wspd'] >= 1 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'SW','69.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 157.5 ) & (rf['wspd'] >= 1 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'S' ,'68.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 112.5 ) & (rf['wspd'] >= 1 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'SE','67.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 67.5  ) & (rf['wspd'] >= 1 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'E' ,'66.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 22.5  ) & (rf['wspd'] >= 1 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'NE','65.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  < 22.5  ) & (rf['wspd'] >= 1 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'N' ,'77.png']

# tišina ili slab vjetar promjenjiva smjera

rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 337.5 ) & (rf['wspd'] < 1 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'N' ,'64.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 292.5 ) & (rf['wspd'] < 1 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'NW','64.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 247.5 ) & (rf['wspd'] < 1 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'W' ,'64.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 202.5 ) & (rf['wspd'] < 1 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'SW','64.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 157.5 ) & (rf['wspd'] < 1 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'S' ,'64.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 112.5 ) & (rf['wspd'] < 1 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'SE','64.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 67.5  ) & (rf['wspd'] < 1 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'E' ,'64.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  > 22.5  ) & (rf['wspd'] < 1 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'NE','64.png']
rf.loc[(rf['winddone'].isnull()) & (rf['wd']  < 22.5  ) & (rf['wspd'] < 1 ) , ['winddone', 'winddir', 'wind']] =  ['1', 'N' ,'64.png']

# Reset precipitation amount to 0.0 if weather symbol does not contain precipitation
rf.loc[(rf['weather'].isin([ '1.png', '2.png', '3.png', '4.png', '102.png', '32.png', '33.png', '34.png', '35.png' ])), 'precave'] = '0.0'

rf.wspd = rf.wspd.apply(lambda x: round(x,0))
rf.wdir = rf.wd.apply(lambda x: round(x,0))

rf.loc[rf['wdir'] == 360, 'wdir'] = 0

rf.loc[rf['weekday'] == "Monday"   , 'weekday' ] = "Ponedjeljak"
rf.loc[rf['weekday'] == "Tuesday"  , 'weekday' ] = "Utorak"
rf.loc[rf['weekday'] == "Wednesday", 'weekday' ] = "Srijeda"
rf.loc[rf['weekday'] == "Thursday" , 'weekday' ] = "Četvrtak"
rf.loc[rf['weekday'] == "Friday"   , 'weekday' ] = "Petak"
rf.loc[rf['weekday'] == "Saturday" , 'weekday' ] = "Subota"
rf.loc[rf['weekday'] == "Sunday"   , 'weekday' ] = "Nedjelja"

rf.loc[(rf['rtsratio'].isnull()) & (rf['rtspct_ratio'] >= 999), 'rtsratio' ] = '>999' #rf['rtspct_ratio'].apply(lambda x: round(x,2))
rf.loc[(rf['rtsratio'].isnull()) & (rf['rtspct_ratio'] >= 100), 'rtsratio' ] = rf['rtspct_ratio'].apply(lambda x: '{:.2f}'.format(round(x,0)))
rf.loc[(rf['rtsratio'].isnull()) & (rf['rtspct_ratio'] >= 10), 'rtsratio' ] = rf['rtspct_ratio'].apply(lambda x: '{:.2f}'.format(round(x,1)))
rf.loc[(rf['rtsratio'].isnull()) & (rf['rtspct_ratio'] < 10 ), 'rtsratio' ] = rf['rtspct_ratio'].apply(lambda x: '{:.2f}'.format(round(x,2)))


# ovo bas nema smisla jer nikad nije -
#                if [ "$precpctdisp" == "-" ]
#                then
#                    rtsratio="-"
#                fi

rf.h2m = rf.h2m.apply(lambda x: round(x,1))
rf.h2mdisp = rf.h2m.apply(lambda x: round(x,0))
rf.gust = rf.gust.apply(lambda x: round(x,0))

rf.loc[rf['gust'] < rf['wspd'], 'gust'] = rf['wspd']

rf.mslp = rf.mslp.apply(lambda x: round(x,0))
rf.mlcape = rf.mlcape.apply(lambda x: round(x,0))
rf.h0 = np.clip((rf.h0.apply(lambda x: round(x,0))),0, None)
rf.t850 = rf.t850.apply(lambda x: round(x,0))
rf.hour = rf.date.apply(lambda x: x.strftime('%H:%M'))
rf.ymd = rf.date.apply(lambda x: x.strftime('%Y-%m-%d'))

#j = (rf.groupby(['ymd','weekday'], as_index=False).apply(lambda x: x[['hour','weather']].to_dict('r')).reset_index().rename(columns={0:'forecast'}).to_json(orient='records'))

a=rf[['location','ymd','weekday','hour','weather','tstorm','fog','wind','wspd','gust','wdir','altt2m','d2m','h2mdisp','precpct','prec','snowpct','tstormpct','mslp','h0','t850','mlcape']]

ff=a.rename(index=str, columns={'altt2m': 'temperature',\
                             'ymd': 'date',\
                             'd2m': 'dewpoint',\
                             'h2mdisp':'humidity',\
                             'precave':'prec',\
                             'snowpctdisp':'snowpct',\
                             'tstormpctdisp': 'tstormpct',\
                             'h0':'h0m'})

'''
                                        "hour": "${currenthour}",
                                        "weather": "${weather}",
                                        "tstorm": "${tstorm}",
                                        "fog": "${fog}",
                                        "wind": "${wind}",
                                        "wspd": "${wspd}",
                                        "gust": "${gust}",
                                        "wdir": "${wdir}",
                                        "temperature": "${altt2m}",
                                        "dewpoint": "${d2m}",
                                        "humidity": "${h2mdisp}",
                                        "precpct": "${precpctdisp}",
                                        "prec": "${precave}",
                                        "snowpct": "${snowpctdisp}",
                                        "tstormpct": "${tstormpctdisp}",
                                        "mslp": "${mslp}",
                                        "h0m": "${h0}",
                                        "t850": "${t850}",
                                        "mlcape": "${mlcape}"
'''


#a=rf[['ymd','weekday','hour','weather']]
#j=a.groupby('location')[['ymd', 'weekday','hour','weather']].apply(lambda x: x[['ymd', 'weekday','hour','weather']].to_dict('r')).reset_index().rename(columns={0:'forecast', 'ymd':'date'}).to_json(orient='records')

#a=rf[['location','ymd','weekday','hour','weather']]
#a.set_index(['location', 'ymd'], inplace=True)
#j=a.groupby(level=[0,1]).apply(lambda x: x[['hour','weather']].to_dict('r')).to_json(orient='records')

# RADIj=a.groupby(['ymd', 'weekday'],as_index=False).apply(lambda x: x[['hour','weather']].to_dict('r')).reset_index().rename(columns={0:'forecast'}).to_json(orient='records')
j=ff.groupby(['date', 'weekday'],as_index=False).apply(lambda x: x[['hour','weather','tstorm','fog','wind','wspd','gust','wdir','temperature','dewpoint','humidity','precpct','prec','snowpct','tstormpct','mslp','h0m','t850','mlcape']].to_dict('r')).reset_index().rename(columns={0:'forecast'})

#j=a.groupby('location', as_index=False).apply(lambda x: x).reset_index().groupby(['ymd', 'weekday'],as_index=False).apply(lambda x: x[['hour','weather']].to_dict('r')).reset_index().rename(columns={0:'forecast'}).to_json(orient='records')

#j=a.groupby('ymd', as_index=True)[['hour','weather']].apply(lambda x: x[['hour','weather']].to_dict(orient='index')).to_json(orient='index')

#print (daygroups)

locations = {}
for locgrp, locdf in a.groupby('location'):
#  print('Group: %s' % locgrp)
#  print('DataFrame description: \n%s\n' % locdf)
  my_dict = {'location': locgrp , 'data': j.to_dict('r')}
#  for dategrp, datedf in locdf.groupby(['ymd','weekday']):
    #my_dict["data"].append({'date' : dategrp[0], 'weekday' : dategrp[1], 'forecast' : []})
#  my_dict["data"].append({json.loads(j).astype(str)})

#my_dict["data"].append(j.to_dict('r'))


print(json.dumps(my_dict, indent=2, sort_keys=False))

elapsed_time = time.time() - start_time

#print(elapsed_time)
#print(rf.to_string())
#print(rf.dtypes)

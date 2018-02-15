#!/usr/bin/python3

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



conn = mysql.connector.connect(host='localhost',
                               database='meteo',
                               user='meteo',
                               password='meteo')
#if conn.is_connected():
#  print('Connected to MySQL database')


conn.autocommit = True
cursor = conn.cursor()

csvdir="input_csv"
fileprefix="matrixstats"
location="Zagreb"
master = pd.DataFrame()
dflist = []
df = {}

# csv sources list
# fields: 
# postproc table field name
# file suffix
# relevant csv column

sources = [["cldave","cld",0],["precave","prec",0],["precpct","prec",1],["upthrpct","up",2],["rdrmax","rdr",0],["capeave","capep1",0]]
#sources = [["cldave","cld",0]]

for i in range(len(sources)):
  filename=str(csvdir) + "/" + str(fileprefix) + "_" + str(location) + "_" + sources[i][1]
  varname=str(sources[i][0])
  field=int(sources[i][2])
  df[varname] = pd.read_csv(filename, header=None, usecols=[field], names=[varname])
  dflist.append(df[varname])
  rf = pd.concat(dflist, axis=1)


#add cloumn weather
rf['weather'] = "None"
#print(str(rf.query('cldave > 85')))

start_time = time.time()
# your code

rf.loc[(rf['precave'] > 4) & (rf['precpct'] > 20) & (rf['cldave'] < 50) & (rf['weather'] == 'None'), 'weather'] = '7.png'
rf.loc[(rf['precave'] > 4) & (rf['precpct'] > 20) & (rf['cldave'] < 85) & (rf['weather'] == 'None'), 'weather'] = '16.png'
rf.loc[(rf['precave'] > 1) & (rf['precpct'] > 20) & (rf['cldave'] < 50) & (rf['weather'] == 'None'), 'weather'] = '6.png'

rf.loc[(rf['precave'] > 1) & (rf['precpct'] > 20) & (rf['cldave'] < 85) & (rf['weather'] == 'None'), 'weather'] = '15.png'
rf.loc[(rf['precave'] > 0) & (rf['precpct'] > 20) & (rf['cldave'] < 50) & (rf['weather'] == 'None'), 'weather'] = '5.png'
rf.loc[(rf['precave'] > 0) & (rf['precpct'] > 20) & (rf['cldave'] < 85) & (rf['weather'] == 'None'), 'weather'] = '14.png'

rf.loc[(rf['precave'] > 4) & (rf['precpct'] > 20) & (rf['weather'] == 'None'), 'weather'] = '25.png'
rf.loc[(rf['precave'] > 1) & (rf['precpct'] > 20) & (rf['weather'] == 'None'), 'weather'] = '24.png'
rf.loc[(rf['precave'] > 0) & (rf['precpct'] > 20) & (rf['weather'] == 'None'), 'weather'] = '23.png'

rf.loc[(rf['cldave'] > 85) & (rf['weather'] == 'None'), 'weather'] = '102.png'
rf.loc[(rf['cldave'] > 50) & (rf['weather'] == 'None'), 'weather'] = '4.png'
rf.loc[(rf['cldave'] > 15) & (rf['weather'] == 'None'), 'weather'] = '3.png'
rf.loc[(rf['cldave'] > 0) & (rf['weather'] == 'None'), 'weather'] = '2.png'

rf.loc[rf['weather'] == 'None', 'weather'] = "1.png"
#print (str(rf.loc[rf['weather'] == "None", "weather"]))

#result.add(other, axis='columns', level=None, fill_value=None)[source]

#result.to_sql(con=conn, name='postproc', if_exists='append', flavor='mysql')
elapsed_time = time.time() - start_time

print(elapsed_time)
#print(rf.to_string())


  

cursor.close()

conn.close()
 

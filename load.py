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
import math


'''
conn = mysql.connector.connect(host='localhost',
                               database='meteo',
                               user='meteo',
                               password='meteo')
#if conn.is_connected():
#  print('Connected to MySQL database')


conn.autocommit = True
cursor = conn.cursor()
'''

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

sources = [["matrixstats","cldave","cld",0], \
          ["matrixstats","precave","prec",0], \
          ["matrixstats","precpct","prec",1], \
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
          ["extract","wspd","wspd",0]] \
#sources = [["cldave","cld",0]]

for i in range(len(sources)):
  filename=str(csvdir) + "/" + sources[i][0] + "_" + str(location) + "_" + sources[i][2]
  varname=str(sources[i][1])
  field=int(sources[i][3])
  df[varname] = pd.read_csv(filename, header=None, usecols=[field], names=[varname], dtype=np.float64)
  dflist.append(df[varname])
  rf = pd.concat(dflist, axis=1)


#add cloumn weather
rf['weather'] = "None"
rf['precpctfinal'] = "None"
rf['snowpct'] = "None"
rf['rainpct'] = "None"


start_time = time.time()

# B) Calculate final precipitation probability
rf.loc[(rf['precpctfinal'] == "None"), 'precpctfinal'] = np.clip((rf['precpct'] + (np.clip((rf['rdrmax'] - 20),0,None)/2) + (np.clip((rf['cldave'] - 60),0,None)/4)),0,100).apply(lambda x: round(x,0))

# C) Calculate snow probability
zeroChgt = np.clip(rf['h0'],0,None).apply(lambda x: round(x,0))

# MeteoAdriatic Triangle method <start>
# ------------------ config start ---------------------#
MTM_TempFac=200         # Larger the number --> Positive t2m has LARGER influence on melting falling snow (default=200)
MTM_TriangleFac=700     # Larger the number --> Positive t2m AND positive zeroChgt have SMALLER influence on melting (default=500)
# ------------------- config end ----------------------#
#triangle = np.clip(((np.clip(rf['h0'],0,None).apply(lambda x: round(x,0)) + MTM_TempFac * rf['t2m'])/2),0,None).apply(lambda x: round(x,3))
#                    triangle=$(bc <<< "scale=3;((${zeroChgt}+${MTM_TempFac}*${t2m_arr[$i]})/2)")  # defines influence ratio of 0CHGT and t2m
#                    if [ 1 -eq "$(echo "${triangle} < 0" | bc)" ]
#                    then
#                        triangle=0
#                    fi
#snowpct = np.clip(rf['precpctfinal']*(1 - triangle / MTM_TriangleFac),0,100)
rf.loc[(rf['snowpct'] == "None"), 'snowpct'] = np.clip(rf['precpctfinal']*(1 - (np.clip(((np.clip(rf['h0'],0,None).apply(lambda x: round(x,0)) + MTM_TempFac * rf['t2m'])/2),0,None).apply(lambda x: round(x,3))) / MTM_TriangleFac),0,100).apply(lambda x: round(x,0))
#                    snowpct=$(bc <<< "scale=3;(${precpct}*(1-(${triangle}/${MTM_TriangleFac})))")     # defines snow probab. for triangle size
                # MeteoAdriatic Triangle method <end>
rf.loc[(rf['rainpct'] == "None"), 'rainpct'] = np.clip((rf['precpctfinal']-rf['snowpct']),0,100).apply(lambda x: round(x,0))


# F) Clouds and rain
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



'''
                rdmx=$(bc <<< "scale=3;(${rdrmax_arr[$i]}-20)")
                if [ 1 -eq "$(echo "${rdmx} < 0" | bc)" ] ; then rdmx=0 ; fi
                clav=$(bc <<< "scale=3;(${cldave_arr[$i]}-60)")
                if [ 1 -eq "$(echo "${clav} < 0" | bc)" ] ; then clav=0 ; fi
                precpct=$(bc <<< "scale=3;(${precpct_arr[$i]}+${rdmx}/2+${clav}/4)")
                precpct=`printf "%.0f\n" ${precpct}`
                if [ 1 -eq "$(echo "${precpct} < 0" | bc)" ]
                then
                    precpct=0
                fi
                if [ 1 -eq "$(echo "${precpct} > 100" | bc)" ]
                then
                    precpct=100
                fi
                if [ "$precpct" == "-0" ] ; then precpct=0 ; fi

'''
elapsed_time = time.time() - start_time

print(elapsed_time)
print(rf.to_string())


  
'''
cursor.close()

conn.close()
''' 

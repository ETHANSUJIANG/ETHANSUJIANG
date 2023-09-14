# -*- coding: utf-8 -*-
"""
Spyder Editor
Created by ZHUSJ

update command
conda update anaconda
conda install spyder=5.4.3

"""

import numpy as np
import pandas as pd
import os,time,re,sys,math
import snowflake.connector
import matplotlib.pyplot as plt 

"""
# three method to read excel file with multiple sheets
# method1 
# excel= pd.ExcelFile(path1）
# excel.sheet_name
# df = excel.parse(sheet_name='EF')
# method2
# df= pd.read_excel(path2,sheet_name=0)
# method3
# df= pd.read_excel(path3,sheet_name=None)
# df = pd.read_csv(path,encoding='ISO-8859-1',header=None)
"""

path = (r'C:\Users\zhusj\python\Project\Air_Ocea_Road.xlsx')
df_all = pd.ExcelFile(path)
# df_all.sheet_names
df_dist = df_all.parse(sheet_name='Distance')
df_vendor = df_all.parse(sheet_name='Countrycity')
# df_dist.columns
# country1 = df_dist['Origin Country (UN Country Code ISO 3166-1)'].unique()
df_dist.columns
# country2 = df_vendor['Vendor_country'].unique()
iso2orin = 'Origin Country (UN Country Code ISO 3166-1)'
iso2dest = 'Destination Country (UN Country Code ISO 3166-1)'
group1 = df_dist.groupby(['Main Carriage Transport Mode (Air, Ocean, Road)',\
                          iso2dest,'City_Desti'])[['City_Desti']].count()

group1.to_excel('20230913.xlsx')
# for i,j in enumerate(df_dist['Origin City']):
#     if len(j)==3:
#         code = df_dist.loc[i,iso2orin]+j
#         countrycode = df_dist.loc[i,iso2orin]
#         df1 = df_2AIRPORT[(df_2AIRPORT['Country_Code']==countrycode) & (df_2AIRPORT['IATA']==j)]
#         df2 = df_iso[df_iso['Countrycity']==code]
#         df3 = df_iata[df_iata['IATA code']==j]
#         if len(df1)>=1:
#             df_dist.loc[i,'City_Orin'] =df1['City'].tolist()
#         if len(df2)>=1:
#             df_dist.loc[i,'City_Orin'] =df2['City_name'].tolist()[0]
#         if len(df3)>=1:
#             df_dist.loc[i,'City_Orin'] =df3['City/Airport'].tolist()[0]
#     elif len(j)==5:
#         df1 = df_2AIRPORT[(df_2AIRPORT['Country_Code']==j[0:2]) & (df_2AIRPORT['IATA']==j[2:])]
#         df2 = df_iso[df_iso['Countrycity']==j]
#         df3 = df3 = df_iata[df_iata['IATA code']==j[2:]]
#         if len(df1)>=1:
#             df_dist.loc[i,'City_Orin'] =df1['City'].tolist()
#         if len(df2)>=1:
#             df_dist.loc[i,'City_Orin'] =df2['City_name'].tolist()[0]
#         if len(df3)>=1:
#             df_dist.loc[i,'City_Orin'] =df3['City/Airport'].tolist()[0]
#     elif len(j)>5:
#         df_dist.loc[i,'City_Orin'] =j

# for i,j in enumerate(df_dist['Destination City']):
#     if len(j)==3:
#         code = df_dist.loc[i,iso2dest]+j
#         countrycode = df_dist.loc[i,iso2dest]
#         df1 = df_2AIRPORT[(df_2AIRPORT['Country_Code']==countrycode) & (df_2AIRPORT['IATA']==j)]
#         df2 = df_iso[df_iso['Countrycity']==code]
#         df3 = df_iata[df_iata['IATA code']==j]
#         if len(df1)>=1:
#             df_dist.loc[i,'City_Desti'] =df1['City'].tolist()
#         if len(df2)>=1:
#             df_dist.loc[i,'City_Desti'] =df2['City_name'].tolist()[0]
#         if len(df3)>=1:
#             df_dist.loc[i,'City_Desti'] =df3['City/Airport'].tolist()[0]
#     elif len(j)==5:
#         df1 = df_2AIRPORT[(df_2AIRPORT['Country_Code']==j[0:2]) & (df_2AIRPORT['IATA']==j[2:])]
#         df2 = df_iso[df_iso['Countrycity']==j]
#         df3 = df3 = df_iata[df_iata['IATA code']==j[2:]]
#         if len(df1)>=1:
#             df_dist.loc[i,'City_Desti'] =df1['City'].tolist()
#         if len(df2)>=1:
#             df_dist.loc[i,'City_Desti'] =df2['City_name'].tolist()[0]
#         if len(df3)>=1:
#             df_dist.loc[i,'City_Desti'] =df3['City/Airport'].tolist()[0]
#     elif len(j)>5:
#         df_dist.loc[i,'City_Desti'] =j

# df_dist.to_excel('20230911.xlsx')



# with pd.ExcelWriter('Air_Ocea_Road.xlsx') as writer:
#     np_df.to_excel(writer, sheet_name = 'Distance')

str1 = 'OBSOLETE AND NOT REPLACED, ELECTRICAL FEEDTHROUGH ASSY, EVDT, TH FEEDTHROUGH, WETMATE/DRYMATE, 1 PIN, TELEDYNE DGO, 1375050-110, 15KSI WP, 302F/150C, 22.5KSI TP, 44.74 INCH. SHOULDER LENGTH F/ 5-15K EVDT TH'
str2 = 'OBSOLETE'
str2 in str1
num = re.search(r',', str1)
str1[26:]
num.start()
print(num)
"""
lat1 = 31.19790077
lat2 = 33.94250107
long1 = 121.3359985
long2 = -118.4079971

def arcDistance(lat1,long1,lat2,long2,R=6400):
    lat1 = lat1/180*math.pi
    lat2 = lat2/180*math.pi
    long1 = long1/180*math.pi
    long2 = long2/180*math.pi
    deltaX = abs(math.sin(long1)*math.cos(lat1)-math.sin(long2)*math.cos(lat2))
    deltaY = abs(math.cos(long1)*math.cos(lat1)-math.cos(long2)*math.cos(lat2))
    deltaZ = abs(math.sin(lat1)-math.sin(lat2))
    costheta = 1 - (deltaX**2+ deltaY**2+deltaZ**2)/2
    theta = math.acos(costheta)
    return theta, R*theta

theta,distance = arcDistance(lat1,long1,lat2,long2)
print(theta,distance)
"""
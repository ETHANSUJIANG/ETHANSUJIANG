# coding: utf-8
"""
Created on Mon Jul  3 14:15:13 2023

@author: zhusj
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import warnings
import snowflake.connector
import sys
import os
import re
warnings.filterwarnings('ignore')

PN='P1000194588'
PN_string = PN,PN
my_query = f"""
select
ltrim(a.aufnr,'0') as production_order,
a.WERKS as plant,
b.GLTRI as actual_finish_date,
ltrim(b.plnbez,'0') as PN0,
h.description as description,
ltrim(c.matnr,'0') as PN1,
c.wewrt as accounting_cost,
ltrim(d.kstar,'0') as cost_element,
d.wrttp as cost_type,
d.WTG001,d.WTG002,d.WTG003,d.WTG004,d.WTG005,d.WTG006,d.WTG007,
d.WTG008,d.WTG009,d.WTG010,d.WTG011,d.WTG012,d.WTG013,d.WTG014,
d.WTG015,d.WTG016
from rpl_sap_attunity.aufk as a
inner join rpl_sap_attunity.afko as b on b.aufnr = a.aufnr
inner join core.material as h on h.material = b.plnbez
inner join rpl_sap_attunity.afpo as c on c.aufnr = a.aufnr
inner join rpl_sap_attunity.coss as d on d.objnr = a.objnr
where b.terkz ='1'
and c.elikz ='X'
and d.wrttp ='04'
--and a.WERKS = '2101'
and PN0 in {PN_string}
order by a.aufnr
"""    # internal cost query 

my_query1 = f"""
select 
ltrim(a.aufnr,'0') as production_order,
a.WERKS as plant,
b.GLTRI as actual_finish_date,
ltrim(b.plnbez,'0') as PN0,
ltrim(c.matnr,'0') as PN1,
c.wewrt as accounting_cost,
ltrim(d.kstar,'0') as cost_element,
d.wrttp as cost_type,
d.WTG001,d.WTG002,d.WTG003,d.WTG004,d.WTG005,d.WTG006,d.WTG007,d.WTG008,
d.WTG009,d.WTG010,d.WTG011,d.WTG012,d.WTG013,d.WTG014,d.WTG015,d.WTG016
from rpl_sap_attunity.aufk as a
inner join rpl_sap_attunity.afko as b on b.aufnr = a.aufnr
inner join rpl_sap_attunity.afpo as c on c.aufnr = a.aufnr
inner join rpl_sap_attunity.cosp as d on d.objnr = a.objnr
where b.terkz ='1'
and c.elikz ='X'
and d.wrttp ='04'
and PN0 in {PN_string}
--and a.WERKS = '2101'
order by a.aufnr
"""  # query statement

my_query2 = f"""
select 
ltrim(a.aufnr,'0') as production_order,
a.WERKS as plant,
b.GLTRI as actual_finish_date,
ltrim(b.plnbez,'0') as PN0,
d.matnr as component_pn,
f.description as description,
case when d.waers ='USD'
    then d.ENWRT 
    else round(d.enwrt * g.exchange_rate,2)
end as cot_total_cost_usd,
d.waers as orin_currency,
d.enmng as qty_consumption,
d.saknr as cost_element,
d.erfmg as qty_per_unit,
d.ERFME as UOM,
d.baugr as toplevel_pn,
case when d.waers ='USD'
    then d.GPREIS 
    else round(d.GPREIS * g.exchange_rate,2)
end as unit_cost_usd
from rpl_sap_attunity.aufk as a
inner join rpl_sap_attunity.afko as b on b.aufnr = a.aufnr
inner join rpl_sap_attunity.afpo as c on c.aufnr = a.aufnr
inner join (select matnr,ENWRT,waers,aufnr,enmng,saknr,erfmg,erfme,baugr,gpreis,sbter,concat(waers::text,substring(sbter::text,1,8)) as 
                                                                                 uni_key from rpl_sap_attunity.resb ) as d on d.aufnr = a.aufnr
inner join core.material as f on f.material = d.matnr
left join (select from_currency, to_currency, exchange_rate,rate_date,concat(from_currency::text,substring(to_char(rate_date,'yyyymmdd')::text,1,8)) as 
                                                                              uni_key from SAP_REPORTING.CURRENCY_CONVERSION where to_currency='USD') as g on g.uni_key = d.uni_key
where b.terkz ='1'
and c.elikz ='X'
and PN0 in {PN_string}
order by a.aufnr
""" # query the purchased individual part cost 


# query data from snowfake
with snowflake.connector.connect( 
    user='ethan.zhu@technipfmc.com', # Required. Replace with your email 
    authenticator="externalbrowser", # Required. 
    account='technipfmc-data', # Required. 
    database="idsprod", # Optional 
    schema="rpl_sap.ekko", # Optional. Replace with the schema you will be working on 
    role="reporting", # Optional. Replace with the role you will be working with 
    warehouse="reporting_wh", # Optional. Replace with the warehouse you will be working with 
    client_store_temporary_credential=True, # Only if installing secure-local-storage to avoid reopening tabs
    ) as conn: 
    cursor = conn.cursor()
    cursor.execute(my_query)
    cursor1 = conn.cursor()
    cursor1.execute(my_query1)
    cursor2 = conn.cursor()
    cursor2.execute(my_query2)
    # res = cursor.fetchall() # To return a list of tuples 
    df_toplevelpn = cursor.fetch_pandas_all() # To return a dataframe
    df_externalcost = cursor1.fetch_pandas_all()
    df_individualpn = cursor2.fetch_pandas_all()
print(df_toplevelpn.head(2),df_toplevelpn.shape)
print(os.getcwd())
df_toplevelpn.to_excel('data/order.xlsx')
df_externalcost.to_excel(f'data/outcost{PN}.xlsx')
df_individualpn.to_excel(f'data/indicost{PN}.xlsx')
pro_order = df_toplevelpn['PRODUCTION_ORDER'].unique()
# prod_cols = df_toplevelpn.columns

tpname =df_toplevelpn['PN0'][0] +' '+ df_toplevelpn['DESCRIPTION'][0][:35]
print(tpname)

class InternalCost:
    @staticmethod
    def iTcostSum(data):
        order = data['PRODUCTION_ORDER'].unique()
        df_new = data.iloc[:,0:8]
        df_new.index = df_new['PRODUCTION_ORDER']
        df_new['WTG_SUM']=0.0
        wtg_col=['WTG'+'00'+str(i) for i in range(1,10)]+\
            ['WTG'+'0'+str(i) for i in range(10,17)]
        for i in order:
            df = data[data['PRODUCTION_ORDER']==i][wtg_col]
            df_new.loc[i,'WTG_SUM']=df.sum(axis=1).tolist()
            if len(df_new[df_new['PRODUCTION_ORDER']==i])>1:
                total = df_new[df_new['PRODUCTION_ORDER']==i]['WTG_SUM'].sum()
                idx =list(df_new[df_new['PRODUCTION_ORDER']==i].index)
                df_new.drop(index=idx[1:])
                df_new.loc[i,'WTG_SUM']=total
        return df_new

df_order_inter = InternalCost.iTcostSum(df_toplevelpn)           
df_order_inter.to_excel(f'data/{PN}sum_order.xlsx',index=False)




class ComponentCost:
    @staticmethod
    def ComponentLevel(data):
        # order = data['PRODUCTION_ORDER'].unique()
        data.sort_values('ACTUAL_FINISH_DATE')
        data1 = data[['PRODUCTION_ORDER','ACTUAL_FINISH_DATE','PN0','COMPONENT_PN',\
                      'DESCRIPTION','COT_TOTAL_COST_USD','UNIT_COST_USD']]
        df = data1.groupby('PRODUCTION_ORDER')
        df_dict=dict()
        k =0
        for i,j in df:
            df_dict[k]=j
            k+=1
        return df_dict

df_dict = ComponentCost.ComponentLevel(df_individualpn)
type(df_dict[1])
print(df_dict[0])
re.split("\s|','",'ACTUATOR SPRING HOUSING F/ AH700 (SUBSEA 2.0) 2 1/16-15K, W/ LINEAR OVERRIDE')

class Plot:
    @staticmethod
    def topLevelCost(data):
        x_y=data[['ACTUAL_FINISH_DATE','ACCOUNTING_COST']].\
        drop_duplicates('ACCOUNTING_COST',keep='last')
        x_y.sort_values('ACTUAL_FINISH_DATE')
        x= x_y['ACTUAL_FINISH_DATE']
        y= x_y['ACCOUNTING_COST']
        yper = [round((y.iloc[i+1]-y.iloc[i])/y.iloc[i]*100,0) for i in range(len(y)-1)]
        yper.insert(0,0)
        fig, ax = plt.subplots(figsize=(6,6))
        ax2 = ax.twinx()
        ax.bar(x, y,width =0.5,label ='Per unit cost')
        ax2.plot(x,yper,'*--r',label='cost rolling change by %')
        ax2.set_ylabel('%')
        ax.set_ylabel('usd')
        ax.tick_params(axis='x', rotation=-20)
        ax.set_ylim(y.min()*0.9,y.max()*1.1)
        ax.set_title(f'{PN} Per pcs cost')
        ax.set_xlabel('Prodduction date')
        ax.legend(loc = 'upper left')
        ax2.legend(loc ='best')
        plt.show()
    @staticmethod
    def partLevelCost(data,i=0):
        df = data[i]
        df['Percent']= df['COT_TOTAL_COST_USD']/sum(df['COT_TOTAL_COST_USD'])
        x_y0 = df[['COT_TOTAL_COST_USD','UNIT_COST_USD','Percent','DESCRIPTION']\
                 ].sort_values(by='COT_TOTAL_COST_USD', ascending=False)
        x_y= x_y0.iloc[0:10,]
        date = df['ACTUAL_FINISH_DATE'][0]
        topname = tpname+' '+'Puchased part cost'
        x= x_y['DESCRIPTION']
        y= x_y['COT_TOTAL_COST_USD']
        y1= x_y['UNIT_COST_USD']
        y2 = x_y['Percent']
        yper = [round(y2.iloc[i]*100,0) for i in range(len(y2))]
        fig, ax = plt.subplots(figsize=(6,6))
        ax2 = ax.twinx()
        ax.bar(x, y,width =0.5,alpha=0.5,label ='Subtotal Cost per unit system')
        ax.bar(x,y1,width =0.2,alpha =0.8,label = 'unit cost')
        ax2.plot(x,yper,'*--b',label='Cost Share by %')
        ax2.set_ylabel('%')
        ax.set_ylabel('usd')
        ax.tick_params(axis='x', rotation=-20)
        ax.set_ylim(y.min()*0.9,y.max()*1.1)
        plt.title(topname)
        ax.set_xlabel(date)
        ax.legend(loc = 'upper left')
        ax2.legend(loc ='best')
        plt.show()

Plot.partLevelCost(df_dict)



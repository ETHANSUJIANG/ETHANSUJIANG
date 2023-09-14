# -*- coding: utf-8 -*-
"""
Created on Thu May 11 16:09:04 2023

@author: zhusj
"""

# Purchased price only outsourcing part number
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import snowflake.connector as connector
my_query = """
select 
distinct
a.ebeln as PO,
b.ebelp as PO_item,
ltrim(b.matnr,'0')as PN,
e.description,
c.name1 as Vendor_name,
c.BRSCH as Industry_key,
c.land1,
b.netpr as purchased_price,
b.menge as PO_qty,
a.aedat as po_date,
d.eindt as delivery_date,
a.waers as original_currency,
case when a.waers = 'USD'
          then b.netpr
          else round(b.netpr*m.exchange_rate,2)
end as unit_price_usd,
c.zzcatman as Category_manager,
f.eknam as Buyer_name,
c.zzcatego as category,
zzsubcat as SCL,
b.werks
from (select ebeln,waers,bstyp,ekgrp,lifnr,aedat, concat(waers::text,substring(aedat::text,1,6)) as uni_key from rpl_sap.ekko) as a 
left join rpl_sap.t024 as f on a.ekgrp = f.ekgrp
left join rpl_sap.ekpo as b on a.ebeln = b.ebeln
left join rpl_sap.eket as d on b.ebeln = d.ebeln and b.ebelp = d.ebelp
left join rpl_sap.lfa1 as c on a.lifnr = c.lifnr
left join core.material as e on b.matnr = e.material
left join (SELECT from_currency, to_currency, exchange_rate, date_text,concat(from_currency,date_text) as uni_key
   FROM ( SELECT c.fcurr AS from_currency,
                 c.tcurr AS to_currency, 
                CASE
                    WHEN c.ffact = 0::numeric THEN 
                    CASE
                        WHEN c.ukurs < 0::numeric THEN power(c.ukurs, (-1)::numeric) * (-1)::numeric
                        ELSE c.ukurs
                    END
                    ELSE 
                    CASE
                        WHEN c.ukurs < 0::numeric THEN power(c.ukurs, (-1)::numeric) * (-1)::numeric * c.tfact / c.ffact
                        ELSE c.ukurs * c.tfact / c.ffact
                    END
                END AS exchange_rate,
         "substring"((99999999 - c.gdatu::bigint)::text, 1, 6) AS date_text,
         row_number() OVER(
         PARTITION BY "substring"(c.gdatu::text, 1, 6), c.fcurr, c.tcurr
          ORDER BY c.gdatu) AS ranking
           FROM rpl_sap_bi.tcurr as c
          WHERE c.kurst::text = 'M'::text AND c.tcurr::text = 'USD'::text) as c
  WHERE c.ranking = 1
  and date_text >='201201'
  order by date_text
 ) as m on m.uni_key = a.uni_key 
where b.loekz !='L'
and a.bstyp = 'F'
and a.aedat >to_char(CURRENT_DATE - INTERVAL '370 day','yyyymmdd')
and purchased_price >0.01
"""
with connector.connect( 
    user='ethan.zhu@technipfmc.com', # Required. Replace with your email 
    authenticator="externalbrowser", # Required. 
    account='technipfmc-data', # Required. 
    database="idsprod", # Optional 
    schema="public", # Optional. Replace with the schema you will be working on 
    role="reporting", # Optional. Replace with the role you will be working with 
    warehouse="reporting_wh", # Optional. Replace with the warehouse you will be working with 
    client_store_temporary_credential=True, # Only if installing secure-local-storage to avoid reopening tabs
    ) as conn: 
    cursor = conn.cursor() 
    cursor.execute(my_query)
    df = cursor.fetch_pandas_all() # To return a dataframe
print(df.shape,df.head(1))
data0 = df.copy()
#data cleaning
data0 = data0[data0.CATEGORY!= 'Intercompany']
data0.shape
data0.dropna(subset =['PN','VENDOR_NAME','PO_QTY','UNIT_PRICE_USD'],inplace=True)
data0.shape
# data slice by four columns
print(data0.shape)
vendor_PNs = data0[['PN','VENDOR_NAME','PO_QTY','UNIT_PRICE_USD']]
print(vendor_PNs.shape)
# cols = vendor_PNs.columns
# print(cols[0])
# print(type(vendor_PNs))
vendor_PNs = vendor_PNs[vendor_PNs['PN'] !=""]
vendor_PNs = vendor_PNs[vendor_PNs['VENDOR_NAME'] !=""]
vendor_PNs = vendor_PNs[vendor_PNs['PO_QTY'] !=""]
vendor_PNs = vendor_PNs[vendor_PNs['UNIT_PRICE_USD'] !=""]
vendor_PNs.drop_duplicates(inplace =True)
print(vendor_PNs.shape)
pnvendorcount = vendor_PNs.groupby(by =['PN','VENDOR_NAME']).size()     
print(pnvendorcount.shape)
def economicBatch(data):
    gb = data.groupby(by =['PN','VENDOR_NAME'])
    df = pd.DataFrame(columns= ['PN-VENDOR','Max_Price','Min_price','Mean_price','Economical_batch_size','Economical_price']
    for i,j in gb :
        lg = len(df)
        if len(j)==1:
            price1 = j['UNIT_PRICE_USD'][0]
            qty1 = j['PO_QTY'][0]
            df.loc[lg,'PN-VENDOR']= i[0]+'-'+i[1]
            df.loc[lg,'Max_Price']=price1
            df.loc[lg,'Min_price']=price1
            df.loc[lg,'Mean_price']=price1
            df.loc[lg,'Economical_batch_size']=qty1
            df.loc[lg,'Economical_price']=price1
        elif len(j)==2:
            max2 = j['UNIT_PRICE_USD'].max()
            mean2 = (j['UNIT_PRICE_USD']*j['PO_QTY']).sum()/j['PO_QTY']).sum()
            min2 = j['UNIT_PRICE_USD'].min()
            qty2 = j['PO_QTY'].min()
            df.loc[lg,'PN-VENDOR']= i[0]+'-'+i[1]
            df.loc[lg,'Max_Price']=max2
            df.loc[lg,'Min_price']=min2
            df.loc[lg,'Mean_price']=mean2
            df.loc[lg,'Economical_batch_size']=qty2
            df.loc[lg,'Economical_price']=min2
        elif len(j)==3:
            max3 = j['UNIT_PRICE_USD'].max()
            mean3 = (j['UNIT_PRICE_USD']*j['PO_QTY']).sum()/j['PO_QTY']).sum()
            min3 = j['UNIT_PRICE_USD'].min()
            j = j.sort_values(by='PO_QTY')
            x1 = np.array([[(j['PO_QTY'])/**0.2],[j['PO_QTY'])/**0.3]])
            x = np.insert(x1,x1.shape[1],1,axis=1)
            y = np.array(j['UNIT_PRICE_USD'])
            c = np.linalg.solve(x, y)
            xhat = np.linspace(1,50,1)
            yhat = round(c[0]/xhat**0.2+c[1]/xhat**0.3+c[2],2)
            
            # grad = -0.2*c[0]/xhat**1.2-0.3*c[1]/xhat**1.3
            # for i,j in enumerate(grad):
            #     if abs(j)<0.1:
            #         pass
            # df_xyk = np.hstack((x1))
            df.loc[lg,'PN-VENDOR']= i[0]+'-'+i[1]
            df.loc[lg,'Max_Price']=max3
            df.loc[lg,'Min_price']=min3
            df.loc[lg,'Mean_price']=mean3
            df.loc[lg,'Economical_batch_size']=qty3
            df.loc[lg,'Economical_price']=
        elif len(j)==4:
            max4 = j['UNIT_PRICE_USD'].max()
            mean4 = (j['UNIT_PRICE_USD']*j['PO_QTY']).sum()/j['PO_QTY']).sum()
            min4 = j['UNIT_PRICE_USD'].min()
            qty4 = 
            df.loc[lg,'PN-VENDOR']= i[0]+'-'+i[1]
            df.loc[lg,'Max_Price']=max4
            df.loc[lg,'Min_price']=min4
            df.loc[lg,'Mean_price']=mean4
            df.loc[lg,'Economical_batch_size']=qty4
            df.loc[lg,'Economical_price']=
        elif len(j)>=5:
            

        
        
        
        
        
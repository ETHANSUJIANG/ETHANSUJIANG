# -*- coding: utf-8 -*-
"""
GSP spend analysis identify 
by invoice receipt and goods receipt.
@author: zhusj
"""

import pandas as pd 
import numpy as np
import snowflake.connector
my_query = """SELECT 
CASE WHEN l.retpo = 'X' 
    then case when  h.waers = 'USD'
        THEN COALESCE(l.netwr,0) *-1
        ELSE round(COALESCE(l.netwr,0)*m.exchange_rate*-1,2)
        END 
     ELSE case when  h.waers = 'USD'
          THEN COALESCE(l.netwr,0) * 1
          ELSE round(COALESCE(l.netwr,0)*m.exchange_rate,2)
          END 
END AS amount_net_usd,
h.waers AS orin_currency,
l.aedat AS last_document_date, 
h.aedat  AS first_report_date, 
CONCAT(CONCAT(COALESCE(l.ebeln,''),'-'),COALESCE(l.ebelp,'')) AS po_line_number, 
l.txz01 as material_description,
lf.BRSCH as Industry_key,
tt.brtxt as Industry_name,
l.ebeln AS po_number, 
CASE WHEN (l.wepos = '' AND l.repos = 'X' AND l.elikz = '' AND l.erekz = '') 
OR (l.wepos = '' AND l.repos = 'X' AND l.elikz = 'X' AND l.erekz = '') 
OR (l.wepos = 'X' AND l.repos = '' AND l.elikz = '' AND l.erekz = '') 
OR (l.wepos = 'X' AND l.repos = '' AND l.elikz = '' AND l.erekz = 'X') 
OR (l.wepos = 'X' AND l.repos = 'X' AND l.elikz = '' AND l.erekz = '') 
OR (l.wepos = 'X' AND l.repos = 'X' AND l.elikz = '' AND l.erekz = 'X') 
OR (l.wepos = 'X' AND l.repos = 'X' AND l.elikz = 'X' AND l.erekz = '') 
THEN 'Open' 
ELSE 'Closed' 
END AS open_closed, 
CASE WHEN l.retpo = 'X' 
THEN l.menge * -1 
ELSE l.menge 
END AS po_quantity, 
rd.menge as GR_quantity,
l.meins AS unit_of_measure, 
case when  h.waers = 'USD' 
     THEN COALESCE(l.netpr,0) * 1
     ELSE round(COALESCE(l.netpr,0)*m.exchange_rate,2)
END as unit_price_usd,
'SAP' AS source,   
ltrim(h.lifnr,'0') AS erp_supplier_id, 
lf.name1 as Vendor_name,
l.werks AS plant_id, 
h.ekgrp as PG_ID,
ff.eknam as Buyer_name,
ltrim(l.matnr,'0') AS part_no, 
eket.eindt as po_item_del_Date, 
l.webaz as 	GR_Processing_Time_in_Days, 
rd.Posting_Date_in_the_Document as GR_Posting_Date, 
es.eindt as vendor_Confirmed_delivery_date 
from (select ebeln,waers,bstyp,ekgrp,bsakz,lifnr,aedat, concat(waers::text,substring(aedat::text,1,6)) as uni_key from rpl_sap.ekko) AS h 
JOIN rpl_sap.ekpo AS l ON l.ebeln = h.ebeln 
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
 ) as m on m.uni_key = h.uni_key 
left join rpl_sap.t024 as ff on h.ekgrp = ff.ekgrp
left JOIN rpl_sap.eket as eket on h.ebeln=eket.ebeln and l.ebelp = eket.ebelp
left join rpl_sap.lfa1 as lf on h.lifnr = lf.lifnr
left join (select SPRAS, brsch, BRTXT from rpl_sap.T016T where spras = 'E') as tt on lf.brsch = tt.brsch
left join ( 
Select 
ebeln, 
ebelp, 
max(eindt) as eindt 
from 
rpl_sap.ekes 
group by ebeln,ebelp 
) es on es.ebeln = l.ebeln and es.ebelp = l.ebelp 
left join ( 
select 
ebeln, 
ebelp, 
sum(menge) as menge,
max(budat) as Posting_Date_in_the_Document 
from 
rpl_sap.ekbe 
where 
bewtp = 'E' and 
bwart = '101' 
group by ebeln,ebelp 
) as rd on rd.ebeln = l.ebeln and rd.ebelp = l.ebelp 
WHERE h.bstyp = 'F' 
AND h.bsakz <> 'T' 
AND l.loekz = '' 
AND (((l.wepos = 'X' AND l.elikz = 'X') 
OR (l.wepos = '' AND l.elikz = '') 
OR (l.wepos = 'X' AND l.elikz = '')) 
OR ((l.repos = 'X' AND l.erekz = 'X') 
OR (l.repos = '' AND l.erekz = '') 
OR (l.repos = 'X' AND l.erekz = ''))) 
and substring(h.aedat,1,4)>='2023'
"""
with snowflake.connector.connect( 
    user='ethan.zhu@technipfmc.com', # Required. Replace with your email 
    authenticator="externalbrowser", # Required. 
    account='technipfmc-data', # Required. 
    database="idsdev", # Optional 
    schema="rpl_sap.ekko", # Optional. Replace with the schema you will be working on 
    role="reporting", # Optional. Replace with the role you will be working with 
    warehouse="reporting_wh", # Optional. Replace with the warehouse you will be working with 
    client_store_temporary_credential=True, # Only if installing secure-local-storage to avoid reopening tabs
    ) as conn: 
    cursor = conn.cursor()
    cursor.execute(my_query) 
    # res = cursor.fetchall() # To return a list of tuples 
    df = cursor.fetch_pandas_all() # To return a dataframe
print(df.head(2))
    # %%
'''
from snowflake.connector.pandas_tools import write_pandas
success, num_chunks, num_rows, output = write_pandas( 
        conn=conn,
        df=df,
        table_name="my_table",
        database=None,
        schema=None,)
'''

#df.insert(loc=25, column= 'Landing_Year',value=pd.to_datetime(df['GR_POSTING_DATE']).dt.year)
#names = df.columns
print(df.columns)
Vendorspend = df.groupby(['VENDOR_NAME','OPEN_CLOSED','Landing_Year'])['AMOUNT_NET_USD'].sum()
print(Vendorspend[:,'Closed',:].sort_values(ascending=False)[0:20])


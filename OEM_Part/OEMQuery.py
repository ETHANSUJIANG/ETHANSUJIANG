# -*- coding: utf-8 -*-
"""
GSP spend analysis identify 
by invoice receipt and goods receipt.
@author: zhusj
"""

import pandas as pd 
import numpy as np
import snowflake.connector
# query the industry key A410 more than 3 years purchased history
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
lf.zzcatego as category,
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
and substring(h.aedat,1,4)>='2019'
and lf.BRSCH = 'A410'
"""
# query Hwasin more than 3 years purchased history 
my_query0 = """
select 
a.ebeln as PO,
b.ebelp as PO_item,
ltrim(b.matnr,'0')as PN,
b.txz01 as material_description,
c.name1 as Vendor_name,
b.netpr as purchased_price,
b.menge as po_qty,
g.menge as gr_qty,
a.aedat as po_date,
d.eindt as delivery_date,
g.budat as landing_date,
a.waers as original_currency,
case when a.waers = 'USD'
          then b.netpr
          else round(b.netpr*m.exchange_rate,2)
end as unit_price_usd,
(unit_price_usd*po_qty) as po_values,
c.zzcatman as Category_manager,
f.eknam as Buyer_name,
c.zzcatego as category,
zzsubcat as SCL,
b.werks
from (select ebeln,bstyp,waers,ekgrp,lifnr,lands,aedat, concat(waers::text,substring(aedat::text,1,6)) as uni_key from rpl_sap.ekko) as a 
left join rpl_sap.t024 as f on a.ekgrp = f.ekgrp
inner join rpl_sap.ekpo as b on a.ebeln = b.ebeln
left join rpl_sap.eket as d on b.ebeln = d.ebeln and b.ebelp = d.ebelp
left join rpl_sap.lfa1 as c on a.lifnr = c.lifnr
left join rpl_sap.ekbe as g on b.ebeln = g.ebeln and b.ebelp = g.ebelp
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
where ltrim(c.lifnr,'0')='161194'
and a.aedat >='20200101'
order by a.aedat desc
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
    cursor0 = conn.cursor()
    cursor.execute(my_query)
    cursor0.execute(my_query0)
    # res = cursor.fetchall() # To return a list of tuples 
    df = cursor.fetch_pandas_all() # To return a dataframe
    df0 = cursor0.fetch_pandas_all()
print(df.head(2))
print(df0.head(2))
print(df.shape,df0.shape)
df_col = df.columns
df0_col =df0.columns
print(df.isnull().sum()) # amount the null cell number df.isnull() or df.isna()
df1 = df.dropna(subset=['GR_POSTING_DATE','GR_QUANTITY']) # drop the po have not delivery
print(df1.isnull().sum()) # amount again null cell number
print(df0['PO_DATE'][0:10])
# function for statistic letter,number and dash
def amountLetterNumber(data):
    ''' input data is PN pd.series
    get set of numbers of letter,digit and dash from PN listreturn a set type data '''
    st = set()
    for PN in data:
        lr_num = 0
        dt_num =0
        dash_num =0
        if len(PN)>7 and len(PN)<18:
            for i in PN:
                if i.isdigit():
                    dt_num+=1
                elif i.isalpha():
                    lr_num+=1
                elif i=='-':
                    dash_num+=1
            if dt_num>1 and lr_num <4:
                b =(lr_num,dt_num,dash_num)
                st.add(b)
    return st
# function for extract PN from material description
def getPN(data):
    '''extract PN from material description,
    input is pd.series and return is list'''
    pn_ls = pd.DataFrame(data=[],columns=['MATERIAL_DESCRIPTION','PART_NO'])
    for i in data:
        words = i.split()
        for j in words:
            lr_num = 0
            dt_num =0
            dash_num =0
            if len(str(j))>7 and len(str(j))<=16:
                for k in j:
                    if not(k.isdigit() or k.isalpha() or k=='-'):
                        break
                    if k.isdigit():
                        dt_num+=1
                    elif k.isalpha():
                        lr_num+=1
                    elif k=='-':
                        dash_num+=1
                pn = (lr_num,dt_num,dash_num)
                if pn in pn_set:
                    pn_ls=pn_ls.append({'MATERIAL_DESCRIPTION':i,'PART_NO':j},ignore_index=True)
                    break
    return pn_ls
#Hwasin_pn_description = getPN(df0.MATERIAL_DESCRIPTION.unique())
# data clear of df0 
HW_df = df0.dropna(subset=['PN','UNIT_PRICE_USD'])
print(HW_df.shape)
HW_df = HW_df[HW_df['PN']!='']
print(HW_df.shape)
len(HW_df['PN'].unique())
HW_PN_date = HW_df.groupby(['PN'])['PO_DATE'].max()
# ls = [len(i) for i in HW_PN_date.index]
# ls = np.asarray(ls)
# (ls<2).sum() # check the PN is whether has nothing
HW_PN_date['100038868']
def getLPP(data_orin,data_PN_date):
    '''create dataframe with 3 columns as below'''
    LPP_PN = pd.DataFrame(data =[],columns=['PN','PO_DATE','LPP'])
    for i in data_PN_date.index:
        LPP = (data_orin[data_orin['PN']==i][data_orin['PO_DATE']==data_PN_date[i]])['UNIT_PRICE_USD'].mean()
        LPP_PN = LPP_PN.append({'PN':i,'PO_DATE':data_PN_date[i],'LPP':LPP},ignore_index=True)
    return LPP_PN
LPP_PN = getLPP(HW_df,HW_PN_date)
LPP_PN.to_excel('data1/LPP_PN_HW.xlsx')
LPP_PN = LPP_PN[LPP_PN['LPP']>0.01]
# data clear of df 
print(df_col)
total_df_with_pn = df1[df1['PART_NO'] !='']
total_df_with_pn = df1[df1['GR_QUANTITY'] !='']
total_df_with_pn = df1[df1['GR_QUANTITY'] >0]
total_df_with_pn = df1[df1['AMOUNT_NET_USD'] !='']
total_df_with_pn = df1[df1['AMOUNT_NET_USD'] >0]
total_df_with_pn.insert(loc=0,column='Landing_Year',value= (pd.to_datetime(total_df_with_pn.GR_POSTING_DATE)).dt.year)
total_df_with_pn.insert(loc=0,column='Amt_USD_New',value= total_df_with_pn['GR_QUANTITY'].astype(float) * total_df_with_pn['UNIT_PRICE_USD'])
# print(total_df_with_pn.columns)
orin_annual_spend = total_df_with_pn.groupby('Landing_Year')['Amt_USD_New'].sum()
orin_annual_spend_ = total_df_with_pn.groupby('Landing_Year')['AMOUNT_NET_USD'].sum()
print(orin_annual_spend)
# print(total_df_with_pn.shape)


print(total_df_with_pn.columns)
def unitPriceReplace(data,data_replace):
    for i in data_replace.index:
        data[data['PART_NO']==i]['UNIT_PRICE_USD']=data_replace[i]
    return data
total_df_with_pn = unitPriceReplace(total_df_with_pn,hw_pn_avg_price)


total_df_with_pn.insert(loc=0,column='Amt_USD_New',value= total_df_with_pn['GR_QUANTITY'].astype(float) * total_df_with_pn['UNIT_PRICE_USD'])

hw_price_replace_annual_spend = total_df_with_pn.groupby('Landing_Year')['Amt_USD_New'].sum()

print(hw_price_replace_annual_spend)

print(orin_annual_spend)

# pn_set = amountLetterNumber(df1.PART_NO)#get the unique tuple number of letter,digit and dash
# len(pn_set) # look at the combination of  letter,digit and dash number of PN
# print(pn_set)
# ls_nothing = [{i:(df[i]=='').sum()} for i in df_col] 
# ls_nothing
ls_no_pn =df1[df1['PART_NO']=='']['MATERIAL_DESCRIPTION']
#ls_no_pn[0:10]
#df['CATEGORY'].value_counts() #look at is there an intercompany PO 
# df['OPEN_CLOSED'].value_counts() # take a glance PO line amount open or closed

pn_list = getPN(ls_no_pn)             
print(pn_list)
print(ls_no_pn.shape)
len(pn_list)
print(df1.shape)
df1.insert(loc=0,column='Landing_Year',value= (pd.to_datetime(df1.GR_POSTING_DATE)).dt.year)
df1.Landing_Year.value_counts()# transaction qty yearly
df1_col = df1.columns
print(df1_col)
Hwasin = df1[df1['ERP_SUPPLIER_ID']=='161194']
HS_PN = Hwasin['PART_NO'].unique()
HS_PN = pd.Series(HS_PN)
HS_PN.dropna()
true_false =  [ (i in HS_PN) for i in df1.PART_NO]
HW_PN_data = df1[true_false]
print(HW_PN_data.shape)
Hwasin_spend_year =Hwasin.groupby('Landing_Year')['AMOUNT_NET_USD'].sum()
HW_PN_data.insert(loc =15,column='Cal_Sum',value = HW_PN_data['UNIT_PRICE_USD'] * HW_PN_data['PO_QUANTITY'].astype(float))
HW_Amount_qty = Hwasin.groupby('PART_NO')['PO_QUANTITY'].sum()
HW_avg_unit_price = HW_Amount_value/HW_Amount_qty
len(HW_PN_data['PART_NO'].unique())
HW_PN_2019_spend = HW_PN_2019['AMOUNT_NET_USD'].sum()
total_qty_2019 = HW_PN_2019.groupby('PART_NO')['PO_QUANTITY'].sum()
uni_PN_2019 = HW_PN_2019['PART_NO'].unique()
HW_avg_unit_price*total_qty_2019



# def repeatPN(data):
#     pn = set(data.PART_NO)
#     b =set((str(2019),str(2020),str(2021),str(2022)))
#     unique=[]
#     for i in pn:
#         m = data.loc[i]
#         if b.issubset(set((m))):
#             unique.append(i)
#     return pd.Series(unique)

'''
from snowflake.connector.pandas_tools import write_pandas
success, num_chunks, num_rows, output = write_pandas( 
        conn=conn,
        df=df,
        table_name="my_table",
        database=None,
        schema=None,)
'''
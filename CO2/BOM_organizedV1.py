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
warnings.filterwarnings('ignore')


class LoadingData:
    @staticmethod
    def LoadingData(PN):
        PN = PN+'.xlsx'
        path = os.path.join('data',PN)
        data = pd.read_excel(path)
        EF_PR = EF_PR = pd.read_excel('data/EF_vendor_region.xlsx')
        return data,EF_PR
# data = pd.read_excel('data/P1000220410.xlsx')# read BOM data
# EF_PR = pd.read_excel('data/EF_vendor_region.xlsx')
data, EFe= LoadingData.LoadingData('P7000073756')

class GetEF:
    @staticmethod
    def getEF(EF_PR):
        columns = EF_PR.columns
        EFe = {i:2 for i in columns}
        for i in columns:
            for j in EF_PR[i]:
                if not pd.isna(j):
                    EFe[i]=j
                    break
        return EFe

EFe = GetEF.getEF(EFe)
####################################
# EF_vendor = pd.read_excel('data/Master file -Survey based.xlsx',sheet_name ='GHG Calculation')# read BOM data
# cols= ['Supplier Name','Supplier Country','Please choose reporting year:','Total EVG EF']
# EF_VD = EF_vendor[EF_vendor['Direct/Indirect']=='Direct'][cols]
# EF_VD.dropna(subset=['Total EVG EF'],inplace=True)
# EF_VD.where(EF_VD['Total EVG EF']>1e-5,inplace =True)
# EF_VD.dropna(subset=['Total EVG EF'],inplace=True)
# EF_VD1 = EF_VD.groupby(['Supplier Name','Supplier Country'])[['Total EVG EF']].mean().sort_values(by='Supplier Country')
# EF_VD1.to_excel('data/EF_vendor_region2.xlsx')
#####################################
# clear data 
class DataClear:
    @staticmethod
    def dataClear(data):
        data0 = data.dropna(subset=['Part','Level']) # drop rows without part and level row
        data0= data0.reset_index(drop=True) # reset the index again
        col = data0.columns
        for i in col:
            if i!='Qty':
                if i =='Level':
                    if data0[i].dtype=='float64':
                        data0[i]=data0[i].astype(int)
                        data0[i]=data0[i].astype(str)
                    elif data0[i].dtype=='O':
                        pass 
                else:
                    data0[i]=data0[i].astype(str)
        data0['Part'][1:]= pd.Series([i[(i.count('+')+1):] for i in data0['Part'][1:]])
        data0.replace(u'\xa0\xa0\xa0\xa0','',inplace=True)
        data0.replace(u'\xa0','',inplace=True)
        return data0
data0 =DataClear.dataClear(data)
# Qeury data 
PN_string = tuple(set(data0['Part']))# get the PN list to be query as tuple 
my_query = f"""
select
distinct
ltrim(h.material,'0') as Part,
h.description,
h.components,
h.PROD_FAMILY,
h.ELASTOMER_SPECS,
h.COATING_SPECS,
h.prod_line,
h.PC1,
h.PC2,
h.OEM_VENDOR
from core.material as h
where Part in {PN_string}
"""  # query statement

my_query1 = f"""
select
distinct
ltrim(a.matnr,'0') as Part,
a.aedat as purchase_date,
c.lifnr as vendor_ID,
c.name1 as vendor_nanme,
c.land1 as vendor_country
from rpl_sap.ekpo as a 
left join rpl_sap.ekko as b on a.ebeln = b.ebeln
left join rpl_sap.lfa1 as c on b.lifnr = c.lifnr
where Part in {PN_string}
and a.aedat > '20210101'
"""  # query statement

# query data from snowfake
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
    cursor1 = conn.cursor()
    cursor1.execute(my_query1)
    # res = cursor.fetchall() # To return a list of tuples 
    df_toplevelpn = cursor.fetch_pandas_all() # To return a dataframe
    df_externalcost = cursor1.fetch_pandas_all()
# print(df_toplevelpn.head(2))
# len(df_toplevelpn.PART.unique())
class DFClear:
    @staticmethod
    def dfClear(df):
        col = df.columns
        for i in col:
            df[i]=df[i].astype(str)
        return df
df_toplevelpn =DFClear.dfClear(df_toplevelpn)
# df_externalcost =DFClear.dfClear(df_externalcost)


# organize one dataframe purchased history with different regions
class PNCountry:
    @staticmethod
    def pnCountry(df):
        df0 = df[['PART','VENDOR_COUNTRY']]
        df0.drop_duplicates(keep ='first',inplace=True)
        PN = list(df0['PART'].unique())
        out= pd.DataFrame()
        for _, j in enumerate(PN):
            if len(df0[df0['PART']==j])==0:
                cty_list = []
                cty_list.append(j)
            else:
                cty_list = list(df0[df0['PART']==j]['VENDOR_COUNTRY'])
            cty_list.append(j)
            lth = len(cty_list)
            cty_col = ['CTY'+str(k+1) for k in range(lth-1)]
            cty_col.append('Part')
            df1 = pd.DataFrame(data=[cty_list],columns=cty_col)
            out = pd.concat([out,df1])
        out.replace('nan','',inplace=True)
        out.sort_values(by=cty_col,na_position='last',inplace=True)
        part = out.pop('Part')
        out.insert(loc=0,column=('Part'),value=part)
        # out.sort_index(axis=1,ascending=False,inplace=True)
        return out
# df_pncty = PNCountry.pnCountry(df_externalcost)
# df_pncty.head(2)
# df_pncty.shape

# general class for concatenate two DF
class Twodfconcat:
    @staticmethod
    def combine(source,target,index=False,start=1):
        if index:
            if 'Part' in target.columns:
                target.index = target['Part']
            elif 'PART' in target.columns:
                target.index = target['PART']
        cols= target.columns
        slice_col = cols[start:]
        lth = source.shape[1]
        for i,j in enumerate(slice_col):
            source.insert(lth+i,column=j,value=None)
        if 'PART' in source.columns:
            for k,l in enumerate(source.PART):
                if len(target[target['Part']==l])==0:
                    pass
                else:
                    source.loc[k,slice_col]=target.loc[l,slice_col]
        elif 'Part' in source.columns:
            for l,k in enumerate(source.Part):
                source.iloc[l,lth:]=target.loc[k,slice_col]
        else:
            for l,k in enumerate(source.index):
                source.loc[k,slice_col]=target.loc[k,slice_col]
        return source
# concatenate vendor region information
# df_0 = Twodfconcat.combine(df_toplevelpn,df_pncty,index=True)
df_0 = df_toplevelpn
# define the class to prepare and clear the data 
class DataPrepare:
    # def __init__(self):
    #     self.dataWeight = dataWeight
    #     self.subTotal = subTotal
    #     self.singleAssy = singleAssy
    #     self.rawLevel = rawLevel
    def dataWeight(self,source_data):
        '''insert a new column with weight_kg '''
        cl_data = pd.DataFrame()
        a=0.0
        for i in source_data['Weight LBS(KGS)']:
            if i == '':
                a = 0.01
            elif '(' in set(i):
                a1=i.find('(')
                a = max(float(i[a1+1:-1]),0.01)
            else:
                a = max(round(float(i)/2.2,2),0.01)
            cl_data =cl_data.append({'Weight_kg':a,'Weight LBS(KGS)':i},ignore_index=True)
        source_data['Weight LBS(KGS)']=cl_data['Weight_kg']
        source_data.rename(columns={'Weight LBS(KGS)':'Weight_kg'},inplace=True)
        # return self.source_data
    def subTotal(self,source_data):
        '''function amount subtotal qty per line'''
        df0 = pd.DataFrame(data=[],columns=['Level','Sub_total_qty'])
        for i,j in enumerate(source_data['Level']): 
            if j=='1' or j=='2':    
                df0=df0.append({'Level':source_data.iloc[i,:]['Level'],
                                'Sub_total_qty':source_data.iloc[i,:]['Qty']},ignore_index=True)
            elif 'A' in str(j):
                df0=df0.append({'Level':source_data.iloc[i,:]['Level'],'Sub_total_qty':0},
                            ignore_index=True)
            else:
                upper_level = str(int(j)-1)# get upper level string
                upper_df= df0[df0['Level']==upper_level]['Sub_total_qty'] # get upper string qty series
                upper_index = list(upper_df.index) # convert index to list
                upper_above_close = upper_index[-1]
                upper_level_qty = upper_df[upper_above_close]
                df0 =df0.append({'Level':source_data.iloc[i,:]['Level'],
                                 'Sub_total_qty':source_data.iloc[i,:]['Qty']*upper_level_qty},
                                 ignore_index=True)
            # this for loop setup alternative parts itself and below parts substotal is 0
            for i,j in enumerate(df0['Level']):
                if 'A' in j:
                    level_A = j[0]
                    df1 = df0.iloc[i:,:]
                    lower_level = str(int(level_A)+1)
                    level_list = list(df1[df1['Level']==level_A].index)
                    up_level = str(int(level_A)-1)
                    if len(level_list)==0:
                        up_list = list(df1[df1['Level']==up_level].index)
                        lower_list = list(df1[df1['Level']==lower_level].index)
                        if len(up_list)==0:
                            if len(lower_list) >=1:
                                df0.loc[i:,'Sub_total_qty']=0 
                        elif len(up_list)>=1:
                            if len(lower_list) >=1:
                                df0.loc[i:up_list[0],'Sub_total_qty']=0 
                    elif len(level_list)>=1:
                        df2 = df0.iloc[i:level_list[0],:]
                        up_list = list(df2[df2['Level']==up_level].index)
                        lower_list = list(df2[df2['Level']==lower_level].index)
                        if len(up_list)==0:
                            if len(lower_list) >=1:
                                df0.loc[i:level_list[0]-1,'Sub_total_qty']=0 
                        elif len(up_list)>=1:
                            if len(lower_list) >=1:
                                df0.loc[i:up_list[0],'Sub_total_qty']=0 
        source_data.insert(loc=22,column='Sub_total_qty',value=df0['Sub_total_qty'])
        # return self.source_data
    def singleAssy(self,source_data):
        '''add up one column as label distinguish is single or assy part
        identify each of line is single part or assy part '''
        lst =[]
        for i,j in enumerate(source_data['Level'].astype(str)):
            if j=='1':
                lst.append('TopAssy')
            elif 'A' in set(j):
                lst.append(' ')
            else:
                df1 = source_data.iloc[i:,:]
                level_list = list(df1[df1['Level']==j]['Level'].index)
                lower_level = str(int(j)+1)
                up_level = str(int(j)-1)
                level_list = list(df1[df1['Level']==j].index)
                # slice below this PN all items
                if len(level_list)==1:
                    df1 = source_data.iloc[i:,:]
                elif len(level_list)>=2:
                    df1 =source_data.iloc[i:level_list[1],:]
                up_list = list(df1[df1['Level']==up_level].index)
                if len(up_list)==0:
                    pass
                elif len(up_list)>=1:
                    df1=source_data.iloc[i:up_list[0],:]
                lower_list = list(df1[df1['Level']==lower_level].index)
                if len(lower_list)<=1:
                    lst.append('Single')
                else:
                    # second_j = level_list[1]
                    # lower_df = source_data.iloc[i:second_j,:]
                    # lower_level = str(int(j)+1)
                    # lower_list = lower_df[lower_df['Level']==lower_level]
                    # if len(lower_list) <=1:
                    #     lst.append('Single')
                    # else:
                    lst.append('Assy')
        source_data.insert(loc =source_data.shape[1],column='Single_ASSY',value=lst)
        # return self.source_data
    def rawLevel(self,source_data,target_data):
        '''insert couple of columns into data0 '''
        if len(target_data)<1:
            pass
        else:
            cols =['COMPONENTS','PROD_FAMILY','ELASTOMER_SPECS',
                   'COATING_SPECS','PC1','PC2','OEM_VENDOR']
            for l,k in enumerate(cols):
                source_data.insert(loc=2+l,column=k,value=None)
            for i,j in enumerate(source_data['Part']):
                source_data.iloc[i,2:len(cols)+2]=target_data[target_data['PART']==j].iloc[0,:][cols]
            # df_3col = pd.DataFrame()
            # for i,j in enumerate(source_data['Part']):
            #     df_ = target_data[target_data['PART']==j][cols]
            #     df_3col = df_3col.append(df_,ignore_index=True)
            # for k,l in enumerate(df_3col.columns):
                
            #     source_data.insert(loc=2+k,column=l,value =df_3col[l])
            # # return self.source_data
    def getData(self,source_data,target_data):
        self.dataWeight(source_data)
        self.subTotal(source_data)
        self.singleAssy(source_data)
        self.rawLevel(source_data,target_data)
        return source_data
test_data = DataPrepare()
data_rawlv=test_data.getData(data0,df_0)
print( f'data_rawlv shape is {data_rawlv.shape}')
# data_rawlv.to_excel('data/P1000220410_edit5.xlsx',index =False)# write clear data to local file

# class for isolate text into each of word
class StrIsolate:
    # def __init__(self,string):
    #     self.string = string
    def split(self,string):
        setA = set()
        if len(string)<=1:
            pass
        elif len (string)>=2:
            strA= string.split()
            for i in strA:
                if len(i)<=1:
                    pass
                else:
                    a = i.split(sep=',')
                    for j in a:
                        if len(j)<4:
                            pass
                        elif len(j)>4 :
                            setA.add(j)
                    b = i.split(sep=':')
                    for k in b:
                        if len(k)<4:
                            pass
                        elif len(k)>4:
                            setA.add(k)
                    c = i.split(sep='/')
                    for l in c:
                        if len(l)<4:
                            pass
                        elif len(l)>4:
                            setA.add(l)
        return setA

# class to recognize each of PN in which mfg process 
class ProcessIdentify:
    def __init__(self,data):
        self.word = StrIsolate()
        self.data =data
        self.lst = list(set(self.data['Part']))
        self.cat = ['ASSY','FORGING','MACHINING','OEM_METAL','OEM_NONMETAL','CASTING','CHEMICAL',
                    'CLADDING','FABRICATION','COATING','RAW_METAL','RAW_NONMETAL']
        self.df  = pd.DataFrame(data=0,index=self.lst,columns=self.cat)
    def chemical(self,PN):
        if self.data[self.data['Part']==PN].iloc[0,:]['PROD_FAMILY']=='CHEMICAL':
            self.df.loc[PN,:]['CHEMICAL']=1
    def assy(self,PN):
        if self.data[self.data['Part']==PN].iloc[0,:]['Single_ASSY'] =='TopAssy':
            self.df.loc[PN,:]['ASSY']=1
        elif self.data[self.data['Part']==PN].iloc[0,:]['Single_ASSY'] =='Assy':
            self.df.loc[PN,:]['ASSY']=1
    def casting(self,PN):
        if self.data[self.data['Part']==PN].iloc[0,:]['Single_ASSY'] =='Single':
            if len(self.data[self.data['Part']==PN].iloc[0,:]['OEM_VENDOR'])<5:
                if len(self.data[self.data['Part']==PN].iloc[0,:]['COMPONENTS'])<5:
                    word1 = self.word.split(self.data[self.data['Part']==PN].iloc[0,:]['Quality Specs'])
                    word2 = self.word.split(self.data[self.data['Part']==PN].iloc[0,:]['Engineering Specs'])
                    if 'Q00328' in word1:
                        self.df.loc[PN,:]['CASTING']=1
                        self.df.loc[PN,:]['RAW_METAL']=1
                    elif len(self.data[self.data['Part']==PN].iloc[0,:]['Engineering Specs'])>5:
                        if 'P60157' in word2:
                            self.df.loc[PN,:]['CASTING']=1
                            self.df.loc[PN,:]['RAW_METAL']=1
    def coating(self,PN):
        c_prefix =('C80','C81','C82')
        if len(self.data[self.data['Part']==PN].iloc[0,:]['Engineering Specs'])>5:
            word = self.word.split(self.data[self.data['Part']==PN].iloc[0,:]['Engineering Specs'])
            for i in word:
                for j in c_prefix:
                    if j in i:
                        self.df.loc[PN,:]['COATING']=1
    def OEM(self,PN):
        if self.data[self.data['Part']==PN].iloc[0,:]['Single_ASSY'] =='Single':
            if len(self.data[self.data['Part']==PN].iloc[0,:]['OEM_VENDOR'])>=5:
                if len(self.data[self.data['Part']==PN].iloc[0,:]['Engineering Specs'])>5:
                    word0 = self.word.split(self.data[self.data['Part']==PN].iloc[0,:]['Engineering Specs'])
                    for i in word0:
                        if 'E5' in i:
                            self.df.loc[PN,:]['OEM_NONMETAL']=1
                            self.df.loc[PN,:]['RAW_NONMETAL']=1
                        elif 'M1' in i:
                            self.df.loc[PN,:]['OEM_METAL']=1
                            self.df.loc[PN,:]['RAW_METAL']=1
                        elif 'M2' in i:
                            self.df.loc[PN,:]['OEM_METAL']=1
                            self.df.loc[PN,:]['RAW_METAL']=1
                        elif 'M3' in i:
                            self.df.loc[PN,:]['OEM_METAL']=1
                            self.df.loc[PN,:]['RAW_METAL']=1
                        elif 'M4' in i:
                            self.df.loc[PN,:]['OEM_METAL']=1
                            self.df.loc[PN,:]['RAW_METAL']=1
                elif len(self.data[self.data['Part']==PN].iloc[0,:]['Engineering Specs'])<5:
                    if sum(self.df.loc[PN,:][self.cat])<1:
                        self.df.loc[PN,:]['OEM_METAL']=1
                        self.df.loc[PN,:]['RAW_METAL']=1
            elif len(self.data[self.data['Part']==PN].iloc[0,:]['OEM_VENDOR'])<5:
                if len(self.data[self.data['Part']==PN].iloc[0,:]['PROD_FAMILY'])>=5:
                    Nonmetal = ('O-RING','ELASTOMERS','INSULATION MATERIAL')
                    metal = ('WASHER','LABELS','KEY','CABLE','SPRING','HYDRAULIC TUBE/PIPE','PIPE-TUBE-FITTINGS',
                             'FASTERNS','HYDRAULIC COUPLER COMPONENTS','ANODE','SCREW','TUBULAR')
                    for i in Nonmetal:
                        if self.data[self.data['Part']==PN].iloc[0,:]['PROD_FAMILY']==i:
                            self.df.loc[PN,:]['OEM_NONMETAL']=1
                            self.df.loc[PN,:]['RAW_NONMETAL']=1
                    for j in metal:
                        if self.data[self.data['Part']==PN].iloc[0,:]['PROD_FAMILY']==j:
                            if sum(self.df.loc[PN,:][self.cat])<1:
                                self.df.loc[PN,:]['OEM_METAL']=1
                                self.df.loc[PN,:]['RAW_METAL']=1
                    word2 = self.word.split(self.data[self.data['Part']==PN].iloc[0,:]['PROD_FAMILY'])
                    terms = ('FASTENER','SCREW','SPRING')
                    for i in word2:
                        for j in terms:
                            if j in i:
                                self.df.loc[PN,:]['OEM_METAL']=1
                                self.df.loc[PN,:]['RAW_METAL']=1
                    if len(self.data[self.data['Part']==PN].iloc[0,:]['Quality Specs'])>=5:
                        word3 = self.word.split(self.data[self.data['Part']==PN].iloc[0,:]['Quality Specs'])
                        if 'Q00500' in word3:
                            if sum(self.df.loc[PN,:][self.cat])<1.0:
                                self.df.loc[PN,:]['OEM_METAL']=1
                                self.df.loc[PN,:]['RAW_METAL']=1
                    elif 'SEAL' in word2:
                        if self.data[self.data['Part']==PN].iloc[0,:]['Model']=='NO-DWG':
                            word4 = self.word.split(self.data[self.data['Part']==PN].iloc[0,:]['Quality Specs'])
                            q_spec = ('Q03801','Q03802','Q03803')
                            for i in q_spec:
                                if i in word4:
                                    if sum(self.df.loc[PN,:][self.cat])<1.0:
                                        self.df.loc[PN,:]['OEM_NONMETAL']=1
                                        self.df.loc[PN,:]['RAW_NONMETAL']=1
                                else:
                                    if sum(self.df.loc[PN,:][self.cat])<1.0:                            
                                        self.df.loc[PN,:]['OEM_METAL']=1
                                        self.df.loc[PN,:]['RAW_METAL']=1
                    elif len(self.data[self.data['Part']==PN].iloc[0,:]['COMPONENTS'])<5:
                        if len(self.data[self.data['Part']==PN].iloc[0,:]['Engineering Specs'])<5:
                            if self.data[self.data['Part']==PN].iloc[0,:]['Weight_kg']<1.0:
                                if sum(self.df.loc[PN,:][self.cat])<1.0:
                                    self.df.loc[PN,:]['OEM_NONMETAL']=1
                                    self.df.loc[PN,:]['RAW_NONMETAL']=1
                        elif len(self.data[self.data['Part']==PN].iloc[0,:]['Engineering Specs'])>=5:
                            if self.data[self.data['Part']==PN].iloc[0,:]['Model']=='NO-DWG':
                                if self.data[self.data['Part']==PN].iloc[0,:]['Weight_kg']<1.0:
                                    if sum(self.df.loc[PN,:][self.cat])<1.0:
                                        self.df.loc[PN,:]['OEM_NONMETAL']=1
                                        self.df.loc[PN,:]['RAW_NONMETAL']=1
                                elif sum(self.df.loc[PN,:][self.cat])<1.0:
                                    self.df.loc[PN,:]['OEM_METAL']=1
                                    self.df.loc[PN,:]['RAW_METAL']=1
                    elif self.data[self.data['Part']==PN].iloc[0,:]['Model']=='NO-DWG':
                        if len(self.data[self.data['Part']==PN].iloc[0,:]['Engineering Specs'])<5:
                            if sum(self.df.loc[PN,:][self.cat])<1.0:
                                print('OEM_METAL',PN)
                                self.df.loc[PN,:]['OEM_METAL']=1
                                self.df.loc[PN,:]['RAW_METAL']=1
    def forging(self,PN):
        if self.data[self.data['Part']==PN].iloc[0,:]['Single_ASSY'] =='Single':
            if self.data[self.data['Part']==PN].iloc[0,:]['PROD_FAMILY']=='BILLET':
                self.df.loc[PN,:]['FORGING']=1
                self.df.loc[PN,:]['RAW_METAL']=1
            elif self.data[self.data['Part']==PN].iloc[0,:]['PROD_FAMILY']=='FORGING':
                self.df.loc[PN,:]['FORGING']=1
                self.df.loc[PN,:]['RAW_METAL']=1
            elif self.data[self.data['Part']==PN].iloc[0,:]['PROD_FAMILY']=='BAR STOCK':
                self.df.loc[PN,:]['FORGING']=1
                self.df.loc[PN,:]['RAW_METAL']=1
            elif self.data[self.data['Part']==PN].iloc[0,:]['PROD_FAMILY']=='SEMI FINISHED':
                self.df.loc[PN,:]['FORGING']=1
                self.df.loc[PN,:]['RAW_METAL']=1
            elif len(self.data[self.data['Part']==PN].iloc[0,:]['OEM_VENDOR'])<5:
                if len(self.data[self.data['Part']==PN].iloc[0,:]['COMPONENTS'])<5:
                    if len(self.data[self.data['Part']==PN].iloc[0,:]['Engineering Specs'])>5:
                        word = self.word.split(self.data[self.data['Part']==PN].iloc[0,:]['Engineering Specs'])
                        spec = ('P60161','M2','M3','M40')
                        for i in word:
                            for j in spec:
                                if j in i and self.data[self.data['Part']==PN].iloc[0,:]['Weight_kg']>0.5:
                                    self.df.loc[PN,:]['FORGING']=1
                                    self.df.loc[PN,:]['RAW_METAL']=1
    def machining(self,PN):
        m_spec =('E55','E50','E47')
        eng_words = self.word.split(self.data[self.data['Part']==PN].iloc[0,:]['Engineering Specs'])
        level = self.data[self.data['Part']==PN].iloc[0,:]['Level']
        if 'A' in level:
            level = level[0]
        else:
            pass
        lower_level = str(int(level)+1)
        up_level = str(int(level)-1)
        idx =list(self.data[self.data['Part']==PN].index)
        level_df =self.data.iloc[idx[0]:,:]
        level_list = list(level_df[level_df['Level']==level].index)
        # slice below this PN all items
        if len(level_list)==1:
            lower_df = self.data.iloc[level_list[0]:,:]
        elif len(level_list)>=2:
            lower_df =self.data.iloc[level_list[0]:level_list[1],:]
        up_list = list(lower_df[lower_df['Level']==up_level].index)
        if len(up_list)==0:
            pass
        elif len(up_list)>=1:
            lower_df=self.data.iloc[idx[0]:up_list[0],:]
        lower_list = list(lower_df[lower_df['Level']==lower_level].index)                                      
        weight_level = self.data[self.data['Part']==PN].iloc[0,:]['Weight_kg']
        if len(lower_list)==0:
            for i in m_spec:
                for j in eng_words:
                    if i in j:
                        if self.df.loc[PN,:]['OEM_METAL']==0 and self.df.loc[PN,:]['OEM_NONMETAL']==0:
                            self.df.loc[PN,:]['MACHINING']=1
                            self.df.loc[PN,:]['RAW_NONMETAL']=1
            if len(self.data[self.data['Part']==PN].iloc[0,:]['Engineering Specs'])<5:
                if self.df.loc[PN,:]['OEM_METAL']==0 and self.df.loc[PN,:]['OEM_NONMETAL']==0:
                    self.df.loc[PN,:]['MACHINING']=1
                    self.df.loc[PN,:]['RAW_NONMETAL']=1
        elif len(lower_list)==1:
            weight_lower = self.data.iloc[lower_list[0],:]['Weight_kg']
            lower_qty = self.data.iloc[lower_list[0],:]['Qty']
            if weight_level <= weight_lower*lower_qty:
                self.df.loc[PN,:]['MACHINING']=1
        elif len(lower_list)==2:
            weight_lower0 = self.data.iloc[lower_list[0],:]['Weight_kg']
            weight_lower1 = self.data.iloc[lower_list[1],:]['Weight_kg']
            lower_qty0 = self.data.iloc[lower_list[0],:]['Qty']
            lower_qty1 = self.data.iloc[lower_list[1],:]['Qty']
            if weight_level < (weight_lower0*lower_qty0 + weight_lower1*lower_qty1):
                self.df.loc[PN,:]['MACHINING']=1
        elif len(lower_list)==3:
            weight_lower0 = self.data.iloc[lower_list[0],:]['Weight_kg']
            weight_lower1 = self.data.iloc[lower_list[1],:]['Weight_kg']
            weight_lower2 = self.data.iloc[lower_list[2],:]['Weight_kg']
            lower_qty0 = self.data.iloc[lower_list[0],:]['Qty']
            lower_qty1 = self.data.iloc[lower_list[1],:]['Qty']
            lower_qty2 = self.data.iloc[lower_list[2],:]['Qty']
            if weight_level < (weight_lower0*lower_qty0 + weight_lower1*lower_qty1+
                               weight_lower2*lower_qty2):
                self.df.loc[PN,:]['MACHINING']=1
        elif self.data[self.data['Part']==PN].iloc[0,:]['Single_ASSY'] =='Single':        
            if len(self.data[self.data['Part']==PN].iloc[0,:]['OEM_VENDOR'])<5:
                if len(self.data[self.data['Part']==PN].iloc[0,:]['COMPONENTS'])<5:
                    if len(self.data[self.data['Part']==PN].iloc[0,:]['Engineering Specs'])<5:
                        if self.df.loc[PN,:]['OEM_METAL']==0 and self.df.loc[PN,:]['OEM_NONMETAL']==0:
                            self.df.loc[PN,:]['MACHINING']=1
                            self.df.loc[PN,:]['RAW_NONMETAL']=1
    def cladding(self,PN):
        term =('OVERLAY','INLAY','CLADDING')
        words = self.word.split(self.data[self.data['Part']==PN].iloc[0,:]['Part Description'])
        if len(self.data[self.data['Part']==PN].iloc[0,:]['OEM_VENDOR'])<5:
            if len(self.data[self.data['Part']==PN].iloc[0,:]['COMPONENTS'])>5:
                for j in term:
                    if j in words:
                        level = self.data[self.data['Part']==PN].iloc[0,:]['Level']
                        if 'A' in level:
                            level = level[0]
                        else:
                            pass
                        lower_level = str(int(level)+1)
                        idx = list(self.data[self.data['Part']==PN].index)
                        level_df = self.data.iloc[idx[0]:,:]
                        level_list = list(level_df[level_df['Level']==level].index)
                        weight_level = self.data[self.data['Part']==PN].iloc[0,:]['Weight_kg']
                        if len(level_list)==1:
                            lower_list = list(level_df[level_df['Level']==lower_level].index)
                            if len(lower_list)==1:
                                weight_lower = level_df.iloc[lower_list[0],:]['Weight_kg']
                                lower_qty = level_df.iloc[lower_list[0],:]['Qty']
                                if weight_level >= weight_lower*lower_qty:
                                    self.df.loc[PN,:]['CLADDING']=1
                            elif len(lower_list)==2:
                                weight_lower0 = level_df.iloc[lower_list[0],:]['Weight_kg']
                                weight_lower1 = level_df.iloc[lower_list[1],:]['Weight_kg']
                                lower_qty0 = level_df.iloc[lower_list[0],:]['Qty']
                                lower_qty1 = level_df.iloc[lower_list[1],:]['Qty']
                                if weight_level >= (weight_lower0*lower_qty0 + weight_lower1*lower_qty1):
                                    self.df.loc[PN,:]['CLADDING']=1
                            elif len(lower_list)==3:
                                weight_lower0 = level_df.iloc[lower_list[0],:]['Weight_kg']
                                weight_lower1 = level_df.iloc[lower_list[1],:]['Weight_kg']
                                weight_lower2 = level_df.iloc[lower_list[2],:]['Weight_kg']
                                lower_qty0 = level_df.iloc[lower_list[0],:]['Qty']
                                lower_qty1 = level_df.iloc[lower_list[1],:]['Qty']
                                lower_qty2 = level_df.iloc[lower_list[2],:]['Qty']
                                if weight_level > (weight_lower0*lower_qty0 + weight_lower1*lower_qty1+
                                                   weight_lower2*lower_qty2):
                                    self.df.loc[PN,:]['CLADDING']=1
                        elif len(level_list)>=2:
                            lower_df2 = self.data.iloc[level_list[0]:level_list[1],:]
                            lower_list2 = list(lower_df2[lower_df2['Level']==lower_level].index)
                            if len(lower_list2)==1:
                                weight_lower = self.data.iloc[lower_list2[0],:]['Weight_kg']
                                lower_qty = self.data.iloc[lower_list2[0],:]['Qty']
                                if weight_level >= weight_lower*lower_qty:
                                    self.df.loc[PN,:]['CLADDING']=1
                            elif len(lower_list2)==2:
                                weight_lower0 = self.data.iloc[lower_list2[0],:]['Weight_kg']
                                weight_lower1 = self.data.iloc[lower_list2[1],:]['Weight_kg']
                                lower_qty0 = self.data.iloc[lower_list2[0],:]['Qty']
                                lower_qty1 = self.data.iloc[lower_list2[1],:]['Qty']
                                if weight_level >= (weight_lower0*lower_qty0 + weight_lower1*lower_qty1):
                                    self.df.loc[PN,:]['CLADDING']=1
                            elif len(lower_list2)==3:
                                weight_lower0 = self.data.iloc[lower_list2[0],:]['Weight_kg']
                                weight_lower1 = self.data.iloc[lower_list2[1],:]['Weight_kg']
                                weight_lower2 = self.data.iloc[lower_list2[2],:]['Weight_kg']
                                lower_qty0 = self.data.iloc[lower_list2[0],:]['Qty']
                                lower_qty1 = self.data.iloc[lower_list2[1],:]['Qty']
                                lower_qty2 = self.data.iloc[lower_list2[2],:]['Qty']
                                if weight_level > (weight_lower0*lower_qty0 + weight_lower1*lower_qty1+
                                                   weight_lower2*lower_qty2):
                                    self.df.loc[PN,:]['CLADDING']=1                                                
    def fabrication(self,PN):
        term= ('WELDMENT','FRAME',)
        Q_spec = ('Q00070','Q00075','Q00083','Q00825')
        w_spec =('W99000','W99101')
        m_spec =('M10','M11','M12')
        desc_words = self.word.split(self.data[self.data['Part']==PN].iloc[0,:]['Part Description'])
        eng_words = self.word.split(self.data[self.data['Part']==PN].iloc[0,:]['Engineering Specs'])
        Q_words = self.word.split(self.data[self.data['Part']==PN].iloc[0,:]['Quality Specs'])
        level = self.data[self.data['Part']==PN].iloc[0,:]['Level']
        if 'A' in level:
            level = level[0]
        else:
            pass
        lower_level = str(int(level)+1)
        up_level = str(int(level)-1)
        idx =list(self.data[self.data['Part']==PN].index)
        level_df =self.data.iloc[idx[0]:,:]
        level_list = list(level_df[level_df['Level']==level].index)
        # slice below this PN all items
        if len(level_list)==1:
            lower_df = self.data.iloc[level_list[0]:,:]
        elif len(level_list)>=2:
            lower_df =self.data.iloc[level_list[0]:level_list[1],:]
        up_list = list(lower_df[lower_df['Level']==up_level].index)
        if len(up_list)==0:
            pass
        elif len(up_list)>=1:
            lower_df=self.data.iloc[idx[0]:up_list[0],:]
        lower_list = list(lower_df[lower_df['Level']==lower_level].index)
        # weight_level = self.data[self.data['Part']==PN].iloc[0,:]['Weight_kg']
        for i in term:
            if i in desc_words:
                if len(lower_list)<=1:
                    if self.df.loc[PN,:]['OEM_METAL']==0 and self.df.loc[PN,:]['OEM_NONMETAL']==0:
                        self.df.loc[PN,:]['FABRICATION']=1
                        self.df.loc[PN,:]['RAW_METAL']=1
                elif len(lower_list)>=2:
                    if self.df.loc[PN,:]['OEM_METAL']==0 and self.df.loc[PN,:]['OEM_NONMETAL']==0:
                        self.df.loc[PN,:]['FABRICATION']=1
        for j in  Q_spec:
            if j in Q_words:
                if len(lower_list)<=1:
                    if self.df.loc[PN,:]['OEM_METAL']==0 and self.df.loc[PN,:]['OEM_NONMETAL']==0:
                        self.df.loc[PN,:]['FABRICATION']=1
                        self.df.loc[PN,:]['RAW_METAL']=1
                elif len(lower_list)>=2:
                    if self.df.loc[PN,:]['OEM_METAL']==0 and self.df.loc[PN,:]['OEM_NONMETAL']==0:
                        self.df.loc[PN,:]['FABRICATION']=1
        for k in w_spec:
            if k in eng_words:
                if len(lower_list)<=1:
                    if self.df.loc[PN,:]['OEM_METAL']==0 and self.df.loc[PN,:]['OEM_NONMETAL']==0:
                        self.df.loc[PN,:]['FABRICATION']=1
                        self.df.loc[PN,:]['RAW_METAL']=1
                elif len(lower_list)>=2:
                    if self.df.loc[PN,:]['OEM_METAL']==0 and self.df.loc[PN,:]['OEM_NONMETAL']==0:
                        self.df.loc[PN,:]['FABRICATION']=1
        for l in m_spec:
            for m in eng_words:
                if l in m:
                    if len(lower_list)<=1:
                        if self.df.loc[PN,:]['OEM_METAL']==0 and self.df.loc[PN,:]['OEM_NONMETAL']==0:
                            self.df.loc[PN,:]['FABRICATION']=1
                            self.df.loc[PN,:]['RAW_METAL']=1
                    elif len(lower_list)>=2:
                        if self.df.loc[PN,:]['OEM_METAL']==0 and self.df.loc[PN,:]['OEM_NONMETAL']==0:
                            self.df.loc[PN,:]['FABRICATION']=1
    def proccesSet(self):
        for i in self.df.index:
            # self.Series= self.data[self.data['Part']==i].iloc[0,:]
            self.chemical(i)
            self.assy(i)
            self.casting(i)
            self.coating(i)
            self.forging(i)
            self.cladding(i)
            self.OEM(i)
            self.machining(i)
            self.fabrication(i)
        return self.df
Prs = ProcessIdentify(data_rawlv)
df_Prs =Prs.proccesSet()
# print(df_Prs.head(2),f'initial df shape is {df_Prs.shape}')
# df_Prs.to_excel('data/P4000083572W01_process.xlsx')# write clear data to local file
# df_pncty.to_excel('data/P1000220410_vendor.xlsx',index=False)# write clear data to local file

# df_concat = Twodfconcat.combine(df_Prs, df_0,index=True,start=10)

# df_concat.to_excel('data/P4000083572W01_pr&vr.xlsx')# write clear data to local file
df_complete= Twodfconcat.combine(data_rawlv,df_Prs,start=0)
df_complete.columns
# df_complete.to_excel('data/P1000220410_complete_test.xlsx',index=False)

class RawMaterial:
    def rawNetMass(self,data):
        data.insert(loc= 33,column='Raw_Net_Mass',value=0)
        for i,j in enumerate(data['Part']):
            if data.loc[i,'RAW_METAL']==1:
                data.loc[i,'Raw_Net_Mass']= data.loc[i,'Weight_kg']
            elif data.loc[i,'RAW_NONMETAL']==1:
                data.loc[i,'Raw_Net_Mass']= data.loc[i,'Weight_kg']
    def raW_Uti(self,data):
        data.insert(loc=34,column='Raw_Uti',value=1)
        for i,j in enumerate(data['Part']):
            if data.loc[i,'FORGING']==1:
                if data.loc[i,'Model']=='NO-DWG':
                    if data.loc[i,'Weight_kg'] * data.loc[i,'Qty']<100:
                        data.loc[i,'Raw_Uti']=0.7
                    elif data.loc[i,'Weight_kg'] * data.loc[i,'Qty'] >=100:
                        data.loc[i,'Raw_Uti']=0.65
                elif data.loc[i,'Model']!='NO-DWG':
                    if data.loc[i,'Weight_kg']<100:
                        data.loc[i,'Raw_Uti']=0.6
                    elif data.loc[i,'Weight_kg']>=100 and data.loc[i,'Weight_kg']<500:
                        data.loc[i,'Raw_Uti']=0.5
                    elif data.loc[i,'Weight_kg']>500:
                        data.loc[i,'Raw_Uti']=0.45
            elif data.loc[i,'FORGING']==0 and data.loc[i,'RAW_METAL']==1:
                data.loc[i,'Raw_Uti']=0.8
            elif data.loc[i,'FORGING']==0 and data.loc[i,'RAW_NONMETAL']==1:   
                data.loc[i,'Raw_Uti']=0.4
    def setUp(self,data):
        self.rawNetMass(data)
        self.raW_Uti(data)
        return data
    
Raw = RawMaterial()
df_final =Raw.setUp(df_complete)
df_final.shape
df_final.columns

# df_final.to_excel('data/P7000094258-26_final.xlsx',index=False)

# =============================================================================
# country_number = list(set(df_externalcost['VENDOR_COUNTRY']))
# country_number.remove('nan')
# process = ['ASSY','FORGING', 'MACHINING', 'OEM_METAL', 'OEM_NONMETAL', 'CASTING',
#        'CHEMICAL', 'CLADDING', 'FABRICATION', 'COATING']
# arr = np.around(np.random.uniform(low=.5,high =6,size=(len(country_number),10)),2)
# EF_table = pd.DataFrame(data= arr,index=country_number,columns=process)
# ASSY = np.around(np.random.uniform(0.3,1,size =29),2)
# Coating = np.around(np.random.uniform(0.2,.5,29),2)
# EF_table['ASSY']=ASSY
# EF_table['COATING']=Coating
# EF_table.insert(loc=10,value= 4.018,column='RAW_METAL')
# EF_table.insert(loc=10,value= 3.116,column='RAW_NONMETAL')
# EF_table.to_excel('data/EF.xlsx')
# EF_table.columns
# 
# =============================================================================

class ProcesEF:
    @staticmethod
    def procesEF(data,EFe):
        '''EFe is dictionary'''
        for i,j in EFe.items():
            if i =='Country':
                data[i]='CN'
            else:
                data[i+'_Mass']=0
        for h in data.index:
            for k,l in EFe.items():
                if data.loc[h,k]==1:
                    col = k+'_Mass'
                    data.loc[h,col]=l
        return data 

Final_df = ProcesEF.procesEF(df_final, EFe)
# Final_df.to_excel('data/fi_df.xlsx',index =False)
Final_df.columns[-12:]


def emissionSum(data):
    cols = data.columns[-12:]
    data['Pr_sum']=0
    data['Raw_sum_metal']= data['Raw_Net_Mass']/data['Raw_Uti']*data['RAW_METAL_Mass']*data['Sub_total_qty']
    data['Raw_sum_non'] = data['Raw_Net_Mass']/data['Raw_Uti']*data['RAW_NONMETAL_Mass']*data['Sub_total_qty']
    for i in cols[0:10]:
        data['Pr_sum']+=data[i]
    data['Pr_total'] = data['Pr_sum']*data['Weight_kg']*data['Sub_total_qty']
    data['Raw_Total']=data['Raw_sum_metal']+data['Raw_sum_non']
    data['CO2_total_Mass'] = data['Raw_Total'] + data['Pr_total']
    total =sum(data['CO2_total_Mass'])
    EF = total/data['Weight_kg'][0]
    return data, total, EF

data,total, EF = emissionSum(Final_df)
data.to_excel('data/fi_df_P7000073756.xlsx',index =False)
print(f'total emission is {total:.0f} kg')
print(f' emission factor is {EF:.2f} kg/kg')


# if __name__== '__main__':
#     PN = 'P7000094258-26'
#     data, EFe= LoadingData.LoadingData(PN)
#     # read data from loacal iBOM and emission factor table
#     EFe = GetEF.getEF(EFe)
#     # create EFe dict ignore the region currently
#     data0 =DataClear.dataClear(data)
#     # clear the iBOM data drop nan value
#     df_toplevelpn =DFClear.dfClear(df_toplevelpn)
#     # clear data convert each of column data into str type
#     test_data = DataPrepare()
#     data_rawlv=test_data.getData(data0,df_toplevelpn)
#     # create subtotal and weight_kg column
#     Prs = ProcessIdentify(data_rawlv)
#     df_Prs =Prs.proccesSet()
#     # identify each of PN itself mfg process 
#     df_complete= Twodfconcat.combine(data_rawlv,df_Prs,start=0)
#     # concatenate iBOM and process data
#     Raw = RawMaterial()
#     df_final =Raw.setUp(df_complete)
#     data = ProcesEF.procesEF(df_final, EFe)
    
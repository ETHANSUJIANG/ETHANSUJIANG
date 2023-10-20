# -*- coding: utf-8 -*-
"""
Created on Mon May 29 09:52:19 2023

@author: zhusj
"""

import pandas as pd
import numpy as  np
import matplotlib.pyplot as plt
from matplotlib.patches import ConnectionPatch
import os 
import sys
import time 
import snowflake.connector
import re


# from functools import wraps
# =============================================================================
# method1 
# os.walk #deeply go through path, dir, files
# os.getcwd() #get the current directory
# os.listdir #get curret folder or file name 
# os.path.isdir #tell this path is folder or not 
# os.path.isfile #tell this path is file or not
# os.mkdir #create new folder
# os. makedir #create deep folder
# os.remove #only delete file
# od.rmdir #only delete empty folder
# os.path.join #join two paths as new 
# =============================================================================

# =============================================================================
# #method 2
# # root_path = os.path.abspath(os.path.dirname(current_directory) + os.path.sep + ".")
# # sys.path.append(root_path)
# #print(sys.path[0])
# 
# #method 3
# currentPath = os.getcwd()#.replace('\\','/')# get current path
# print(currentPath)
# os.path.dirname(currentPath)
# print(currentPath)
# os.listdir(os.getcwd())   # get current path and document list
# os.path.isdir('data1')
# sys.path # get the system enviroment path
# 
# =============================================================================
# =============================================================================
# a ='GUIDE FUNNEL WELDMENT, 10K/15K TUBING HEAD, TITUS VI CONNECTOR,\
#      F/ LDB SEALING APPLICATION, 27 INCH OR 30 INCH WHD, SUBSEA 2.0'
# # df1.replace(u'\xa0\xa0\xa0\xa0','',inplace=True)
# # arr = np.around(np.random.uniform(low=.5,high =8,size=15).reshape(5,-1),2)
# # Process = ['ASSY','FORGING', 'MACHINING', 'OEM_METAL', 'OEM_NONMETAL', 'CASTING',\
# #         'CHEMICAL', 'CLADDING', 'FABRICATION', 'COATING']
# # Country = ['CZ','FR','AE','NG','AR','MY','US','BR','AZ','SA','IN','CN','NO','PL' \
# #            ,'ES','NL','KR','SI','AU','BE','CO','CA','IT','GB','SE','AO','DE','MX','SG']
# # arr = np.around(np.random.uniform(low=.5,high =6,size=(len(Country),len(Process))),2)

# a ='name,456852,STTUR,0710-2850273,ATRM12100,M00022,00123,M20735,M40755'
# txt ='M40405 (ALT)/I M40400/I C81007/F P60136/B'
# print(re.findall(r'M[1-4]\d{4}',txt))
# print(re.findall(r'[0-4][5-9]', txt))
# print(re.findall(r'0\d{3,4}-\d{7,8}|1\d{10}',a))
# print(re.findall(r'\w{4}', a))
# print(re.findall(r'[4-5]\d{3}', a))
# print(re.findall(r'M[1-4]\d{4}', a))
# print(re.findall(r'^[,s]M[1-4]\d{4}', a))
# ls=re.findall(r'[C]\d{5}', txt)

### make figure and assign axis objects
'''
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(9, 5))
fig.subplots_adjust(wspace=0)

# pie chart parameters
overall_ratios = [.27, .56, .17]
labels = ['Approve', 'Disapprove', 'Undecided']
explode = [0.1, 0, 0]
# rotate so that first wedge is split by the x-axis
angle = -180 * overall_ratios[0]
wedges, *_ = ax1.pie(overall_ratios, autopct='%1.1f%%', startangle=angle,
                     labels=labels, explode=explode)

# bar chart parameters
age_ratios = [.33, .54, .07, .06]
age_labels = ['Under 35', '35-49', '50-65', 'Over 65']
bottom = 1
width = .2

# Adding from the top matches the legend.
for j, (height, label) in enumerate(reversed([*zip(age_ratios, age_labels)])):
    bottom -= height
    bc = ax2.bar(0, height, width, bottom=bottom, color='C0', label=label,
                 alpha=0.1 + 0.25 * j)
    ax2.bar_label(bc, labels=[f"{height:.0%}"], label_type='center')

ax2.set_title('Age of approvers')
ax2.legend()
ax2.axis('off')
ax2.set_xlim(- 2.5 * width, 2.5 * width)

# use ConnectionPatch to draw lines between the two plots
theta1, theta2 = wedges[0].theta1, wedges[0].theta2
center, r = wedges[0].center, wedges[0].r
bar_height = sum(age_ratios)

# draw top connecting line
x = r * np.cos(np.pi / 180 * theta2) + center[0]
y = r * np.sin(np.pi / 180 * theta2) + center[1]
con = ConnectionPatch(xyA=(-width / 2, bar_height), coordsA=ax2.transData,
                      xyB=(x, y), coordsB=ax1.transData)
con.set_color([0, 0, 0])
con.set_linewidth(4)
ax2.add_artist(con)
# draw bottom connecting line
x = r * np.cos(np.pi / 180 * theta1) + center[0]
y = r * np.sin(np.pi / 180 * theta1) + center[1]
con = ConnectionPatch(xyA=(-width / 2, 0), coordsA=ax2.transData,
                      xyB=(x, y), coordsB=ax1.transData)
con.set_color([0, 0, 0])
ax2.add_artist(con)
con.set_linewidth(4)

plt.show()
'''



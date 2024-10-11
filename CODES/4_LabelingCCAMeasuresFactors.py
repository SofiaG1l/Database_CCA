# -*- coding: utf-8 -*-
"""
Created on Fri Aug  2 10:26:06 2024

@author: sgilclavel
"""


import json as json
import pandas as pd
import regex as re

DATA_org=pd.read_excel("C:\\Dropbox\\PhD_MaxPlanck\\FamilyTies\\FromHydra\\Family Ties Processed NonMig\\LIWC_Processed_en\\R&R_20230915.xlsx")

EXTRA="germany"
DATA2=DATA_org[DATA_org.apply(lambda x: x.family>0 and len(re.findall(EXTRA,x.COUNTRY_TWEET))>0, axis=1)]

DATA2=DATA2.groupby(['COUNTRY_TWEET','USER_ID_1']).sample(frac=0.2)

TEXT={}

n=DATA2.shape[0]
for i in range(n):
    if i==0:
        with open("C:\\Dropbox\\PhD_MaxPlanck\\FamilyTies\\RandR_CODES\\PROCESSED\\CHECK2_extra_DE.json", 'w') as fp:
            fp.write('[\n')
            json.dump({"text":DATA2["Fam_Eng_Text"].iloc[0],
                       "meta":DATA2[["Column1","X","USER_ID","USER","USER_ID_1"]].iloc[0].to_dict()}, fp)
            fp.write(',\n')
    else:
        if i<(n-1):
            with open("C:\\Dropbox\\PhD_MaxPlanck\\FamilyTies\\RandR_CODES\\PROCESSED\\CHECK2_extra_DE.json", 'a') as fp:
                json.dump({"text":DATA2["Fam_Eng_Text"].iloc[i],
                           "meta":DATA2[["Column1","X","USER_ID","USER","USER_ID_1"]].iloc[i].to_dict()}, fp)
                fp.write(',\n')
        else:
            with open("C:\\Dropbox\\PhD_MaxPlanck\\FamilyTies\\RandR_CODES\\PROCESSED\\CHECK2_extra_DE.json", 'a') as fp:
                json.dump({"text":DATA2["Fam_Eng_Text"].iloc[i],
                           "meta":DATA2[["Column1","X","USER_ID","USER","USER_ID_1"]].iloc[i].to_dict()}, fp)
                fp.write('\n')
                fp.write(']')


with open("C:\\Dropbox\\PhD_MaxPlanck\\FamilyTies\\RandR_CODES\\PROCESSED\\CHECK.json") as json_data:
    d = json.load(json_data)
    print(d)
    
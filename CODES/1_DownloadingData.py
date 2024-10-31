# -*- coding: utf-8 -*-

"""
##################################
# 
# Author: Dr. Sofia Gil-Clavel
# 
# Last update: October 31st, 2024.
# 
# Description: Script to download the data using Scopus, ELSEVIER. As explained in:
#   - Gil-Clavel, S., Wagenblast, T., Akkerman, J., & Filatova, T. (2024, April 26). 
#       Patterns in Reported Adaptation Constraints: Insights from Peer-Reviewed 
#       Literature on Flood and Sea-Level Rise. https://doi.org/10.31235/osf.io/3cqvn
#   - Gil-Clavel, S., Wagenblast, T., & Filatova, T. (2023, November 24). Incremental
#       and Transformational Climate Change Adaptation Factors in Agriculture Worldwide:
#       A Natural Language Processing Comparative Analysis. 
#       https://doi.org/10.31235/osf.io/3dp5e
# 
# Computer Environment:
#   - Windows 
#   - Microsoft Windows 10 Enterprise
#   - Python 3.11
# 
# Conda Environment to run the code:
#   - @SofiaG1L/NLP4LitRev/PY_ENVIRONMENT/elsapy_0.yml
#
##################################
"""

from elsapy.elsclient import ElsClient
from elsapy.elsprofile import ElsAuthor, ElsAffil
from elsapy.elsdoc import FullDoc, AbsDoc
from elsapy.elssearch import ElsSearch
import json
import os
import pandas as pd
import pickle
import time 
from tqdm import tqdm

## To access in Browser
# https://api.elsevier.com/content/search/scopus?query=af-id(60032114)+OR+af-id(60022265)&apiKey=b8f9c2ba03de983ecaf67c49f9bbeb21

# To increase quota:
# https://dev.elsevier.com/api_key_settings.html

# Parameters:
# https://dev.elsevier.com/documentation/ScopusSearchAPI.wadl

# Searching Query:
# https://dev.elsevier.com/sc_search_tips.html

## Load configuration
con_file = open("config.json") 
config = json.load(con_file)
con_file.close()

## Initialize client
client = ElsClient(config['apikey'])
client.inst_token = config['insttoken']

## The searching areas and terms
Subj_Area=["ARTS","ENVI","SOCI","MULT"]

Search_Terms={
    "FIRST":{
        "A":{
            "SEARCH":["TITLE-ABS-KEY(social change)",
                    "TITLE-ABS-KEY(structural change AND [society OR social])",
                    "TITLE-ABS-KEY(transition* AND [society OR social])",
                    "TITLE-ABS-KEY(transformation* AND [society OR social])",
                    "TITLE-ABS-KEY(transformative AND [society OR social])",
                    "TITLE-ABS-KEY(revolution* AND [society OR social])",
                    "TITLE-ABS-KEY(regime shift AND [society OR social])",
                    "TITLE-ABS-KEY(tipping point AND [society OR social])",
                    "TITLE-ABS-KEY(tipping element* AND [society OR social])",
                    "TITLE-ABS-KEY(critical transition AND [society OR social])",
                    "TITLE-ABS-KEY(threshold AND [social OR econom* OR behavio* OR policy OR political])",
                    "TITLE-ABS-KEY(state shift)",
                    "TITLE-ABS-KEY(cascad* OR domino OR chain reaction)",
                    "TITLE-ABS-KEY(relocation OR retreat OR buyout* OR buy out)"]}},
    "SECOND":{
        "A":{
            "CALL":("FIRST","A"),
            "FIELD":Subj_Area,
            "SEARCH":["TITLE-ABS-KEY(flood*)",
                      "TITLE-ABS-KEY(deluge)",
                    "TITLE-ABS-KEY(storm)",
                    "TITLE-ABS-KEY(hurricane)",
                    "TITLE-ABS-KEY(typhoon)",
                    "TITLE-ABS-KEY(heavy rainfall)",
                    "TITLE-ABS-KEY(sea level rise)",
                    "TITLE-ABS-KEY(heavy precipitation)",
                    "TITLE-ABS-KEY(delta)",
                    "TITLE-ABS-KEY(small island)",
                    "TITLE-ABS-KEY(coast*)",
                    "TITLE-ABS-KEY(delta)",
                    "TITLE-ABS-KEY(dike)",
                    "TITLE-ABS-KEY(levee)",
                    "TITLE-ABS-KEY(climate OR climate change)"]},
        "B":{
            "CALL":("FIRST","A"),
            "FIELD":Subj_Area,
            "SEARCH":["TITLE-ABS-KEY(earthquake)",
            "TITLE-ABS-KEY(flood)",
            "TITLE-ABS-KEY(landslide)",
            "TITLE-ABS-KEY(wildfire)",
            "TITLE-ABS-KEY(drought)"]}
        },
    "THIRD":{
        "A":{
            "CALL":("SECOND","A"),
            "FIELD":["SOCI"],
            "SEARCH":["TITLE-ABS-KEY(multiple streams)",
                      "TITLE-ABS-KEY(advocacy coalition*)",
                    "TITLE-ABS-KEY(ecology of games)"]},
        "B":{
            "CALL":("THIRD","A"),
            "FIELD":Subj_Area,
            "SEARCH":["TITLE-ABS-KEY((Room for the River) OR (Room for River) OR (Space for River))",
            "TITLE-ABS-KEY(Delta*)",
            "TITLE-ABS-KEY(Afsluitdijk)",
            "TITLE-ABS-KEY(Houston OR (Houston AND Galveston))"]}
        },
    }

# =============================================================================
# Scopus
# =============================================================================

## Saving the "prism:url" to not repeat those entrances
PRISM_URL=set()

DIR="@SofiaG1l/Database_CCA/PROCESSED/SCOPUS_DATA/"

## Loop to retrieve the data
for k,v in Search_Terms.items():
    print(f'k: {k} and v: {v}')
    # break
    if k.find("FIRST")<0:
        # break
        for k2,v2 in v.items():
            print(f'k2: {k2} and v2: {v2}')    
            # break
            for SA in v2['FIELD']:
                print(f'SA: {SA}')    
                # break
                newpath = SA+"\\"
                if not os.path.exists(DIR+newpath):
                    # print(DIR+newpath)
                    os.makedirs(DIR+newpath)
                
                for ST in v2['SEARCH']: ## Here is the change!
                    print(f'ST: {ST}')    
                    # break
                    for ST2 in Search_Terms[v2['CALL'][0]][v2['CALL'][1]]['SEARCH']:
                        print(f'ST2: {ST2}')
                        # break
                        ## Looking for the articles based on Title, Abstract, and Key Words:
                        query=ST+" AND "+ST2+" AND SUBJAREA("+SA+")"
                        doc_srch = ElsSearch(query,'scopus')
                        doc_srch.execute(client, get_all = True)
                        
                        # Checking that all the columns are in the retrieved data frame
                        if len(doc_srch.results)>1:
                            
                            # print(doc_srch.query+":\nhas", len(doc_srch.results), "results.")
                            
                            DATA1=pd.json_normalize(doc_srch.results)
                            DATA1["query"]=doc_srch.query
                            NotIn_PRISM_URL=list(set(DATA1["prism:url"]).difference(PRISM_URL))
                            
                            PRISM_URL=PRISM_URL.union(set(DATA1["prism:url"]))
                            
                            if len(NotIn_PRISM_URL)>0:
                               
                                DATA1=DATA1[DATA1["prism:url"].apply(lambda x: x in NotIn_PRISM_URL)]
                                DATA1=DATA1.reset_index(drop=True)
                                print("There are: "+str(DATA1.shape[0])+" new articles.\n")
                                
                                DESCRIPTION=[]
                                
                                # Looking for the Abstract
                                for de in tqdm(DATA1["prism:url"]):
                                    # break
                                    doi_doc = AbsDoc(de)
                                    doi_doc.read(client)
                                    
                                    if doi_doc.client.req_status['status_code']!=200:
                                        print(doi_doc.client.req_status)
                                    
                                    while doi_doc.client.req_status['status_code']==429:
                                        print(doi_doc.client.req_status)
                                        time.sleep(60*60)
                                        
                                        doi_doc = AbsDoc(de)
                                        doi_doc.read(client)
                                    
                                    try:
                                        DESCRIPTION.append(doi_doc.data["coredata"]['dc:description'])
                                    except:
                                        DESCRIPTION.append("")
                                    
                                DATA1["description"]=DESCRIPTION
                                
                                # Saving the dataframe generated
                                if os.path.exists(DIR+newpath+"doc_srch.pickle"):
                                    C=len(os.listdir(DIR+newpath))
                                    # with open(DIR+newpath+f"doc_srch_{C-1}.pickle", 'rb') as handle:
                                    #     DATA = pickle.load(handle)
                                    # DATA1=pd.concat([DATA,DATA1])
                                    with open(DIR+newpath+f"doc_srch_{C}.pickle", 'wb') as handle:
                                        pickle.dump(DATA1, handle, protocol=pickle.HIGHEST_PROTOCOL)
                                else:
                                    with open(DIR+newpath+"doc_srch.pickle", 'wb') as handle:
                                        pickle.dump(DATA1, handle, protocol=pickle.HIGHEST_PROTOCOL)
                            


# =============================================================================
# ## Joining all the data by field
# =============================================================================

for ff in ['ARTS', 'ENVI', 'SOCI', 'MULT']:
    TOTAL=len(os.listdir(DIR+ff))
    for ii in tqdm(range(TOTAL)):
        if ii==0:
            with open(DIR+ff+"\\doc_srch.pickle", 'rb') as handle:
                DT1 = pickle.load(handle)
        else:
            with open(DIR+ff+f"\\doc_srch_{ii}.pickle", 'rb') as handle:
                DT0 = pickle.load(handle)
            DT1=pd.concat([DT0,DT1])
            
    with open(DIR+ff+f"\\doc_srch_{ff}_all.pickle", 'wb') as handle:
        pickle.dump(DT1, handle, protocol=pickle.HIGHEST_PROTOCOL)

# =============================================================================
# ## Joining all the data
# =============================================================================

for ff in ['ARTS', 'ENVI', 'SOCI', 'MULT']:
    if ff=='ARTS':
        with open(DIR+ff+f"\\doc_srch_{ff}_all.pickle", 'rb') as handle:
            DT1 = pickle.load(handle)
    else:
        with open(DIR+ff+f"\\doc_srch_{ff}_all.pickle", 'rb') as handle:
            DT0 = pickle.load(handle)
        DT1=pd.concat([DT0,DT1])

# DT1.drop_duplicates(ignore_index=True)
DT1=DT1.reset_index(drop=True)

### Removing duplicates. It is not possible to use drop_duplicates

EXIST=[]
TO_RMV=[]

for cc, ii in tqdm(enumerate(DT1["prism:url"])):
    if ii not in EXIST:
        EXIST.append(ii)
    else:
        TO_RMV.append(cc)
        
### Checking consistency

DT1=DT1.drop(TO_RMV)
DT1=DT1.reset_index(drop=True)

[cc for cc,x in enumerate(DT1["prism:url"]) if x not in PRISM_URL]

### Saving all together

with open(DIR+"doc_srch_all.pickle", 'wb') as handle:
    pickle.dump(DT1, handle, protocol=pickle.HIGHEST_PROTOCOL)







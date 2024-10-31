# -*- coding: utf-8 -*-

"""
##################################
# 
# Author: Dr. Sofia Gil-Clavel
# 
# Last update: October 31st, 2024.
# 
# Description: Script to clean the text, as explained in:
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
#   - @SofiaG1L/Database_CCA/PY_ENVIRONMENT/pytorch_textacy.yml
#
##################################
"""

import os as os
import subprocess

import pickle

# Data Handling
import pandas as pd
from tqdm import tqdm
tqdm.pandas(desc="my bar!")
pd.set_option('display.max_columns', 500) # Display any number of columns
pd.set_option('display.max_rows',500) # Display any number of rows

# Basic Libraries
import numpy as np
from collections import Counter

# Processing text
import regex as re

# Import Text Processing Libraries
import textacy.preprocessing as tprep
# from textacy.extract import keyword_in_context as KWIC
import spacy as spacy


# =============================================================================
# Functions
# =============================================================================
os.chdir("@SofiaG1L/NLP4LitRev//MainFunctions/")
import Functions as FN

# Function to remove unspected characters and strings
def normalize(text):
    text=tprep.normalize.hyphenated_words(text)
    text=tprep.normalize.quotation_marks(text)
    text=tprep.normalize.unicode(text)
    text=tprep.remove.accents(text)
    return(text)


# =============================================================================
# Cleaning data to merge the metadata with the files info
# =============================================================================
#### This is the code for Data <=August 2022 ####
DIRF="@SofiaG1l/Database_CCA/PROCESSED/SCOPUS_DATA/ASREVIEW_CSV"
FOLDERS1=os.listdir(DIRF)
FOLDERS1=[x for x in FOLDERS1 if x.find("csv")>-1]

# SCOPUS=pd.read_csv("C:\Dropbox\TU_Delft\Projects\ML_FindingsGrammar\DATA\Articles_Supervised.csv")
SCOPUS1=pd.read_csv(DIRF+"\\"+FOLDERS1[0])
for x in FOLDERS1[1:]:
    SCOPUS1=pd.concat([SCOPUS1,
              pd.read_csv(DIRF+"\\"+x)])
SCOPUS1=SCOPUS1[pd.isna(SCOPUS1.FILE_NAME)==False]
SCOPUS1=SCOPUS1.reset_index(drop=True)
SCOPUS1.head()
SCOPUS1=SCOPUS1.reset_index(drop=True)
SCOPUS1=SCOPUS1[['clusters2','dc:identifier','doi','FILE_NAME']]

with open('@SofiaG1l/Database_CCA/PROCESSED/SCOPUS_DATA/4_SCOPUSwARTICLES/SCOPUSwArt_August2022.pickle', 'rb') as handle:
    DT1_org1 = pickle.load(handle)
DT1_org1=pd.merge(DT1_org1,SCOPUS1, left_on=['dc:identifier', 'prism:doi'], right_on=['dc:identifier', 'doi'])
DT1_org1['period']="<Aug,2022"

#### This is the code for Data August 2022 - January 2024 ####
SCOPUS2=pd.read_csv("@SofiaG1l/Database_CCA/PROCESSED/SCOPUS_DATA/asreview_dataset_relevant_UpdatedDatabaseJan2024.csv",
                   encoding="utf-8")
SCOPUS2=SCOPUS2[pd.isna(SCOPUS2.FILE_NAME)==False]
SCOPUS2=SCOPUS2.reset_index(drop=True)
SCOPUS2=SCOPUS2[['dc:identifier','doi','FILE_NAME']]
SCOPUS2['clusters2']=None

with open('@SofiaG1l/Database_CCA/PROCESSED/SCOPUS_DATA/SCOPUSwArt_Aug2022_Feb2024.pickle', 'rb') as handle:
    DT1_org2 = pickle.load(handle)
DT1_org2=pd.merge(DT1_org2,SCOPUS2, left_on=['dc:identifier', 'prism:doi'], right_on=['dc:identifier', 'doi'])
DT1_org2['period']="Aug,2022-Feb,2024"

## Both in one database
DT1_org=pd.concat([DT1_org1,DT1_org2],ignore_index=False)

DT1_org1.index=DT1_org1.apply(lambda x: str(x['clusters2'])+"_"+str(x['FILE_NAME']),axis=1)
DT1_org2.index=DT1_org2['FILE_NAME']

# =============================================================================
# Openning and cleaning the text
# =============================================================================

### Data < August 2022
DIR1="@SofiaG1l/Database_CCA/PROCESSED/SCOPUS_DATA/PDFs_Clusters/TXT/" # !!!

### Data August 2022 - January 2024
DIR2="@SofiaG1l/Database_CCA/PROCESSED/SCOPUS_DATA/TXT/" # !!!


### Data < August 2022
FOLDERS1=os.listdir(DIR1)
FOLDERS1=[DIR1+x for x in FOLDERS1 if x.find("Cluster")>-1]

### Data August 2022 - January 2024
FOLDERS2=[DIR2+"pdfs"]


df=pd.DataFrame(columns=["period","FILE_NAME","dc:identifier","prism:doi","original","TXT","introduction",
                "background","review","framework","methodology","analysis","results",
                "findings","conclusions","discussion","bibliography","references"]) 

count=0
for efe in FOLDERS1+FOLDERS2:
    # break
    FILES=os.listdir(efe)
    # break
    for fi in tqdm(FILES):
        # break
        try:
            if efe.find(DIR1)>-1:
                vals=DT1_org1.loc[(efe[-1]+"_"+fi)[:-4]]
                period="<Aug,2022"
                dc_identifier=vals["dc:identifier"]
                doi=vals["prism:doi"]
            else:
                vals=DT1_org2.loc[(fi)[:-4]]
                period="Aug,2022-Feb,2024"
                dc_identifier=vals["dc:identifier"]
                doi=vals["prism:doi"]
            # Replacing abreviations with meanings
            subprocess.run('conda run -n scispacy python "@SofiaG1L/Database_CCA/MainFunctions/FindAbreviations.py" "'+\
                           efe+"\\"+fi+'"',shell=True, check=True)
            ABR=pd.read_csv("@SofiaG1L/NLP4LitRev/MainFunctions/PROCESSED/Temporal.csv")
            
            with open(efe+"\\"+fi,encoding="utf-8") as ewe:
                TXT=ewe.read()
            TXT=TXT.lower()
            TXT=normalize(TXT)
            # doi=[x for x in tprep.resources.RE_URL.findall(TXT) if x.find("doi")>-1][0]
            TXT=tprep.replace.urls(TXT)
            # TXT=TXT.replace("-\n","")
            # TXT=FN.FixingSpaces(TXT)
            
            TXT=FN.ReplaceAbre(TXT,ABR)
            
            sections={"references":['r(\s|)e(\s|)f(\s|)e(\s|)r(\s|)e(\s|)n(\s|)c(\s|)e'],
                      "bibliography":['b(\s|)i(\s|)b(\s|)l(\s|)i(\s|)o(\s|)g(\s|)r(\s|)a(\s|)p(\s|)h'],
                      "discussion":['d(\s|)i(\s|)s(\s|)c(\s|)u(\s|)s(\s|)s(\s|)i(\s|)o(\s|)n'],
                      "conclusions":['c(\s|)o(\s|)n(\s|)c(\s|)l(\s|)u(\s|)s(\s|)'], # sion
                      "findings":['f(\s|)i(\s|)n(\s|)d(\s|)i(\s|)n(\s|)g(\s|)s'],
                      "results":['r(\s|)e(\s|)s(\s|)u(\s|)l(\s|)t'],
                      "analysis":['a(\s|)n(\s|)a(\s|)l(\s|)y(\s|)s'],
                      "methodology":['m(\s|)e(\s|)t(\s|)h(\s|)o(\s|)d'],
                      "framework":['f(\s|)r(\s|)a(\s|)m(\s|)e(\s|)w(\s|)o(\s|)r(\s|)k'],
                      "review":['l(\s|)i(\s|)t(\s|)e(\s|)r(\s|)a(\s|)t(\s|)u(\s|)r(\s|)e(\s|)r(\s|)e(\s|)v(\s|)i(\s|)e(\s|)w'],
                      "background":['b(\s|)a(\s|)c(\s|)k(\s|)g(\s|)r(\s|)o(\s|)u(\s|)n(\s|)d'],
                      "introduction":['i(\s|)n(\s|)t(\s|)r(\s|)o(\s|)d(\s|)u(\s|)c'],
                      "TXT":['t(\s|)h(\s|)e(\s|)o(\s|)r']}
            
            # Second checking whether the heading is between one \n
            HEADINGS=re.findall(r'(?<=\n\n)(.*)(?=\n)', TXT)
            HEADINGS_OR=HEADINGS.copy()
            HEADINGS=[(ehe.split(":"))[0] for ehe in HEADINGS]
            
            TEMP=[]
            LOCT=[] # we need to locate the original header to later use it
            for ece,ehe in enumerate(HEADINGS):
                if len(ehe)>5 and len(ehe)<30:
                    TEMP.append(ehe)
                    LOCT.append(ece)
            
            HEADINGS=TEMP.copy()
            HEADINGS_OR=[HEADINGS_OR[ece] for ece in LOCT]
            
            HEADINGS2=[]
            LOCT=[]
            for k in sections.keys():
                # break
                for ece,ehe in enumerate(HEADINGS):
                    # break
                    WHERE= re.search(sections[k][0],ehe) #list(map(ehe.find, sections[k]))
                    if type(WHERE)!=type(None):
                        # break
                        HEADINGS2.append(ehe) # sections[k][val[0]]
                        sections[k]=HEADINGS_OR[ece] #ehe
                        LOCT.append(ece)
                        break
            
            for k,v in sections.items():
                if type(v)==list:
                    sections[k]=""
                    
            # Checking that discussion and conclusions do not repeat
            if sections['discussion']==sections['conclusions']:
                sections['conclusions']=''
                
            ROW={}
            ROW[df.columns[0]]=period # cluster
            ROW[df.columns[1]]=re.sub(".txt","",fi) # cluster
            ROW[df.columns[2]]=dc_identifier# doi
            ROW[df.columns[3]]=doi# doi
            # TXT=re.sub('\W+',' ',TXT).strip() # Removes invisible chars
            original=re.split(r''+sections['references']+'',TXT)[0]
            ROW[df.columns[4]]=FN.tokenize(original) # text
            COUNT=len(df.columns)
            NUM_SECTIONS=0
            for k,v in sections.items():
                if len(v)>0:
                    NUM_SECTIONS+=1
                    SE=v
                    try:
                        SPLIT=re.split(r'(?<=\n\n.*)('+SE+')(?=\n)', TXT)
                        TXT=SPLIT[0]
                        if len(SPLIT)>1:
                            ROW[df.columns[COUNT-1]]=[SPLIT[2]]
                    except:
                        print(SE)
                COUNT-=1
            
            ROW["TXT"]=re.sub('\W+',' ',TXT).strip()
            
            df=pd.concat([df,pd.DataFrame([ROW])])
            
            count+=1
            
        except:
            count+=1
            print((fi)[:-4])

df=df.reset_index(drop=True)
# Some "dc:identifiers" are pandas.core.series.Series instead of strings
df["FILE_NAME"]=df["FILE_NAME"].apply(lambda x: x[0] if type(x)!=str else x)
df["dc:identifier"]=df["dc:identifier"].apply(lambda x: x[0] if type(x)!=str else x)
df["prism:doi"]=df["prism:doi"].apply(lambda x: x[0] if type(x)==pd.core.series.Series else x)

# Cleaning each column
for i in df.columns[5:]:
    try:
        df[i]=df[i].apply(lambda x: x.replace("\n\n"," ") if not pd.isna(x) else '')
        
    except:
        df[i]=df[i].apply(lambda x: x[0].replace("\n\n"," ") if not pd.isna(x) else '')

    # df[i]=df[i].apply(lambda x: x.replace("\n"," "))
    df[i]=[re.sub("[^0-9a-z'\(\).,;:\-]+"," ",kk) for kk in df[i]] # removing special characters
    df[i]=[re.sub("\([a-z\s\.]+(,|\s)(;|,|\s|)(,|\s|)[0-9\s\,]+\)","",kk) for kk in df[i]] # removing citations
    df[i]=[re.sub("\(([a-z\s\.]+(,|\s)(;|,|\s|)(,|\s|)[0-9\s\,]+(;|\&|(\band\b)|))+\)","",kk) for kk in df[i]] # removing citations
    df[i]=[re.sub("et al(.|)(\s|)\([0-9]+\)","",kk) for kk in df[i]] # removing et al. (YEAR)
    df[i]=[re.sub("et al(.|)","",kk) for kk in df[i]] # removing et al.
    df[i]=[re.sub("\([0-9]+\)","",kk) for kk in df[i]] # removing (YEAR)
    df[i]=[re.sub(r'(?<=[.,:;])(?=[^\s])', r' ', kk) for kk in df[i]] # Adding space when missing
    # Removing things like: "a n o i t c a e v i t p 00 00 80 0 "
    df[i]=[re.sub("(([a-z]|[0-9.]{1,4})\s){2,20}","",kk) for kk in df[i]]
    # Removing anything between parentheses and the parentheses
    df[i]=[re.sub("[\(\[].*?[\)\]]", "", kk) for kk in df[i]]
    

# =============================================================================
# Merging with Metadata
# =============================================================================
df2=df.merge(DT1_org,how="left",on=["period","FILE_NAME","dc:identifier","prism:doi"])

df2=df2[pd.isna(df2["dc:title"])==False]
df2=df2.reset_index(drop=True)

# =============================================================================
# Cleaning Title
# =============================================================================
# Removing journals mark
df2['dc:title']=df2['dc:title'].apply(normalize)
df2['dc:title']=[re.sub("(\s|)[^0-9a-z\(\).,;:-]+"," ",kk.lower()) for kk in df2['dc:title']] 

# =============================================================================
# Cleaning Abstract
# =============================================================================

# Removing journals mark
df2['description']=df2['description'].apply(lambda x: '' if pd.isna(x) else x)
df2['description']=df2['description'].apply(normalize)
df2['description']=[re.sub("(\s|)[^0-9a-z\(\).,;:-]+"," ",kk.lower()) for kk in df2['description']] # removing special characters

df2['description']=[re.sub("^[a-z0-9,\s]* ag.","",kk) for kk in df2['description']] # 2019 elsevier ltd 
df2['description']=[re.sub("^[a-z0-9,\s]* ltd(\.|)","",kk) for kk in df2['description']] # 2019 elsevier ltd 
df2['description']=[re.sub("^[a-z0-9,\s]* b.v.","",kk) for kk in df2['description']] # 2019 elsevier ltd 
df2['description']=[re.sub("^[a-z0-9,\s]* llc.","",kk) for kk in df2['description']] # 2019 elsevier ltd 

df2['description']=[re.sub("^[a-z0-9,\s\(\)\.]* the author(\(|)(s|)(\)|)(\.|)","",kk) for kk in df2['description']] # 2021 the author(s)
df2['description']=[re.sub("^[a-z0-9,\s\(\)\.]* the author(\(|)(s|)(\)|) [0-9]+.","",kk) for kk in df2['description']] # 2021 the author(s)
df2['description']=[re.sub("^[a-z0-9,\s\.]* all rights reserved.","",kk) for kk in df2['description']] # 2019 elsevier ltd 

df2['description']=[re.sub("^[a-z0-9,\s\.]* sage [a-z0-9,\s]*.","",kk) for kk in df2['description']]
df2['description']=[re.sub("^[a-z0-9,\s\.]* wiley [a-z0-9,\s]*.","",kk) for kk in df2['description']]
df2['description']=[re.sub("^[a-z0-9,\s\.]*elsevier gmbh","",kk) for kk in df2['description']]
df2['description']=[re.sub("^[a-z0-9,\s\.]*elsevier [a-z0-9,\s]*.","",kk) for kk in df2['description']]
df2['description']=[re.sub("^[a-z0-9,\s\.]* springer[a-z0-9,\-\s]*.","",kk) for kk in df2['description']] # 2021, springer nature switzerland ag.
df2['description']=[re.sub("^[a-z0-9,\s\.]* francis group[a-z0-9,\s]*.","",kk) for kk in df2['description']] # 2021, springer nature switzerland ag.
df2['description']=[re.sub("^[a-z0-9,\s\.]* taylor francis[a-z0-9,\s]*.","",kk) for kk in df2['description']] # 2021, springer nature switzerland ag.
df2['description']=[re.sub("^[a-z0-9,\s]* all rights reserved.","",kk) for kk in df2['description']] # 2019 elsevier ltd 
df2['description']=[re.sub("^[a-z0-9,\s\.]*copyright[a-z0-9,\-\s]*.","",kk) for kk in df2['description']] # 2021, springer nature switzerland ag.
df2['description']=[re.sub("^[a-z0-9,\s\.]*published[a-z0-9,\-\s]*.","",kk) for kk in df2['description']] # 2021, springer nature switzerland ag.
df2['description']=[re.sub("^[a-z0-9,\s\.]*open access[a-z0-9,\-\s]*.","",kk) for kk in df2['description']]

df2['description']=[re.sub("(\s|)licensee mdpi(,|) basel(,|) switzerland.","",kk) for kk in df2['description']] # 2021 by the author(s)
df2['description']=[re.sub("(\s|)[0-9]+(,|) emerald publishing limited(\.|\s|)purpose(\.|:|\s|)","",kk) for kk in df2['description']] # 2018, emerald publishing limited.purpose:
df2['description']=[re.sub("copyright [0-9]+ gammage and jarre.","",kk) for kk in df2['description']] # 2019 elsevier ltd 
df2['description']=[re.sub("e.c.h. keskitalo and b.l. preston [0-9]+.","",kk) for kk in df2['description']] # 2019 elsevier ltd 
df2['description']=[re.sub("[0-9]+ castrej n, charles.","",kk) for kk in df2['description']] # 2019 elsevier ltd 

df2['description']=[re.sub("[0-9,\s]+ american [a-z]* of [a-z\s]*(\.|\s|)","",kk) for kk in df2['description']] 
df2['description']=[re.sub("[0-9,\s]+ hong kong [a-z]* of [a-z\s]*(\.|\s|)","",kk) for kk in df2['description']] 
df2['description']=[re.sub("(\s|)[0-9]+ international institute for environment and development (iied)(\.|\s|)","",kk) for kk in df2['description']] 
df2['description']=[re.sub("iwa publishing [0-9]+.","",kk) for kk in df2['description']] 
df2['description']=[re.sub("^[a-z0-9,\s]* by iwmi.","",kk) for kk in df2['description']] 
df2['description']=[re.sub("[0-9,]+ newcastle university.","",kk) for kk in df2['description']] 

df2['description']=[re.sub("^[0-9\s]+","",kk) for kk in df2['description']] # 2019 elsevier ltd
df2['description']=[re.sub(r'(?<=[.,:;])(?=[^\s])', r' ', kk) for kk in df2['description']] # Adding space when missing


# =============================================================================
# Adding descriptive columns
# =============================================================================

df2.affiliation.iloc[3][0]['affiliation-country']

AFF=[]
countries=Counter()
for ii in df2.affiliation:
    
    if type(ii)==list:
        AFF0=[]
        n=len(ii)
        
        for ee in range(n):
            AFF0.append(ii[ee]['affiliation-country'])
        
        countries.update(AFF0)            
        AFF.append(AFF0)
    
    else:
        AFF.append([])

df2['affiliation_Country']=AFF


#### Studied Countries ####

## Opening file with possible names
CITIES=pd.read_csv("@SofiaG1l/Database_CCA/DATA/world-cities_csv.csv")
CITIES=CITIES.fillna("")
for cc in CITIES.columns[:-1]:
    CITIES[cc]= CITIES[cc].apply(lambda x: normalize(x.lower()))

## Opening file with possible names
NATIONALITIES=pd.read_csv("@SofiaG1l/Database_CCA/DATA/Nationalities.csv")
NATIONALITIES=NATIONALITIES.fillna("")
for cc in NATIONALITIES.columns:
    NATIONALITIES[cc]= NATIONALITIES[cc].apply(lambda x: normalize(x.lower()))
NATIONALITIES.index=NATIONALITIES['Nationalities in English']

## Some places based in a quick check
DICT_PLC={'usa': 'united states',
 'us': 'united states',
 'u.s.': 'united states',
 'u.s': 'united states',
 'usa': 'united states',
 'san francisco bay': 'united states',
 'southern arizona':'united states',
 'interior alaska':'alaska',
 'hawai i':'alaska',
 'uk .': 'united kingdom',
 'nsw australia':'australia',
 'new york state':'united states',
 'monterey bay': 'united states',
 'western usa':'alaska',
 'california': 'united states',
 'connecticut':'united states',
 'massachusetts':'united states',
 'north american':'united states',
 'west bengal': 'bengal',
 'hawai': 'united states',
 'east naples': 'italy',
 'himalayas': 'nepal',
 'himalaya': 'nepal',
 'uk': 'united kingdom',
 'england': 'united kingdom',
 'london': 'united kingdom',
 'alaska': 'united states',
 'melbourne': 'australia',
 'mexico city': 'mexico',
 'lima': 'peru',
 "cote d'ivoire": "ivory coast",
 'paris': 'france',
 'milan':'italy',
 'philippine':'philippines',
 'south china':'china',
 'north china plain':'china',
 'austrian alps':'austria',
 'bahia - brazil':'brazil',
 'canada s':'canada',
 'northern canada s':'canada',
 'sri lanka s':'sri lanka',
 'vietnam mekong delta':'vietnam',
 'chile s maule':"chile",
 'republic of the marshall islands':'marshall islands'}

## Getting the NEs
nlp = spacy.load('en_core_web_trf')

df2["entitiesTIT"]=df2.progress_apply(lambda x: FN.extract_entities(nlp(x['dc:title']), 
                                    include_types=["GPE","LOC","NORP"], sep=' '),axis=1)
df2["entitiesABS"]=df2.progress_apply(lambda x: FN.extract_entities(nlp(x['description']), 
                                    include_types=["GPE","LOC","NORP"], sep=' '),axis=1)


STD_TIT=df2["entitiesTIT"].progress_apply(lambda x: FN.ExtractCountry(x,CITIES,NATIONALITIES,DICT_PLC))
STD_ABS=df2["entitiesABS"].progress_apply(lambda x: FN.ExtractCountry(x,CITIES,NATIONALITIES,DICT_PLC))


## Checking with title
df2["StudiedPlace"]=[STD_TIT[ii] for ii in range(len(STD_TIT))]
NON=[n for n,i in enumerate(df2.StudiedPlace) if len(i)==0]
len(NON)
## Checking with abstract
df2["StudiedPlace"].iloc[NON]=[STD_ABS[i] for i in NON]
NON=[n for n,i in enumerate(df2.StudiedPlace) if len(i)==0]
len(NON)

### Checking which ones are countries and giving preference to those
    
df2['affiliation_Country'].apply(lambda x: [] if type(x)!=list else x)
df2['affiliation_Country']=df2['affiliation_Country'].apply(lambda x: [y for y in x if y!=None])

## Creating Database with country, code, and region
Countries=Counter([item for sublist in df2['affiliation_Country'] for item in sublist])+\
    Counter([item.title() for sublist in df2['StudiedPlace'] for item in sublist])

COUNTRIES=pd.DataFrame({"Country":Countries.keys()})
COUNTRIES["ISO2"]=""
COUNTRIES["Continent"]=""

for ii in tqdm(range(COUNTRIES.shape[0])):
    text0=FN.get_continent(COUNTRIES.iloc[ii]["Country"])
    COUNTRIES["ISO2"].iloc[ii]=text0[0]
    COUNTRIES["Continent"].iloc[ii]=text0[1]

## Manually checking those classified as Unknown
for cc in tqdm(range(COUNTRIES.shape[0])):
    if  COUNTRIES.ISO2.iloc[cc]=="Unknown":
        C=COUNTRIES.Country.iloc[cc].lower()
        if C in list(DICT_PLC.keys()): # If the country is in the dictionary
            text0=FN.get_continent(DICT_PLC[C].title())
            COUNTRIES["ISO2"].iloc[cc]=text0[0]
            COUNTRIES["Continent"].iloc[cc]=text0[1]
        elif (FN.CheckSubCountry(C,CITIES)).shape[0]>0:  # Checking the region and returning the country
            SUB=FN.CheckSubCountry(C,CITIES)
            SUB=Counter(SUB.country).most_common(1)
            if len(SUB)>0:
                text0=FN.get_continent(SUB[0][0].title())
                COUNTRIES["ISO2"].iloc[cc]=text0[0]
                COUNTRIES["Continent"].iloc[cc]=text0[1]
        else: # Checking the city and returning the country
            SUB=FN.CheckCities(C,CITIES)
            SUB=Counter(SUB.country).most_common(1)
            if len(SUB)>0:
                text0=FN.get_continent(SUB[0][0].title())
                COUNTRIES["ISO2"].iloc[cc]=text0[0]
                COUNTRIES["Continent"].iloc[cc]=text0[1]

# 83    Cote d'Ivoire
COUNTRIES["ISO2"].iloc[83]="CI"
COUNTRIES["Continent"].iloc[83]="AF"
# 94    Bengal
COUNTRIES["ISO2"].iloc[94]="IN"
COUNTRIES["Continent"].iloc[94]="AS"
# 99    Trinidad And Tobago
COUNTRIES["ISO2"].iloc[99]="TT"
COUNTRIES["Continent"].iloc[99]="SA"
# 100    U.S. Virgin Islands
COUNTRIES["ISO2"].iloc[100]="US"
COUNTRIES["Continent"].iloc[100]="NA"
# 101    Western Sahara
COUNTRIES["ISO2"].iloc[101]="EH"
COUNTRIES["Continent"].iloc[101]="AF"
# 103    Svalbard And Jan Mayen
COUNTRIES["ISO2"].iloc[103]="NO"
COUNTRIES["Continent"].iloc[103]="EU"
# 108    Saint Vincent And The Grenadines
COUNTRIES["ISO2"].iloc[108]="VC"
COUNTRIES["Continent"].iloc[108]="NA"
# 109    East Timor
COUNTRIES["ISO2"].iloc[109]="TL"
COUNTRIES["Continent"].iloc[109]="AS"
# 116    Alaska
COUNTRIES["ISO2"].iloc[116]="US"
COUNTRIES["Continent"].iloc[116]="NA"

COUNTRIES.index=COUNTRIES.Country.str.lower()

df2['affiliation_ISO2']=\
    df2['affiliation_Country'].progress_apply(lambda x: [z for z in np.unique([COUNTRIES.loc[y.lower()]["ISO2"] for y in x ]).tolist() if z!="Unknown"])
df2['affiliation_Continent']=\
    [[z for z in np.unique([COUNTRIES.loc[y.lower()]["Continent"] for y in x ]).tolist() if z!="Unknown"]  for x in df2['affiliation_Country']]
df2['StudiedPlace_ISO2']=\
    [[z for z in np.unique([COUNTRIES.loc[y.lower()]["ISO2"] for y in x ]).tolist() if z!="Unknown"] for x in df2['StudiedPlace']]
df2['StudiedPlace_Continent']=\
    [[z for z in np.unique([COUNTRIES.loc[y.lower()]["Continent"] for y in x ]).tolist() if z!="Unknown"]  for x in df2['StudiedPlace']]

NON=[n for n,i in enumerate(df2.StudiedPlace) if len(i)==0]
len(NON)
NON=[n for n,i in enumerate(df2.StudiedPlace_ISO2) if len(i)==0]
len(NON)

## Checking if the region is in the title or abstract for the NONs
REGIONS=pd.read_csv("@SofiaG1l/Database_CCA/DATA/Just_Regions.csv")

for cc in REGIONS.columns:
    REGIONS[cc]=REGIONS[cc].str.lower()

for ii in NON:
    text=df2["dc:title"].iloc[ii]+" "+df2["description"].iloc[ii]
    CC3=np.unique(REGIONS.Sub_Region.iloc[np.where([len(x)>0 for x in
                                    [re.findall(jj,text) for jj in list(REGIONS.Intermediate_Region)]])])
    if len(CC3)>0:
        CC3=CC3.tolist()
        print("Index: "+str(ii)+" "+str(CC3))
        df2['StudiedPlace'].iloc[ii]=CC3
        df2['StudiedPlace_ISO2'].iloc[ii]=CC3
        df2['StudiedPlace_Continent'].iloc[ii]=CC3

NON=[n for n,i in enumerate(df2.StudiedPlace_Continent) if len(i)==0]
len(NON)

#### Adding type of CC hazard ####
HZD_DICT=pd.read_csv("@SofiaG1l/Database_CCA/DATA/Hazards_Dict.csv")

df2["HAZARD"]=""

for ii in range(df2.shape[0]):
    IND=np.where([len(jj)>0 for jj in 
                  [re.findall(jj, df2["dc:title"].iloc[ii]+" "+df2["description"].iloc[ii]) 
                   for jj in HZD_DICT.Hazard]])
    
    if len(IND[0])>0:
        df2["HAZARD"].iloc[ii]='/'.join(HZD_DICT.Hazard[IND[0]])
    
    else:
        IND=np.where([len(jj)>0 for jj in 
                      [re.findall(jj, df2.conclusions[ii]+" "+df2.discussion[ii]) 
                       for jj in HZD_DICT.Hazard]])
        
        if len(IND[0])>0:
            df2["HAZARD"].iloc[ii]='/'.join(HZD_DICT.Hazard[IND[0]])
            # print(df2["HAZARD"].iloc[ii])
            
sum(df2["HAZARD"]!="")
         
#### Type of Study: Qualy vs Quanty ####
STD_DICT=pd.read_csv("@SofiaG1l/Database_CCA/DATA/Qualitative.csv")

df2["TypeMethod"]=""
df2["TypeMethod_RGX"]=""

for ii in range(df2.shape[0]):
    IND=np.where([len(jj)>0 for jj in 
                  [re.findall(jj, df2["dc:title"].iloc[ii]+" "+df2["description"].iloc[ii]) 
                   for jj in STD_DICT.Vocabulary]])
    
    if len(IND[0])>0:
        df2["TypeMethod"].iloc[ii]='/'.join(STD_DICT.Type[IND[0]])
        df2["TypeMethod_RGX"].iloc[ii]='/'.join(STD_DICT.Vocabulary[IND[0]])
    
    else:
        IND=np.where([len(jj)>0 for jj in 
                      [re.findall(jj, df2.methodology[ii]) 
                       for jj in STD_DICT.Vocabulary]])
        
        if len(IND[0])>0:
            df2["TypeMethod"].iloc[ii]='/'.join(STD_DICT.Type[IND[0]])
            df2["TypeMethod_RGX"].iloc[ii]='/'.join(STD_DICT.Vocabulary[IND[0]])
            
        else:
            IND=np.where([len(jj)>0 for jj in 
                          [re.findall(jj, df2.analysis[ii]) 
                           for jj in STD_DICT.Vocabulary]])
            
            if len(IND[0])>0:
                df2["TypeMethod"].iloc[ii]='/'.join(STD_DICT.Type[IND[0]])
                df2["TypeMethod_RGX"].iloc[ii]='/'.join(STD_DICT.Vocabulary[IND[0]])

df2[df2["TypeMethod_RGX"]==""].shape

# with open('@SofiaG1l/Database_CCA/PROCESSED/df2.pickle', 'wb') as handle:
#     pickle.dump(df2, handle, protocol=pickle.HIGHEST_PROTOCOL)

# with open('@SofiaG1l/Database_CCA/PROCESSED/df2.pickle', 'rb') as handle:
#     df2 = pickle.load(handle)


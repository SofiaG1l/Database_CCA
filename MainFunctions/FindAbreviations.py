# -*- coding: utf-8 -*-
"""
Created on Fri Dec 15 09:34:39 2023

@author: sgilclavel
"""

# To import parameters from console
import sys
import pandas as pd
import time
import regex as re

# Import Text Processing Libraries
import spacy as spacy
# Next is for the pipeline
nlp = spacy.load("en_core_sci_sm")

''' Function to detect acronyms'''
# https://github.com/allenai/scispacy#abbreviationdetector

from scispacy.abbreviation import AbbreviationDetector
nlp.add_pipe("abbreviation_detector")

def CheckAbre(text):
    doc = nlp(text)
    ABR=pd.DataFrame(columns=["ABR","DEF"])
    for abrv in doc._.abbreviations:
        # break
        DEF=''.join(re.findall(r'[a-z\s]',str(abrv._.long_form)))
        DEF=DEF.strip()
        DEF=re.sub("\n","",DEF)
        ABR=pd.concat([ABR,
                       pd.DataFrame({"ABR":str(abrv).strip(),"DEF":DEF}, index=[0])],\
                    ignore_index=True)
        # print(f"{abrv} \t ({abrv.start}, {abrv.end}) {abrv._.long_form}")
        ABR=ABR.drop_duplicates()
        ABR=ABR.reset_index(drop=True)
    return(ABR)


if __name__ ==  '__main__':
    
    DIR=sys.argv[1]
    
    with open(DIR,encoding="utf-8") as ewe:
        TXT=ewe.read()
    TXT=TXT.lower()
    TXT=TXT.replace("-\n","")
    
    ABR=CheckAbre(TXT)
    
    ABR.to_csv("C:\Dropbox\TU_Delft\Projects\Floods_CCA\PROCESSED\Temporal.csv")
    








# -*- coding: utf-8 -*-
"""
Created on Fri Nov 11 13:53:34 2022

@author: sgilclavel
"""

# =============================================================================
# Packages
# =============================================================================
import pdfminer
from pdfminer.high_level import extract_text
import os as os


# =============================================================================
# Main
# =============================================================================

# DIR="C:\\Dropbox\\TU_Delft\\FromTatiana\\Erwin-Florence-Wieke project\\"
## Data <=August 2022
DIR="C:\\Dropbox\\TU_Delft\\Projects\\ML_FindingsGrammar\\DATA\\PDFs_Clusters\\"

## Data August 2022 - January 2024
DIR="C:\\Dropbox\\TU_Delft\\Projects\\DataBase\\PROCESSED\\SCOPUS_DATA\\"

# This DB is for the 2nd version methods paper
# DIR="C:\\Dropbox\\TU_Delft\\Projects\\Unsupervised\\Version2\\Paper2Compare\\" 

FOLDERS=os.listdir(DIR)
## Data <=August 2022
# FOLDERS=[x for x in FOLDERS if x.find("Cluster")>-1]
## Data August 2022 - January 2024
FOLDERS=["pdfs"]

for efe in FOLDERS: # ['Here']
    # break
    FILES=os.listdir(DIR+efe)
    FILES=[f for f in FILES if f.find(".pdf")>0]
    for fi in FILES:
        # break
        PDF=extract_text(DIR+efe+"\\"+fi)
        directory=DIR+"\\TXT\\"+efe
        
        if not os.path.exists(directory):
            os.makedirs(directory)
        # break
        with open(directory+"\\"+fi[:-3]+"txt","w", encoding="utf-8") as ewe:
            ewe.write(PDF)














# -*- coding: utf-8 -*-

"""
##################################
# 
# Author: Dr. Sofia Gil-Clavel
# 
# Last update: October 31st, 2024.
# 
# Description: Script to transform PDFs into text, as explained in:
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
#   - @SofiaG1L/Database_CCA/PY_ENVIRONMENT/pdf2text.yml
#
##################################
"""

# =============================================================================
# Packages
# =============================================================================
from pdfminer.high_level import extract_text
import os as os


# =============================================================================
# Transforming PDFs to text files depending on the data batch:
# =============================================================================

## Data <=August 2022
DIR="@SofiaG1l/Database_CCA/PROCESSED/SCOPUS_DATA/PDFs_Clusters/"
FOLDERS=os.listdir(DIR)
FOLDERS=[x for x in FOLDERS if x.find("Cluster")>-1]

## Data August 2022 - January 2024
DIR="@SofiaG1l/Database_CCA/PROCESSED/SCOPUS_DATA/"
FOLDERS=["pdfs"]

for efe in FOLDERS: 
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
















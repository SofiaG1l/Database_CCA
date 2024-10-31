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
#   - @SofiaG1L/NLP4LitRev/PY_ENVIRONMENT/pdf2text.yml
#
##################################
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

DIR="@SofiaG1l/Database_CCA/PROCESSED/SCOPUS_DATA/"

FOLDERS=os.listdir(DIR)

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














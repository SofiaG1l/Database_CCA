# -*- coding: utf-8 -*-
"""
Created on Fri Dec 15 18:25:18 2023

@author: sgilclavel
"""

#function to convert to alpah2 country codes and continents
from pycountry_convert import country_alpha2_to_continent_code, country_name_to_country_alpha2
import pandas as pd
import sys

def get_continent(col):
    if pd.isna(col):
        return ('Unknown', 'Unknown')
    
    try:
        cn_a2_code =  country_name_to_country_alpha2(col)
    except Exception as e: 
        print(e)
        cn_a2_code = 'Unknown' 
    try:
        cn_continent = country_alpha2_to_continent_code(cn_a2_code)
    except Exception as e: 
        print(e)
        cn_continent = 'Unknown' 
    return (cn_a2_code, cn_continent)


if __name__ ==  '__main__':
    cn_a2_code, cn_continent=get_continent(str(sys.argv[1]))
    print(f"{cn_a2_code}, {cn_continent}")
    

# -*- coding: utf-8 -*-
"""
Created on Sat Dec  9 17:10:39 2023

@author: sgilclavel
"""

# To import parameters from console
import sys

# Import Text Processing Libraries
import spacy as spacy
# Next is for the pipeline
# nlp = spacy.load('en_core_web_sm') # Small English language model
import en_core_web_sm
nlp = en_core_web_sm.load()
import neuralcoref
neuralcoref.add_to_pipe(nlp)


def NeuralCoref(STR):
    doc=nlp(STR)
    if len(doc._.coref_resolved)>0:
        return(doc._.coref_resolved)
    else:
        return(str(doc))
    
    
if __name__ ==  '__main__':
    
    with open(sys.argv[1], encoding='utf-8') as text_file:
        text = text_file.read()
        
    text = NeuralCoref(text)
    
    with open(sys.argv[1], "w", encoding='utf-8') as text_file:
        text_file.write(text)


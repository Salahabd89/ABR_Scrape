
import xml.etree.ElementTree as ET 
import requests
import pandas as pd
import numpy as np
import re
import os
import copy
from collections import defaultdict
import multiprocessing as mp


def fastload(csv_file_location, fastload_command):
    
    os.system(csv_file_location)

    print("Fastload started...")

    values = os.system(fastload_command)

    print(values)

def getNodeText(node):
    text = re.sub(r'\s', ' ', node.text) #clean text
    return text if node is not None else None

def parse_root(root):
    # iterate over every node
     return [parse_elements(child, abr_list = None, list_other_c = None) for child in root.getchildren()]

def addToList(other_c_list):
    
    if (other_c_list['ABN'] + other_c_list['NonIndividualNameText']) not in nameDict.keys() and other_c_list is not None:
        otherNames.append(other_c_list)
        
def parse_elements(root, abr_list, list_other_c):
        
        if abr_list is None:
            abr_list = {}
            list_other_c = {}
            
        for key in root.keys():
            
            if key in abr_list and key == 'type':
                type_ = root.attrib.get(key)
                list_other_c['type'] = type_
            else:
                abr_list[key] = root.attrib.get(key)

        if root.text:
            if root.tag in abr_list:
                name = getNodeText(root) 
                abn = abr_list["ABN"]
                list_other_c['abn'] = abn
                list_other_c['name'] = name
            else:       
                 abr_list[root.tag] = getNodeText(root) 
        
        if (len(list_other_c) == 3):
            
            new_dict = copy.deepcopy(abr_list)
            new_dict = {**new_dict,'ABN': list_other_c['abn'], 
                        'type': list_other_c['type'], 'NonIndividualNameText': list_other_c['name']}
            addToList(new_dict)
            nameDict[list_other_c['abn'] + list_other_c['name']] = 0
            list_other_c = {}
                
        for next_child in root:
            parse_elements(next_child, abr_list, list_other_c)
                        
        
        return abr_list

#%%
if __name__ == '__main__':
 
        # Create a data frame

    otherNames  = []
    nameDict  = {}
    abr = pd.DataFrame()

    print("Started parsing ABR xml")

    for filename in os.listdir('C:/Users/m047207/projects/abr/xml/'):
    
        otherNames  = []
        nameDict  = {}
        abr = pd.DataFrame()
                
        xtree = ET.parse('C:/Users/m047207/projects/abr/xml/{0}'.format(filename))

        root = xtree.getroot()
        abr_ = parse_root(root)
                
        abr = abr.append(abr_, ignore_index = True)
        otherNames = [x for x in otherNames if x is not None]
        abr = abr.append(otherNames, ignore_index = True)
        abr.drop_duplicates()
        print(filename + " has Completed")

        #create csv from data frame
        abr.to_csv("C:/Users/m047207/projects/abr/files/abr_{0}.csv".format(filename), sep="|", index=False)
        print("completed")



    print("Started fastloading..")
    final_file = pd.DataFrame()

    for csv in os.listdir('C:/Users/m047207/projects/abr/files/'):
        
        print(csv)
        print("reading started")
        csv_file = pd.read_csv('C:/Users/m047207/projects/abr/files/{0}'.format(csv),sep="|", skiprows=[1])
        print("file completed")
        final_file = final_file.append(csv_file, ignore_index=True)

    final_file.to_csv("C:/Users/m047207/projects/abr/final/abr.csv", sep="|", index=False)


        #fastload into teradata
    fastload('C://Users//m047207//projects//abr//code//files//final//', 'fastload < script.fld')





#
# GISinc Contractor: Alec B.

# Description: This script was used to track the signs implementation amongnst the states. The function here can be used
# to cover other data sets as well. Just change the dataset variable

# NOTE: Certain SDE connections need to you to operate in the CITRIX environment in order to connect to them. Because of this, it is better to run this script
# in the CITRIX environment.
#
# Start imports
import arcpy
import os
import datetime
import calendar
import glob
from os import listdir
from os.path import join,basename,dirname
from collections import OrderedDict
import pandas as pd
from time import time
import sys
import csv
import re
# End imports

######################
## Global Variables ##
######################
start = time()
present_date = str(datetime.date.today())
d = re.sub('-', '', present_date)
root = r'\\blm\dfs\loc\EGIS\ProjectsNational\NationalDataQuality\Sprint\analysis_tools\_sde_connections'
#####
dataset = ['signs'] # Can be changed to look at other datasets
#####
# NOC Replica
qaqc_only = [] ## qaqc list
qaqc_only += [p for p in os.listdir(root) if 'qaqc' in p]
# State Pubs
pub_only = [] # publication list
pub_only += [each for each in os.listdir(root) if 'publication' in each]
pub_only.remove('_national_publication.sde') # Remove NOC national pub
# State Edt
edt_only = [] # Edit list
edt_only += [each for each in os.listdir(root) if 'edit' in each]
# #
SDE_Type = [] # Edt, Pub, or NOC Replica
fc_lst = [] # Feature Dataset Name
feature_paths = [] # SDE absolute full paths
num_fields = [] # Number of fields in dataset
record_count = [] # Total record count
#
# Def
def data_review(sde_list):
    for q in sde_list:
        print ('Checking: ' + q + '.....')
        q_abbrv = q.split('.sde')[0] 
        noc_rep = root + os.sep + q
        arcpy.env.workspace = noc_rep
        FeatureList = arcpy.ListDatasets('*','Feature')
        TableList = arcpy.ListTables('*', 'ALL')

        FeatureList = [str(r) for r in FeatureList]
        TableList = [str(r) for r in TableList]

        NOC_data_flist = [] # Blank list to append the features (from FeatureList) that are a part of the national standards (checklist)
        for d in dataset:
            temp = [] # Temporary List
            temp = [t for t in FeatureList if d in t]
            if len(temp) < 1:
                temp = [t for t in FeatureList if d.upper() in t]
                NOC_data_flist += temp
            elif len(temp) >= 1:
                NOC_data_flist += temp
            else:
                pass
        #
        NOC_data_tlist = [] # Blank list to append the features (from TableList) that are a part of the national standards (checklist)
        for d in dataset:
            temp = [] # Temporary List
            temp = [t for t in TableList if d in t]
            if len(temp) < 1:
                temp = [t for t in TableList if d.upper() in t]
                NOC_data_tlist += temp
            elif len(temp) >= 1:
                NOC_data_tlist += temp
            else:
                pass
        
        #print NOC_data_flist
        #print NOC_data_tlist
        if len(NOC_data_flist) > 0:
            try:
                for i in NOC_data_flist:
                    for fc in arcpy.ListFeatureClasses(feature_dataset=i):
                        SDE_Type.append(q_abbrv)
                        path = os.path.join(arcpy.env.workspace, i, fc)
                        feature_paths.append(path)
                        #
                        num = int(arcpy.GetCount_management(path).getOutput(0)) # Number of Records count
                        record_count.append(num)
                        #
                        field_names = [f.name for f in arcpy.ListFields(path)] # List of fields in unicode i e u'FIELD'
                        field_names = [str(r) for r in field_names] # List of fields in str() format
                        #
                        num_fields.append(len(field_names)) # Number of fields in fc table
                        #
                        fc = str(fc) # Transform feature name
                        fc_lst.append(fc.lower()) # Append feature dataset name
            except:
                SDE_Type.append(q_abbrv)
                feature_paths.append('No fc data/connection')
                num_fields.append('No fc data/connection')
                record_count.append('No fc data/connection')
                fc_lst.append('No fc data/connection')
        else:
            SDE_Type.append(q_abbrv)
            feature_paths.append('No fc data/connection')
            num_fields.append('No fc data/connection')
            record_count.append('No fc data/connection')
            fc_lst.append('No fc data/connection')

        if len(NOC_data_tlist) > 0:
            try:
                for i in NOC_data_tlist:
                    SDE_Type.append(q_abbrv)
                    path = os.path.join(arcpy.env.workspace, i)
                    feature_paths.append(path)
                    #
                    num = int(arcpy.GetCount_management(path).getOutput(0)) # Number of Records count
                    record_count.append(num)
                    #
                    field_names = [f.name for f in arcpy.ListFields(path)] # List of fields in unicode i e u'FIELD'
                    field_names = [str(r) for r in field_names] # List of fields in str() format
                    #
                    num_fields.append(len(field_names)) # Number of fields in fc table
                    #
                    fc = str(i) # Transform feature name
                    fc_lst.append(fc.lower()) # Append feature dataset name
            except:
                SDE_Type.append(q_abbrv)
                feature_paths.append('No table data/connection')
                num_fields.append('No table data/connection')
                record_count.append('No table data/connection')
                fc_lst.append('No table data/connection')
        else:
            SDE_Type.append(q_abbrv)
            feature_paths.append('No table data/connection')
            num_fields.append('No table data/connection')
            record_count.append('No table data/connection')
            fc_lst.append('No table data/connection')
            
######################
## State Edit Check ##
######################

data_review(edt_only)

#####################
## State Pub Check ##
#####################
data_review(pub_only)

#######################
#######################

#######################
## NOC Replica Check ##
#######################

data_review(qaqc_only)

######################
######################

##########################################
## Make Dataframe and create csv output ##
##########################################

# Columns
columns = ['SDE_Type', 'Feature_Name', 'Total_Field_Count', 'Record_Count', 'Path']

# Review Date Variable
present_time = datetime.datetime.now()

# Combine all into pandas dataframe

df = pd.DataFrame(list(zip(SDE_Type, fc_lst, num_fields, record_count, feature_paths)), columns = columns)

# Add review date column
df['Review Date'] = present_time

# Add Comments column
df['Comments'] = ''
#print df

# To signs_imp_check.py directory
df.to_csv(r'\\blm\\dfs\\loc\\EGIS\\ProjectsNational\\NationalDataQuality\\Sprint\\analysis_tools\\SIGNS_review\\signs_compliance\\signs_compliance_%s.csv' % d, index = False)

# To gui_fram.py directory outputs
df.to_csv(r'\\blm\\dfs\\loc\\EGIS\\ProjectsNational\\NationalDataQuality\\Sprint\\analysis_tools\\Sprint_gui\\outputs\\signs_compliance_%s.csv' % d, index = False)


end = time()
print ("--- NOC Review Took: %s seconds ---" % (end - start))
# Wait for 5 seconds
time.sleep(5)

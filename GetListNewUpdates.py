#!/usr/bin/env python
# coding: utf-8

# ## Welcome to your notebook.
# 

# #### Run this cell to connect to your GIS and get started:

# In[1]:


#Script created by Donald Maruta - NCL ICB Senior Geospatial Manager - 21 Feb 24
from arcgis.gis import GIS
gis = GIS("home")


# #### Now you are ready to start!

# In[2]:


#Import Modules
import requests, csv, os, time, shutil, arcpy, glob
import pandas as pd
from arcgis.gis import GIS
from datetime import date
from pathlib import Path
from arcgis.features import FeatureLayerCollection
todayDate = datetime.datetime.now().strftime("%y%m%d%y%H%M")
arcpy.env.workspace = "/arcgis/home/CancerDashboard/FingerTips_Ingest_" + todayDate + ".gdb"
filePath = "/arcgis/home/CancerDashboard"
todayDate = datetime.datetime.now().strftime("%y%m%d%y%H%M")
fileGDB = "FingerTips_Ingest_" + todayDate + ".gdb"
arcpy.management.CreateFileGDB(filePath, fileGDB)
fldrPath = "/arcgis/home/CancerDashboard/"


# In[3]:


#Import Metadata and OldMetadata CSV files
metadata = "/arcgis/home/CancerDashboard/Metadata.csv"
metadata_df = pd.read_csv(metadata)

oldmetadata = "/arcgis/home/CancerDashboard/OldMetadata.csv"
oldmetadata_df = pd.read_csv(oldmetadata)


# In[4]:


combimetadata = pd.merge(oldmetadata_df, metadata_df, on="IndicatorId")
combimetadata.columns = ["IndicatorId", "OldDate", "NewDate"]
combimetadata


# In[5]:


#Import list of GPs
GPlist = "/arcgis/home/CancerDashboard/CancerGP.csv"
GPlistDF = pd.read_csv(GPlist)
GPlistDF.columns = ["IndicatorId"]


# In[6]:


GPmetadata = pd.merge(combimetadata, GPlistDF, on="IndicatorId")
GPmetadata


# In[7]:


NCL_GPs = "/arcgis/home/CancerDashboard/NCLICB_GPs.csv"
NCL_GPs_df = pd.read_csv(NCL_GPs)
NCL_GPs_df.columns = ["Area Code", "Latitude", "Longitude", "PostCode", "MSOA21CD", "Borough", "PCN_Code", "PCN_Name"]


# In[8]:


NCL_GPs_df


# In[9]:


#Number of iterations needed
length = len(GPmetadata)
#length = 1


# In[10]:


#Creation of the loop
for i in range(length):
        
    #Check to see if data requires updating
    oldDate = GPmetadata.loc[i, 'OldDate']
    newDate = GPmetadata.loc[i, 'NewDate']
    if oldDate == newDate:
        continue
    
    # Maximum number of download attempts
    max_attempts = 10

    # Sleep time between retry attempts (in seconds)
    retry_delay = 30
    
    #Input name of Fingertips below
    fingerTips = str(GPmetadata.loc[i, 'IndicatorId'])
    csvfile = "GP"+fingerTips
    
    # Maximum number of download attempts
    max_attempts = 10
    #max_attempts = 1

    # Sleep time between retry attempts (in seconds)
    retry_delay = 30

    for attempt in range(max_attempts):

        #Download Files by GP Practice
        url = "https://fingertips.phe.org.uk/api/all_data/csv/for_one_indicator?indicator_id=" + fingerTips
        print(url)
        response_API = requests.get(url, timeout=3600)
        # Check if the file is correct (you can replace this condition)
        if response_API.status_code == 200:
            data = response_API.text
            print("Data downloaded")

            #Save as CSV
            csvfile = "GP"+fingerTips
            outputcsv = "/arcgis/home/CancerDashboard/GP" + fingerTips + ".csv"
            print(outputcsv)
            open(outputcsv, "wb").write(response_API.content)
            print("Data written")
            
            #Import into Dataframe
            fileDF = pd.read_csv(outputcsv, low_memory=False)
            #print(fileDF)
            tempDF = pd.merge(fileDF, NCL_GPs_df, on="Area Code", how="inner")
            tempDF.to_csv(outputcsv)
            break

        else:
            print("Incorrect file. Will retry")
            time.sleep(retry_delay)
            
    #Initial creation of the service - GP Services
    #my_csv = outputcsv
    #item_prop = {'title':csvfile}
    #csv_item = gis.content.add(item_properties=item_prop, data=my_csv)
    #params={"type": "csv", "locationType": "coordinates", "latitudeFieldName": "Latitude", "longitudeFieldName": "Longitude"}
    #csv_item.publish(publish_parameters=params)
    #csv_item.publish(overwrite = True)
              
    #Overwrite the existing service - All Services
    # Search for 'GP' feature layer collection
    search_result = gis.content.search(query=csvfile, item_type="Feature Layer")
    feature_layer_item = search_result[0]
    feature_layer = feature_layer_item.layers[0]
    feat_id = feature_layer.properties.serviceItemId
    item = gis.content.get(feat_id)
    item_collection = FeatureLayerCollection.fromitem(item)
    #call the overwrite() method which can be accessed using the manager property
    item_collection.manager.overwrite(outputcsv)
    item.share(everyone=True)
    update_dict = {"capabilities": "Query,Extract"}
    item_collection.manager.update_definition(update_dict)
    item.content_status="authoritative"    


# In[11]:


#Code to delete unnecessary files
arcpy.env.workspace = '/arcgis/home/CancerDashboard'

# Get a list of all subdirectories (folders) in the specified folder
folders = [f for f in os.listdir(fldrPath) if os.path.isdir(os.path.join(fldrPath, f))]

for folder in folders:
    folder = os.path.join(fldrPath, folder)
    shutil.rmtree(folder)

#List of files to preserve
files_to_preserve = ["CancerGP.csv", "CancerMSOAWard.csv", "Metadata.csv", "MSOA2011.dbf", "MSOA2011.prj", "MSOA2011.shp", "MSOA2011.shx", "MSOA2011.zip", "NCLICB_GPs.csv", "OldMetadata.csv", "Ward2021.dbf", "Ward2021.prj", "Ward2021.shp", "Ward2021.shx", "Ward2021.zip"]

# Get a list of all files in the directory
all_files = glob.glob(os.path.join(fldrPath, "*"))

# Iterate over each file
for file_path in all_files:
    # Get the file name
    file_name = os.path.basename(file_path)
    
    # Check if the file name is not in the list of files to preserve
    if file_name not in files_to_preserve:
        # Delete the file
        os.remove(file_path)
        print(f"Deleted {file_name}")

print("All files except the specified ones have been deleted.")


# In[12]:


print("Alles gemacht!")


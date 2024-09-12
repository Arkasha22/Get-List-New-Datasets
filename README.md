# Get-List-New-Datasets

Python code desigend to be run in ArcGIS OnLine Notebook

This allows users to create a list of datasets from PHE FingerTips (Open Source Data) which require updating.

It carries out the following actions in the following order  
- Connect to AGOL  
- Import Required Modules
- Copy current metadata file to backup metadata file  
- Variables Required to Import Updated Metadata from FingerTips  
- Open the URL Request  
- Write to the CSV file  
- Create Pandas dataframe  
- Rejig dataframe columns and rename  
- Write the resulting DataFrame to a new CSV file  

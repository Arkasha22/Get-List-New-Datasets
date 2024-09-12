# Get-List-New-Datasets

Python code desigend to be run in ArcGIS OnLine Notebook

This allows users to create a list of datasets from PHE FingerTips (Open Source Data) which require updating.

It carries out the following actions in the following order  
- Connect to AGOL  
- Import Required Modules
- Create Variables  
- Import Metadata and OldMetadata CSV files  
- Merge Both Metadata DataFrames  
- Import list of GPs  
- Merge Metadata and GP list Data Frames
- Create Data Frame of NCL ICB GPs
- Number of iterations needed
- Creation of loop to process FingerTips Data
- Initial creation of the service - GP Services
- Overwrite the existing service - All Services
- Code to delete unnecessary files

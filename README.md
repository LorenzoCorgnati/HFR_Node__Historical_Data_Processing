# HFR_Node__Historical_Data_Processing
These applications are written in Matlab language and they are based on HFR_Progs_2_1_2 and M_Map toolboxes, and the architecture of the workflow is based on a MySQL database containing information about data and metadata. The applications are designed for High Frequency Radar (HFR) data management according to the European HFR node processing workflow, thus generating radial and total velocity files in netCDF format according to the European standard data and metadata model for near real time HFR current data.

THIS APPLICATION IS DESIGNED FOR PROCESSING HISTORICAL DATA. 

This application supports batch processing.

Start and end dates must be specified as comma separated list at lines 50-51 of the H_EU_HFR_Node_Processor.m wrapper.

HFR networks to be processed must be specified as comma separated list at line 54 of the H_EU_HFR_Node_Processor.m wrapper.


THE INPUT DATA TO BE PROCESSED MUST BE PLACED IN A FOLDER NAMED WITH THE NETWORK ID OF THE HFR NETWORK AT THE SAME LEVEL OF THE FOLDER CONTAINING THE APPLICATION (I.E. A FOLDER ROOTWARD WITH RESPECT TO THE SCRIPT FILES). THE NETWORK-ID-NAMED FOLDER MUST HAVE THE FOLLOWING STRUCTURE:

HFR_network_id

      |__________Radials_ruv (containing input .ruv radial files)
      
      |               |__________station_id_1 (one per each radial station)
      
      |               |__________station_id_2
      
      |               |__________    ...
      
      |               |__________station_id_n
      
      |
      
      |__________Radials_asc (containing input .crad_ascii and .asc radial files)
      
      |               |__________station_id_1 (one per each radial station)
      
      |               |__________station_id_2
      
      |               |__________    ...
      
      |               |__________station_id_n
      
      |
      
      |__________Radials_nc (containing output .nc radial files)
      
      |
      
      |__________Totals_tuv (containing input .tuv total files)
      
      |
      
      |__________Totals_asc (containing input .cur_asc and .asc total files)
      
      |
      
      |__________Totals_mat (containing output .mat total files)
      
      |
      
      |__________Totals_nc (containing output .nc total files)

The database is composed by the following tables:
- account_tb: it contains the general information about HFR providers and the HFR networks they manage.
- network_tb: it contains the general information about the HFR network producing the radial and total files. These information will be used for the metadata content of the netCDF files.
- station_tb: it contains the general information about the radar sites belonging to each HFR network producing the radial and total files. These information will be used for the metadata content of the netCDF files.
- radial_input_tb: it contains information about the radial files to be converted and combined into total files.
- radial_HFRnetCDF_tb: it contains information about the converted radial files.
- total_input_tb: it contains information about the total files to be converted.
- total_HFRnetCDF_tb: it contains information about the converted total files.

The applications are intended to:
- load radial files information in a proper data structure;
- load total files information in a proper data structure;
- convert Codar native .tuv files and WERA native .cur_asc files for total currents into the European standard data and metadata model for near real time HFR current data;
- convert Codar native .ruv files and WERA native .crad_ascii files for radial currents into the European standard data and metadata model for near real time HFR current data and combine them for generating total current files according to the European standard data and metadata model for near real time HFR current data.

General information for the tables network_tb and station_tb are loaded onto the database via a webform to be filled by the data providers. The webform is available at http://150.145.136.36/index.php

All generated radial and total netCDF files are quality controlled according the the QC tests defined as standard for the European HFR node and for the data distribution on CMEMS-INSTAC and SeaDataNet platforms.

The whole workflow is intended to run automatically to convert and combine historical HFR data produced by data providers. The wrapper H_EU_HFR_Node_Processor.m sets the network ID, the starting and ending time and launches the input and processing applications.

The applications H_inputRUV.m, H_inputAscRad.m and H_inputCradAScii.m load radial files information in a proper data structure.

The applications H_inputTUV.m H_inputAscTot.m and H_inputCurAsc.m load total files information in a proper data structure.

The application H_HFR_Combiner.m converts Codar native .ruv files and WERA native .crad_ascii and .asc files for radial currents into the European standard data and metadata model for near real time HFR current data and combines them for generating total current files according to the European standard data and metadata model for near real time HFR current data.

The application H_Total_Conversion.m converts Codar native .tuv files and WERA native .cur_asc and .asc files for total currents into the European standard data and metadata model for near real time HFR current data.


The required toolboxes are:
- HFR_Progs-2.1.2 (https://github.com/rowg/hfrprogs); 
- M_Map (https://www.eoas.ubc.ca/~rich/map.html); 
- GSHHS (http://www.ngdc.noaa.gov/mgg/shorelines/data/gshhs/); 
- Nctoolbox-1.1.3 (https://github.com/nctoolbox/nctoolbox); 
- mysql-connector-java-5.1.17 driver (https://mvnrepository.com/artifact/mysql/mysql-connector-java/5.1.17); 
- Rdir (http://www.mathworks.com/matlabcentral/fileexchange/19550-recursive-directory-listing);
- uniqueStrCell (https://www.mathworks.com/matlabcentral/fileexchange/50476-unique-for-cell-array-of-string).


Author: Lorenzo Corgnati

Date: November 10, 2019

E-mail: lorenzo.corgnati@sp.ismar.cnr.it 


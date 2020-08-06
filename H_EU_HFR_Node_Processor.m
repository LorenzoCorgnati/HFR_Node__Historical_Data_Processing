%% H_EU_HFR_Node_Processor.m
% This wrapper launches the scripts for reading the information about 
% radial and total files (both Codar and WERA) from the HFR database and
% for combining radials into totals and converting radials and totals to
% netCDF files according to the European standard data model.

% This application works on historical data, thus the ID of the network to
% be processed and the processing time interval have to be specified in
% lines 37-48.

% Author: Lorenzo Corgnati
% Date: November 9, 2019

% E-mail: lorenzo.corgnati@sp.ismar.cnr.it
%%

warning('off', 'all');

clear all
close all
clc

% Setup netCDF toolbox
setup_nctoolbox;

% Setup JBDC driver for MySQL
javaaddpath('/home/lorenz/Toolboxes/Matlab_HFR_AddOn/mysql-connector-java-5.1.17.jar');

% Setup map colormap
set(0,'DefaultFigureColormap',feval('jet'));

EHNP_err = 0;

disp(['[' datestr(now) '] - - ' 'H_EU_HFR_Node_Processor started.']);

%%

%% Set database parameters NOT TO BE CHANGED

sqlConfig.user = 'HFR_lorenzo';
sqlConfig.password = 'xWeLXHFQfvpBmDYO';
sqlConfig.host = '150.145.136.8';
sqlConfig.database = 'HFR_node_db';

%%

%% Set HFR networks and time interval to be processe

% START AND END DATES TO BE INSERTED IN THE FORMAT YYYY-MM-DD AS COMMA-SEPARATED LIST
procStart = '2013-01-01'; % Start date included
procEnd = '2013-03-31'; % End date excluded

% NETWORK IDS TO BE INSERTED AS COMMA-SEPARATED LIST
HFRnetworkID = 'HFR-US-EastGulfCoast';

%%

%% Retrieve networks IDs, start processing date and end processing date

try
    HFRPnetworks = regexp(HFRnetworkID, '[ ,;]+', 'split');
    procStartDate = regexp(procStart, '[ ,;]+', 'split');
    procEndDate = regexp(procEnd, '[ ,;]+', 'split');
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    iRDB_err = 1;
end

%%

%% Processing

% Set the radial and total structure column names
toBeCombinedRadials_columnNames = {'filename' 'filepath' 'network_id' 'station_id' 'timestamp' 'datetime' 'reception_date' 'filesize' 'extension' 'NRT_processed_flag'};
toBeConvertedTotals_columnNames = {'filename' 'filepath' 'network_id' 'timestamp' 'datetime' 'reception_date' 'filesize' 'extension' 'NRT_processed_flag'};

for HFRPntw_idx=1:length(HFRPnetworks)
    
    % Set the network ID to be processed
    networkID = HFRPnetworks{HFRPntw_idx};
    
    % Set the radial and total structure initial indices
    tBCR_idx = 0;
    tBCT_idx = 0;
    
    try
        startDate = procStartDate{HFRPntw_idx};
        endDate = procEndDate{HFRPntw_idx};
        assert(datenum(startDate)<datenum(endDate), 'Start date must be previous than end date.');
    catch err
        disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
        HFRC_err = 1;
        return
    end
    
    % RADIALS COMBINATION & RADIALS AND TOTALS CONVERSION
    H_inputRUV;
%     H_inputAscRad;
    H_inputCradAscii;
    H_HFRCombiner;
    
    % TOTALS CONVERSION
    H_inputTUV;
    H_inputAscTot;
    H_inputCurAsc;
    H_TotalConversion;
    
    clear toBeCombinedRadials_data toBeConvertedTotals_data
    
end

disp(['[' datestr(now) '] - - ' 'H_EU_HFR_Node_Processor successfully executed.']);

%%

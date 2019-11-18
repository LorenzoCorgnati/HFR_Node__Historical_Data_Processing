%% H_EU_HFR_Node_Processor.m
% This wrapper launches the scripts for inserting into the HFR database
% the information about radial and totala files (both Codar and WERA) and
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
procStart = '2015-01-01'; % Start date included
procEnd = '2015-01-02'; % End date excluded

% NETWORK IDS TO BE INSERTED AS COMMA-SEPARATED LIST
HFRnetworkID = 'HFR-GoM';

%%

%% Retrieve networks IDs

try
    HFRPnetworks = regexp(HFRnetworkID, '[ ,;]+', 'split');
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    iRDB_err = 1;
end

%%

%% Processing

for HFRPntw_idx=1:length(HFRPnetworks)
    
    networkID = HFRPnetworks{HFRPntw_idx};
    
    try
        startDate = procStart{HFRPntw_idx};
        endDate = procEnd{HFRPntw_idx};
        assert(datenum(startDate)<datenum(endDate), 'Start date must be previous than end date.');
    catch err
        disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
        HFRC_err = 1;
        return
    end
    
    
    % RADIALS COMBINATION & RADIALS AND TOTALS CONVERSION
    H_inputRUV;
    H_inputAscRad;
    H_inputCradAscii;
    H_HFRCombiner;
    
    % TOTALS CONVERSION
    H_inputTUV;
    H_inputAscTot;
    H_inputCurAsc;
    H_TotalConversion;
    
end

disp(['[' datestr(now) '] - - ' 'H_EU_HFR_Node_Processor successfully executed.']);

%%

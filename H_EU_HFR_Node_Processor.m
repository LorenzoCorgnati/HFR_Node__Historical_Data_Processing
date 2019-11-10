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

%% Set HFR networks and time interval to be processe

try
    % START AND END DATES TO BE INSERTED IN THE FORMAT YYYY-MM-DD
    startDate = '2015-01-01'; % Start date included
    endDate = '2015-01-02'; % End date excluded
    assert(datenum(startDate)<datenum(endDate), 'Start date must be previous than end date.');
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    HFRC_err = 1;
    return
end

% NETWORK IDS TO BE INSERTED AS COMMA-SEPARATED LIST
HFRnetworkID = 'HFR-GoM';

%%

%% Set database parameters NOT TO BE CHANGED

sqlConfig.user = 'HFR_lorenzo';
sqlConfig.password = 'xWeLXHFQfvpBmDYO';
sqlConfig.host = '150.145.136.8';
sqlConfig.database = 'HFR_node_db';

%%

%% Processing


% RADIALS COMBINATION & RADIALS AND TOTALS CONVERSION
H_inputRUV2DB;
H_inputAscRad2DB;
H_inputCradAscii2DB;
H_HFRCombiner;

% TOTALS CONVERSION
H_inputTUV2DB;
H_inputAscTot2DB;
H_inputCurAsc2DB;
H_TotalConversion;

disp(['[' datestr(now) '] - - ' 'H_EU_HFR_Node_Processor successfully executed.']);

%%

%% H_USreadTimeIndices.m
% This function reads the time indices from the HFR-US networks via OpenDAP and
% selects the ones to be converted according to the processing time
% interval.

% INPUT:
%         procStart: processing start date
%         procEnd: processing end date
%         networkData: cell array containing information about the network
%                      (metadata)
%         networkFields: field names of the cell array containing
%                       information about the network.

% OUTPUT:
%         UrO_err: error flag (0 = correct, 1 = error)
%         iTime: array containing the time indices from HFR-US network to be converted


% Author: Lorenzo Corgnati
% Date: August 6, 2020

% E-mail: lorenzo.corgnati@sp.ismar.cnr.it
%%

function [UrO_err, iTime] = H_USreadTimeIndices(procStart,procEnd,networkData,networkFields)

disp(['[' datestr(now) '] - - ' 'H_USreadTimeIndices.m started.']);

UrO_err = 0;

warning('off', 'all');

%% Find the processing time interval

try
    % Find the TDS_root_url field from network data
    TDS_root_urlIndex = find(not(cellfun('isempty', strfind(networkFields, 'TDS_root_url'))));
    TDS_root_url = networkData{TDS_root_urlIndex};
    
    % Read time and convert it to Matlab time
    nc.time = ncread_cf_time(TDS_root_url,'time');
    
    % Select the data range according to the processing time span
    iTime = find((nc.time>=datenum(procStart)) & (nc.time<=datenum(procEnd)));
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    UrO_err = 1;
end

%%

if(UrO_err==0)
    disp(['[' datestr(now) '] - - ' 'H_USreadTimeIndices.m successfully executed.']);
end

return


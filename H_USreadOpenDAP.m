%% H_USreadOpenDAP.m
% This function reads data from the HFR-US networks via OpenDAP and
% selects the data subset to be converted according to the processing time
% interval.

% INPUT:
%         timeInd: indices of the time array (related to the data series)
%                   to be converted
%         networkData: cell array containing information about the network
%                      (metadata)
%         networkFields: field names of the cell array containing
%                       information about the network.

% OUTPUT:
%         UrO_err: error flag (0 = correct, 1 = error)
%         nc: data from HFR-US network to be converted


% Author: Lorenzo Corgnati
% Date: August 6, 2020

% E-mail: lorenzo.corgnati@sp.ismar.cnr.it
%%

function [UrO_err, nc] = H_USreadOpenDAP(iTime,networkData,networkFields)

disp(['[' datestr(now) '] - - ' 'H_USreadOpenDAP.m started.']);

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
    nc.time = nc.time(iTime);
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    UrO_err = 1;
end

%%

%% Read variables

if(~isempty(nc.time))
    try
        % Coordinate variables
        nc.latitude = ncread(TDS_root_url,'lat');
        nc.longitude = ncread(TDS_root_url,'lon');
        nc.depth = 0;
        
        % Data variables
        nc.ewct = ncread(TDS_root_url,'u',[1,1,min(iTime)],[length(nc.longitude),length(nc.latitude),length(iTime)]);
        nc.nsct = ncread(TDS_root_url,'v',[1,1,min(iTime)],[length(nc.longitude),length(nc.latitude),length(iTime)]);
        nc.gdopX = ncread(TDS_root_url,'dopx',[1,1,min(iTime)],[length(nc.longitude),length(nc.latitude),length(iTime)]);
        nc.gdopY = ncread(TDS_root_url,'dopy',[1,1,min(iTime)],[length(nc.longitude),length(nc.latitude),length(iTime)]);
        nc.ddens = ncread(TDS_root_url,'number_of_radials',[1,1,min(iTime)],[length(nc.longitude),length(nc.latitude),length(iTime)]);
        
    catch err
        disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
        UrO_err = 1;
    end
    
end

%%

if(UrO_err==0)
    disp(['[' datestr(now) '] - - ' 'H_USreadOpenDAP.m successfully executed.']);
end

return


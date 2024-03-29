%% H_inputRUV.m
% This application lists the input ruv files pushed by the HFR data providers
% and inserts into a proper structure the information needed for the
% combination of Codar radial files into totals and for the generation of the
% radial and total data files into the European standard data model.

% This application works on historical data.

% Author: Lorenzo Corgnati
% Date: November 9, 2019

% E-mail: lorenzo.corgnati@sp.ismar.cnr.it
%%

warning('off', 'all');

iRDB_err = 0;

disp(['[' datestr(now) '] - - ' 'H_inputRUV started.']);

startDateNum = datenum(startDate);
endDateNum = datenum(endDate);

%%

%% Connect to database

try
    conn = database(sqlConfig.database,sqlConfig.user,sqlConfig.password,'Vendor','MySQL','Server',sqlConfig.host);
    disp(['[' datestr(now) '] - - ' 'Connection to database successfully established.']);
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    iRDB_err = 1;
end

%%

%% Query the database for retrieving data from managed networks

% Set and exectute the query
try
    network_selectquery = ['SELECT * FROM network_tb WHERE network_id = ''' networkID ''''];
    network_curs = exec(conn,network_selectquery);
    disp(['[' datestr(now) '] - - ' 'Query to network_tb table for retrieving data of the managed networks successfully executed.']);
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    iRDB_err = 1;
end

% Fetch data
try
    network_curs = fetch(network_curs);
    network_data = network_curs.Data;
    disp(['[' datestr(now) '] - - ' 'Data of the managed networks successfully fetched from network_tb table.']);
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    iRDB_err = 1;
end

% Retrieve column names
try
    network_columnNames = columnnames(network_curs,true);
    disp(['[' datestr(now) '] - - ' 'Column names from network_tb table successfully retrieved.']);
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    iRDB_err = 1;
end

% Retrieve the number of networks
try
    numNetworks = rows(network_curs);
    disp(['[' datestr(now) '] - - ' 'Number of managed networks successfully retrieved from network_tb table.']);
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    iRDB_err = 1;
end

% Close cursor
try
    close(network_curs);
    disp(['[' datestr(now) '] - - ' 'Cursor to network_tb table successfully closed.']);
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    iRDB_err = 1;
end

%%

%% Scan the networks, find the stations, list the related radial files and insert information into the database

try
    % Find the index of the network_id field
    network_idIndexC = strfind(network_columnNames, 'network_id');
    network_idIndex = find(not(cellfun('isempty', network_idIndexC)));
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    iRDB_err = 1;
end

% Scan the networks
try
    for network_idx=1:numNetworks
        iRDB_err = 0;
        try
            % Manage the case of the ISMAR-LaMMA-ARPAS integrated network (HFR-WesternItaly)
            if(strcmp(network_data{network_idx,network_idIndex},'HFR-WesternItaly'))
                station_selectquery = ['SELECT * FROM station_tb WHERE network_id = ' '''HFR-TirLig'' OR network_id = ' '''HFR-LaMMA'' OR network_id = ' '''HFR-ARPAS'''];
            else
                station_selectquery = ['SELECT * FROM station_tb WHERE network_id = ' '''' network_data{network_idx,network_idIndex} ''''];
            end
            station_curs = exec(conn,station_selectquery);
            disp(['[' datestr(now) '] - - ' 'Query to station_tb table for retrieving the stations of the ' network_data{network_idx,network_idIndex} ' network successfully executed.']);
        catch err
            disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
            iRDB_err = 1;
        end
        
        % Fetch data
        try
            station_curs = fetch(station_curs);
            station_data = station_curs.Data;
            disp(['[' datestr(now) '] - - ' 'Data of the stations of the ' network_data{network_idx,network_idIndex} ' network successfully fetched from station_tb table.']);
        catch err
            disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
            iRDB_err = 1;
        end
        
        % Retrieve column names
        try
            station_columnNames = columnnames(station_curs,true);
            disp(['[' datestr(now) '] - - ' 'Column names from station_tb table successfully retrieved.']);
        catch err
            disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
            iRDB_err = 1;
        end
        
        % Retrieve the number of stations belonging to the current network
        try
            numStations = rows(station_curs);
            disp(['[' datestr(now) '] - - ' 'Number of stations belonging to the ' network_data{network_idx,network_idIndex} ' network successfully retrieved from station_tb table.']);
        catch err
            disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
            iRDB_err = 1;
        end
        
        % Close cursor to station_tb table
        try
            close(station_curs);
            disp(['[' datestr(now) '] - - ' 'Cursor to station_tb table successfully closed.']);
        catch err
            disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
            iRDB_err = 1;
        end
        
        try
            % Find the index of the input file path field
            inputPathIndexC = strfind(station_columnNames, 'radial_input_folder_path');
            inputPathIndex = find(not(cellfun('isempty', inputPathIndexC)));
            
            % Find the index of the output file path field
            outputPathIndexC = strfind(station_columnNames, 'radial_HFRnetCDF_folder_path');
            outputPathIndex = find(not(cellfun('isempty', outputPathIndexC)));
            
            % Find the index of the station_id field
            station_idIndexC = strfind(station_columnNames, 'station_id');
            station_idIndex = find(not(cellfun('isempty', station_idIndexC)));
        catch err
            disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
            iRDB_err = 1;
        end
        
        % Scan the stations
        for station_idx=1:numStations
            if(~isempty(station_data{station_idx,inputPathIndex}))
                % Override data folder paths for stations
                station_data{station_idx,inputPathIndex} = ['../' networkID filesep 'Radials_ruv' filesep station_data{station_idx,station_idIndex}];
                station_data{station_idx,outputPathIndex} = ['../' networkID filesep 'Radials_nc'];
                
                % Trim heading and trailing whitespaces from folder path
                station_data{station_idx,inputPathIndex} = strtrim(station_data{station_idx,inputPathIndex});
                % List the input ruv files for the current station
                try
                    ruvFiles = rdir([station_data{station_idx,inputPathIndex} filesep '**' filesep '*.ruv']);
                    disp(['[' datestr(now) '] - - ' 'Radial files from ' station_data{station_idx,station_idIndex} ' station successfully listed.']);
                catch err
                    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
                    iRDB_err = 1;
                end
                
                % Insert information about the ruv file into the data structure
                for ruv_idx=1:length(ruvFiles)
                    iRDB_err = 0;
                    % Retrieve the filename
                    [pathstr,name,ext]=fileparts(ruvFiles(ruv_idx).name);
                    noFullPathName=[name ext];
                    % Retrieve information about the ruv file
                    try
                        % Load the radial file as structure
                        radStruct = loadRDLFile(ruvFiles(ruv_idx).name,'false','warning');
                        % Read the file header
                        radHeader = radStruct.OtherMetadata.Header;
                        % Retrieve information from header
                        for header_idx=1:length(radHeader)
                            splitLine = regexp(radHeader{header_idx}, ' ', 'split');
                            % Retrieve TimeStamp
                            if(strcmp(splitLine{1}, '%TimeStamp:'))
                                TimeStamp = strrep(radHeader{header_idx}(length('%TimeStamp:')+2:length(radHeader{header_idx})), '"', '');
                                break;
                            end
                        end
                    catch err
                        disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
                        iRDB_err = 1;
                    end
                    
                    try
                        % Evaluate datetime from, Time Stamp
                        [t2d_err,DateTime] = timestamp2datetime(TimeStamp);
                    catch err
                        disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
                        iRDB_err = 1;
                    end
                    
                    % Check if the current file belongs to the processing time interval
                    if((datenum(DateTime) >= startDateNum) && (datenum(DateTime) < endDateNum))
                        % Retrieve information about the ruv file
                        try
                            ruvFileInfo = dir(ruvFiles(ruv_idx).name);
                            ruvFilesize = ruvFileInfo.bytes/1024;
                        catch err
                            disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
                            iRDB_err = 1;
                        end
                        
                        % Define a cell array that contains the data for insertion
                        tBCR_idx = tBCR_idx + 1;
                        toBeCombinedRadials_data(tBCR_idx,:) = {noFullPathName,pathstr,network_data{network_idx,network_idIndex},station_data{station_idx,station_idIndex},TimeStamp,DateTime,(datestr(now,'yyyy-mm-dd HH:MM:SS')),ruvFilesize,ext,0};
                        
                    end
                end
            end
        end
    end
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    iRDB_err = 1;
end

%%

%% Close connection

try
    close(conn);
    disp(['[' datestr(now) '] - - ' 'Connection to database successfully closed.']);
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    iRDB_err = 1;
end

%%

if(iRDB_err==0)
    disp(['[' datestr(now) '] - - ' 'H_inputRUV successfully executed.']);
end
%% H_TotalConversion.m
% This application reads the HFR database for collecting information about
% the total data files to be converted into the European standard data
% model and calls the conversion functions.

% This application works on historical data.

% Author: Lorenzo Corgnati
% Date: November 9, 2019

% E-mail: lorenzo.corgnati@sp.ismar.cnr.it
%%

warning('off', 'all');

TC_err = 0;

disp(['[' datestr(now) '] - - ' 'H_TotalConversion started.']);

%%

%% Connect to database

try
    conn = database(sqlConfig.database,sqlConfig.user,sqlConfig.password,'Vendor','MySQL','Server',sqlConfig.host);
    disp(['[' datestr(now) '] - - ' 'Connection to database successfully established.']);
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    TC_err = 1;
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


%% Scan the networks and convert the related total files

try
    % Find the index of the network_id field
    network_idIndexC = strfind(network_columnNames, 'network_id');
    network_idIndex = find(not(cellfun('isempty', network_idIndexC)));
    
    % Find the index of the input file path field
    inputPathIndexC = strfind(network_columnNames, 'total_input_folder_path');
    inputPathIndex = find(not(cellfun('isempty', inputPathIndexC)));
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    TC_err = 1;
end

% Scan the networks
try
    for network_idx=1:numNetworks
        TC_err = 0;
        % Find the index of the output file path field
        outputPathIndexC = strfind(network_columnNames, 'total_HFRnetCDF_folder_path');
        outputPathIndex = find(not(cellfun('isempty', outputPathIndexC)));
        % Override data folder paths for stations
        network_data{network_idx,outputPathIndex} = ['../' networkID filesep 'Totals_nc'];
        
        % Retrieve information on the stations belonging to the current network
        try
            station_selectquery = ['SELECT * FROM station_tb WHERE network_id = ' '''' network_data{network_idx,network_idIndex} ''''];
            station_curs = exec(conn,station_selectquery);
            disp(['[' datestr(now) '] - - ' 'Query to station_tb table for retrieving the stations of the ' network_data{network_idx,network_idIndex} ' network successfully executed.']);
        catch err
            disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
            TC_err = 1;
        end
        
        % Fetch data
        try
            station_curs = fetch(station_curs);
            station_data = station_curs.Data;
            disp(['[' datestr(now) '] - - ' 'Data of the stations of the ' network_data{network_idx,network_idIndex} ' network successfully fetched from station_tb table.']);
        catch err
            disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
            TC_err = 1;
        end
        
        % Retrieve column names
        try
            station_columnNames = columnnames(station_curs,true);
            disp(['[' datestr(now) '] - - ' 'Column names from station_tb table successfully retrieved.']);
        catch err
            disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
            TC_err = 1;
        end
        
        % Retrieve the number of stations belonging to the current network
        try
            numStations = rows(station_curs);
            disp(['[' datestr(now) '] - - ' 'Number of stations belonging to the ' network_data{network_idx,network_idIndex} ' network successfully retrieved from station_tb table.']);
        catch err
            disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
            TC_err = 1;
        end
        
        % Close cursor to station_tb table
        try
            close(station_curs);
            disp(['[' datestr(now) '] - - ' 'Cursor to station_tb table successfully closed.']);
        catch err
            disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
            TC_err = 1;
        end
        
        % Process US HFR networks
        if(contains(network_data{1,network_idIndex},'HFR-US'))
            % Read time indices to be converted via OpenDAP
            [TC_err, UStoBeConvertedIndices] = H_USreadTimeIndices(startDate,endDate,network_data(1,:),network_columnNames);
            
            % Convert files
            for UtBC_idx=1:length(UStoBeConvertedIndices)
                % Read data via OpenDAP
                [TC_err, OpenDAPncData] = H_USreadOpenDAP(UStoBeConvertedIndices(UtBC_idx),network_data(1,:),network_columnNames);
                if(TC_err==0)
                    disp(['[' datestr(now) '] - - ' network_data{1,network_idIndex} ' data successfully read via OpenDAP.']);
                    % v2.2
                    [TC_err, network_data(1,:), outputFilename,outputFilesize] = US2netCDF_v22(OpenDAPncData,1,network_data(1,:),network_columnNames,station_data,station_columnNames);
                    if(TC_err==0)
                        disp(['[' datestr(now) '] - - ' outputFilename ' total netCDF v2.2 file successfully created and stored.']);
                    end
                    
                    clear outputFilename outputFilesize OpenDAPncData;
                    
                else
                    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> Something went wrong in reading ' network_data{1,network_idIndex} ' data via OpenDAP.']);
                end
            end
            % Process networks having standard input files
        else
            % Retrieve the number of totals to be converted for the current network
            try
                if(exist('toBeConvertedTotals_data','var')==1)
                    numToBeConvertedTotals = size(toBeConvertedTotals_data,1);
                    disp(['[' datestr(now) '] - - ' 'Number of total files from ' network_data{network_idx,network_idIndex} ' network to be converted successfully retrieved.']);
                else
                    clear outputFilename outputFilesize
                    return
                end
            catch err
                disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
                TC_err = 1;
            end
            
            try
                % Find the index of the filename field
                filenameIndexC = strfind(toBeConvertedTotals_columnNames, 'filename');
                filenameIndex = find(not(cellfun('isempty', filenameIndexC)));
                
                % Find the index of the filepath field
                filepathIndexC = strfind(toBeConvertedTotals_columnNames, 'filepath');
                filepathIndex = find(not(cellfun('isempty', filepathIndexC)));
                
                % Find the index of the extension field
                extensionIndexC = strfind(toBeConvertedTotals_columnNames, 'extension');
                extensionIndex = find(not(cellfun('isempty', extensionIndexC)));
                
                % Find the index of the timestamp field
                timestampIndexC = strfind(toBeConvertedTotals_columnNames, 'timestamp');
                timestampIndex = find(not(cellfun('isempty', timestampIndexC)));
            catch err
                disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
                TC_err = 1;
            end
            
            % Scan the tuv files to be converted
            for toBeConverted_idx=1:numToBeConvertedTotals
                TC_err = 0;
                try
                    if (strcmp(toBeConvertedTotals_data{toBeConverted_idx,extensionIndex}, '.tuv')) % Codar data
                        % v2.2
                        [TC_err, network_data(network_idx,:), outputFilename, outputFilesize] = tuv2netCDF_v22([toBeConvertedTotals_data{toBeConverted_idx,filepathIndex} filesep toBeConvertedTotals_data{toBeConverted_idx,filenameIndex}],toBeConvertedTotals_data{toBeConverted_idx,timestampIndex},network_data(network_idx,:),network_columnNames,station_data,station_columnNames);
                    elseif (strcmp(toBeConvertedTotals_data{toBeConverted_idx,extensionIndex}, '.cur_asc')) % WERA data
                        % v2.2
                        [TC_err, network_data(network_idx,:), outputFilename, outputFilesize] = curAsc2netCDF_v22([toBeConvertedTotals_data{toBeConverted_idx,filepathIndex} filesep toBeConvertedTotals_data{toBeConverted_idx,filenameIndex}],toBeConvertedTotals_data{toBeConverted_idx,timestampIndex},network_data(network_idx,:),network_columnNames,station_data,station_columnNames);
                    elseif (strcmp(toBeConvertedTotals_data{toBeConverted_idx,extensionIndex}, '.asc')) % WERA data
                        % v2.2
                        [TC_err, network_data(network_idx,:), outputFilename, outputFilesize] = ascTot2netCDF_v22([toBeConvertedTotals_data{toBeConverted_idx,filepathIndex} filesep toBeConvertedTotals_data{toBeConverted_idx,filenameIndex}],toBeConvertedTotals_data{toBeConverted_idx,timestampIndex},network_data(network_idx,:),network_columnNames,station_data,station_columnNames);
                    end
                    if(TC_err==0)
                        disp(['[' datestr(now) '] - - ' outputFilename ' total netCDF v2.2 file successfully created and stored.']);
                    end
                catch err
                    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
                    TC_err = 1;
                end
                
                clear outputFilename outputFilesize;
                
            end
        end
    end
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    TC_err = 1;
end

%%

%% Close connection

try
    close(conn);
    disp(['[' datestr(now) '] - - ' 'Connection to database successfully closed.']);
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    TC_err = 1;
end

%%

if(TC_err==0)
    disp(['[' datestr(now) '] - - ' 'H_TotalConversion successfully executed.']);
end

pause(10);
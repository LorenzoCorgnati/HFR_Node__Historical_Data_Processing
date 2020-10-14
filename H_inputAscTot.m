%% H_inputAscTot.m
% This application lists the input asc total (Shom customized WERA totals)
% files pushed by the HFR data providers and inserts into a proper structure
% the information needed for the conversion of the total data files into
% the European standard data model.

% This application works on historical data.

% Author: Lorenzo Corgnati
% Date: November 9, 2019

% E-mail: lorenzo.corgnati@sp.ismar.cnr.it
%%

warning('off', 'all');

iCurDB_err = 0;

disp(['[' datestr(now) '] - - ' 'H_inputAscTot started.']);

startDateNum = datenum(startDate);
endDateNum = datenum(endDate);

%%

%% Connect to database

try
    conn = database(sqlConfig.database,sqlConfig.user,sqlConfig.password,'Vendor','MySQL','Server',sqlConfig.host);
    disp(['[' datestr(now) '] - - ' 'Connection to database successfully established.']);
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    iCurDB_err = 1;
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


%% Scan the networks, list the related total files and insert information into the database

try
    % Find the index of the network_id field
    network_idIndexC = strfind(network_columnNames, 'network_id');
    network_idIndex = find(not(cellfun('isempty', network_idIndexC)));
    
    % Find the index of the input file path field
    inputPathIndexC = strfind(network_columnNames, 'total_input_folder_path');
    inputPathIndex = find(not(cellfun('isempty', inputPathIndexC)));
    
    % Find the index of the output file path field
    outputPathIndexC = strfind(network_columnNames, 'total_HFRnetCDF_folder_path');
    outputPathIndex = find(not(cellfun('isempty', outputPathIndexC)));
    
    % Find the index of the mat file path field
    matPathIndexC = strfind(network_columnNames, 'total_mat_folder_path');
    matPathIndex = find(not(cellfun('isempty', matPathIndexC)));
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    iCurDB_err = 1;
end

% Scan the networks
try
    for network_idx=1:numNetworks
        
        % Override data folder paths for stations
        network_data{network_idx,inputPathIndex} = ['../' networkID filesep 'Totals_asc'];
        network_data{network_idx,outputPathIndex} = ['../' networkID filesep 'Totals_nc'];
        network_data{network_idx,matPathIndex} = ['../' networkID filesep 'Totals_mat'];
        
        iCurDB_err = 0;
        if(~isempty(network_data{network_idx,inputPathIndex}))
            % Trim heading and trailing whitespaces from folder path
            network_data{network_idx,inputPathIndex} = strtrim(network_data{network_idx,inputPathIndex});
            % List the input cur_asc files
            try
                ascFiles = rdir([network_data{network_idx,inputPathIndex} filesep '**' filesep '*.asc']);
                disp(['[' datestr(now) '] - - ' 'Total files from ' network_data{network_idx,network_idIndex} ' network successfully listed.']);
            catch err
                disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
                iCurDB_err = 1;
            end
            
            % Insert information about the cur_asc file into the data structure
            for asc_idx=1:length(ascFiles)
                iCurDB_err = 0;
                % Retrieve the filename
                [pathstr,name,ext]=fileparts(ascFiles(asc_idx).name);
                noFullPathName=[name ext];
                % Retrieve information about the cur_asc file
                try
                    disp(['Processing filename: ' ascFiles(asc_idx).name]);
                    % Read the timestamp from the header
                    [dateY, dateM, dateD, timeH, timeM, timeS] = textread(ascFiles(asc_idx).name, '%4d %*0c %2d %*0c %2d %*0c %2d %*0c %2d %*0c %2d',1, 'headerlines',2);
                    TimeStampVec = [dateY dateM dateD timeH timeM timeS];
                    TimeStamp = [num2str(TimeStampVec(1)) ' ' num2str(TimeStampVec(2),'%02d') ' ' num2str(TimeStampVec(3),'%02d') ' ' num2str(TimeStampVec(4),'%02d') ' ' num2str(TimeStampVec(5),'%02d') ' ' num2str(TimeStampVec(6),'%02d')];
                catch err
                    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
                    iCurDB_err = 1;
                end
                
                % Evaluate datetime from, Time Stamp
                try
                    [t2d_err,DateTime] = timestamp2datetime(TimeStamp);
                catch err
                    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
                    iCurDB_err = 1;
                end
                
                % Check if the current file belongs to the processing time interval
                if((datenum(DateTime) >= startDateNum) && (datenum(DateTime) < endDateNum))
                    
                    % Retrieve information about the cur_asc file
                    try
                        ascFileInfo = dir(ascFiles(asc_idx).name);
                        ascFilesize = ascFileInfo.bytes/1024;
                    catch err
                        disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
                        iCurDB_err = 1;
                    end
                    
                    % Define a cell array that contains the data for insertion
                    tBCT_idx = tBCT_idx + 1;
                    toBeConvertedTotals_data(tBCT_idx,:) = {noFullPathName,pathstr,network_data{network_idx,network_idIndex},TimeStamp,DateTime,(datestr(now,'yyyy-mm-dd HH:MM:SS')),ascFilesize,ext,0};
                    
                end
            end
        end
    end
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    iCurDB_err = 1;
end

%%

%% Close connection

try
    close(conn);
    disp(['[' datestr(now) '] - - ' 'Connection to database successfully closed.']);
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    iCurDB_err = 1;
end

%%

if(iCurDB_err==0)
    disp(['[' datestr(now) '] - - ' 'H_inputAscTot successfully executed.']);
end
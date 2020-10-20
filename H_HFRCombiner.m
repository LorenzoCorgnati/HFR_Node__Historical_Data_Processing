%% H_HFRCombiner.m
% This application reads the HFR database for collecting information about
% the radial data files to be combined into totals and performs the
% combination and the generation of radial and total data files into the
% European standard data model.

% This application works on historical data.

% Author: Lorenzo Corgnati
% Date: November 9, 2019

% E-mail: lorenzo.corgnati@sp.ismar.cnr.it
%%

warning('off', 'all');

HFRC_err = 0;

disp(['[' datestr(now) '] - - ' 'H_HFRCombiner started.']);

%%

%% Connect to database

try
    conn = database(sqlConfig.database,sqlConfig.user,sqlConfig.password,'Vendor','MySQL','Server',sqlConfig.host);
    disp(['[' datestr(now) '] - - ' 'Connection to database successfully established.']);
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    HFRC_err = 1;
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

%% Scan the networks and combine the related radial files

try
    % Find the index of the network_id field
    network_idIndexC = strfind(network_columnNames, 'network_id');
    network_idIndex = find(not(cellfun('isempty', network_idIndexC)));
    
    % Find the indices of the geospatial boundaries fields
    geospatial_lat_minIndex = find(not(cellfun('isempty', strfind(network_columnNames, 'geospatial_lat_min'))));
    geospatial_lat_maxIndex = find(not(cellfun('isempty', strfind(network_columnNames, 'geospatial_lat_max'))));
    geospatial_lon_minIndex = find(not(cellfun('isempty', strfind(network_columnNames, 'geospatial_lon_min'))));
    geospatial_lon_maxIndex = find(not(cellfun('isempty', strfind(network_columnNames, 'geospatial_lon_max'))));
    
    % Find the index of the grid resolution field
    grid_resolutionIndex = find(not(cellfun('isempty', strfind(network_columnNames, 'grid_resolution'))));
    
    % Find the index of the of the combination search radius field
    spatthreshIndex = find(not(cellfun('isempty', strfind(network_columnNames, 'combination_search_radius'))));
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    HFRC_err = 1;
end

try
    % Scan the networks
    for network_idx=1:numNetworks
        HFRC_err = 0;
        
        % Find the index of the output file path field
        ToutputPathIndexC = strfind(network_columnNames, 'total_HFRnetCDF_folder_path');
        ToutputPathIndex = find(not(cellfun('isempty', ToutputPathIndexC)));
        
        % Find the index of the mat file path field
        matPathIndexC = strfind(network_columnNames, 'total_mat_folder_path');
        matPathIndex = find(not(cellfun('isempty', matPathIndexC)));
        
        % Override data folder paths for stations
        network_data{network_idx,ToutputPathIndex} = ['../' networkID filesep 'Totals_nc'];
        network_data{network_idx,matPathIndex} = ['../' networkID filesep 'Totals_mat'];
        
        % Build the regular LonLat grid given the geographical boundaries and the grid resolution for the radial combination into total
        [gridLon, gridLat] = LonLat_grid([network_data{network_idx,geospatial_lon_minIndex},network_data{network_idx,geospatial_lat_minIndex}], [network_data{network_idx,geospatial_lon_maxIndex},network_data{network_idx,geospatial_lat_maxIndex}], network_data{network_idx,grid_resolutionIndex}, 'km');
        gridLat = flipud(gridLat);
        lon = gridLon(1,:);
        lat = gridLat(:,1);
        length_lon=length(lon);
        length_lat=length(lat);
        for i=1:length_lon
            lonG(1+(i-1)*length_lat:(i-1)*length_lat+length_lat) = lon(i)*ones(1,length_lat);
            latG(1+(i-1)*length_lat:(i-1)*length_lat+length_lat) = lat;
        end
        Grid(:,1) = lonG';
        Grid(:,2) = latG';
        
        disp(['[' datestr(now) '] - - ' 'Grid for radial combination for ' network_data{network_idx, network_idIndex} ' network successfully generated.']);
        
        % Build the mask for the total masking
        try
            if (exist(['Total_Masks' filesep network_data{network_idx,network_idIndex}], 'dir') ~= 7)
                mkdir(['Total_Masks' filesep network_data{network_idx,network_idIndex}]);
            end
        catch err
            disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
            HFRC_err = 1;
        end
        try
            fname = makeCoast([network_data{network_idx,geospatial_lon_minIndex},network_data{network_idx,geospatial_lon_maxIndex}],[network_data{network_idx,geospatial_lat_minIndex},network_data{network_idx,geospatial_lat_maxIndex}],'transverse mercator',['Total_Masks' filesep network_data{network_idx,network_idIndex} filesep network_data{network_idx,network_idIndex} '_MaskMap.mat'],5);
            load(['Total_Masks' filesep network_data{network_idx,network_idIndex} filesep network_data{network_idx,network_idIndex} '_MaskMap.mat']);
            disp(['[' datestr(now) '] - - ' 'Mask area for ' network_data{network_idx, network_idIndex} ' network successfully generated.']);
        catch err
            disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
            HFRC_err = 1;
        end
        
        % Retrieve the stations belonging to the current network
        try
            % Manage the case of the ISMAR-LaMMA integrated network (HFR-WesternItaly)
            if(strcmp(network_data{network_idx,network_idIndex},'HFR-WesternItaly'))
                station_selectquery = ['SELECT * FROM station_tb WHERE network_id = ' '''HFR-TirLig'' OR network_id = ' '''HFR-LaMMA'''];
            else
                station_selectquery = ['SELECT * FROM station_tb WHERE network_id = ' '''' network_data{network_idx,network_idIndex} ''''];
            end
            station_curs = exec(conn,station_selectquery);
            disp(['[' datestr(now) '] - - ' 'Query to station_tb table for retrieving the stations of the ' network_data{network_idx,network_idIndex} ' network successfully executed.']);
        catch err
            disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
            HFRC_err = 1;
        end
        
        % Fetch data
        try
            station_curs = fetch(station_curs);
            station_data = station_curs.Data;
            disp(['[' datestr(now) '] - - ' 'Data of the stations of the ' network_data{network_idx,network_idIndex} ' network successfully fetched from station_tb table.']);
        catch err
            disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
            HFRC_err = 1;
        end
        
        % Retrieve column names
        try
            station_columnNames = columnnames(station_curs,true);
            disp(['[' datestr(now) '] - - ' 'Column names from station_tb table successfully retrieved.']);
        catch err
            disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
            HFRC_err = 1;
        end
        
        % Retrieve the number of stations belonging to the current network
        try
            numStations = rows(station_curs);
            disp(['[' datestr(now) '] - - ' 'Number of stations belonging to the ' network_data{network_idx,network_idIndex} ' network successfully retrieved from station_tb table.']);
        catch err
            disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
            HFRC_err = 1;
        end
        
        % Close cursor to station_tb table
        try
            close(station_curs);
            disp(['[' datestr(now) '] - - ' 'Cursor to station_tb table successfully closed.']);
        catch err
            disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
            HFRC_err = 1;
        end
        
        try
            % Find the index of the input file path field
            inputPathIndexC = strfind(station_columnNames, 'radial_input_folder_path');
            inputPathIndex = find(not(cellfun('isempty', inputPathIndexC)));
            
            % Find the index of the output file path field
            RoutputPathIndexC = strfind(station_columnNames, 'radial_HFRnetCDF_folder_path');
            RoutputPathIndex = find(not(cellfun('isempty', RoutputPathIndexC)));
            
            % Find the index of the station_id field in the station_tb table
            STstation_idIndexC = strfind(station_columnNames, 'station_id');
            STstation_idIndex = find(not(cellfun('isempty', STstation_idIndexC)));
            
            % Find the index of the last calibration date field
            last_calibration_dateIndexC = strfind(station_columnNames, 'last_calibration_date');
            last_calibration_dateIndex = find(not(cellfun('isempty', last_calibration_dateIndexC)));
            
            % Find the index of the end of operation date field
            operational_toIndexC = strfind(station_columnNames, 'operational_to');
            operational_toIndex = find(not(cellfun('isempty', operational_toIndexC)));
        catch err
            disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
            HFRC_err = 1;
        end
        
        % Override data folder paths for stations
        try
            for station_idx=1:numStations
                station_data{station_idx,RoutputPathIndex} = ['../' networkID filesep 'Radials_nc'];
            end
        catch err
            disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
            HFRC_err = 1;
        end
        
        % Retrieve the number of operational stations
        try
            numActiveStations = numStations;
            for station_idx=1:numStations
                if(size(station_data{station_idx,operational_toIndex},2)~=4)
                    numActiveStations = numActiveStations - 1;
                end
            end
            if(numActiveStations == 0)
                numActiveStations = numStations;
            end
        catch err
            disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
            HFRC_err = 1;
        end
        
        % Retrieve the number of radials to be combined
        try
            if(exist('toBeCombinedRadials_data','var')==1)
                numToBeCombinedRadials = size(toBeCombinedRadials_data,1);
                disp(['[' datestr(now) '] - - ' 'Number of the radial files from ' network_data{network_idx,network_idIndex} ' network to be combined successfully retrieved.']);
            else
                clear Grid gridLon gridLat lonG latG lon lat
                return
            end
        catch err
            disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
            HFRC_err = 1;
        end
        
        try
            % Find the index of the time stamp field
            timeStampIndexC = strfind(toBeCombinedRadials_columnNames, 'timestamp');
            timeStampIndex = find(not(cellfun('isempty', timeStampIndexC)));
            
            % Find the index of the filename field
            filenameIndexC = strfind(toBeCombinedRadials_columnNames, 'filename');
            filenameIndex = find(not(cellfun('isempty', filenameIndexC)));
            
            % Find the index of the filepath field
            filepathIndexC = strfind(toBeCombinedRadials_columnNames, 'filepath');
            filepathIndex = find(not(cellfun('isempty', filepathIndexC)));
            
            % Find the index of the station_id field in the radial_input_tb table
            RIstation_idIndexC = strfind(toBeCombinedRadials_columnNames, 'station_id');
            RIstation_idIndex = find(not(cellfun('isempty', RIstation_idIndexC)));
            
            % Find the index of the extension field
            extensionIndexC = strfind(toBeCombinedRadials_columnNames, 'extension');
            extensionIndex = find(not(cellfun('isempty', extensionIndexC)));
            
            % Find the index of the NRT_processed_flag field
            NRT_processed_flagIndex = strmatch('NRT_processed_flag',toBeCombinedRadials_columnNames,'exact');
        catch err
            disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
            HFRC_err = 1;
        end
        
        try
            % Scan the radials to be combined, group them by timestamp and combine them
            for radial_idx=1:numToBeCombinedRadials
                HFRC_err = 0;
                if(toBeCombinedRadials_data{radial_idx,NRT_processed_flagIndex} == 0)
                    % Find the indices of the radial files of the current timestamp to be combined
                    toBeCombinedRadialIndicesC = strfind(toBeCombinedRadials_data(:,timeStampIndex), toBeCombinedRadials_data{radial_idx,timeStampIndex});
                    toBeCombinedRadialIndices = find(not(cellfun('isempty', toBeCombinedRadialIndicesC)));
                    try
                        % Build the radial file paths
                        for indices_idx=1:length(toBeCombinedRadialIndices)
                            toBeCombinedStationIndexC = strfind(station_data(:,STstation_idIndex), toBeCombinedRadials_data{toBeCombinedRadialIndices(indices_idx),RIstation_idIndex});
                            toBeCombinedStationIndex = find(not(cellfun('isempty', toBeCombinedStationIndexC)));
                            station_data{toBeCombinedStationIndex,inputPathIndex} = strtrim(station_data{toBeCombinedStationIndex,inputPathIndex});
                            radFiles(indices_idx) = {[toBeCombinedRadials_data{toBeCombinedRadialIndices(indices_idx),filepathIndex} filesep toBeCombinedRadials_data{toBeCombinedRadialIndices(indices_idx),filenameIndex}]};
                        end
                    catch err
                        display(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
                        HFRC_err = 1;
                    end
                    
                    try
                        % Load the radial files to be combined
                        if (strcmp(toBeCombinedRadials_data{toBeCombinedRadialIndices(indices_idx),extensionIndex}, '.ruv')) % Codar data
                            disp(['[' datestr(now) '] - - ' 'loadRDLfile loading ...']);
                            RADIAL = loadRDLFile(radFiles, 'false', 'warning');
                            %                         elseif(strcmp(toBeCombinedRadials_data{toBeCombinedRadialIndices(indices_idx),extensionIndex}, '.mat')) % MetNo data
                            %                             for mat_idx=1:length(radFiles)
                            %                                 load(radFiles{mat_idx});
                            %                                 RADIAL(mat_idx) = ruvRad;
                            %                                 clear ruvRad
                            %                             end
                        elseif(strcmp(toBeCombinedRadials_data{toBeCombinedRadialIndices(indices_idx),extensionIndex}, '.crad_ascii')) % WERA data
                            % NOTHING TO DO
                        end
                    catch err
                        display(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
                        HFRC_err = 1;
                    end
                    
                    % Convert the radial files into netCDF according to the European standard data model
                    for ruv_idx=1:length(toBeCombinedRadialIndices)
                        toBeCombinedStationIndexC = strfind(station_data(:,STstation_idIndex), toBeCombinedRadials_data{toBeCombinedRadialIndices(ruv_idx),RIstation_idIndex});
                        toBeCombinedStationIndex = find(not(cellfun('isempty', toBeCombinedStationIndexC)));
                        try
                            if (strcmp(toBeCombinedRadials_data{toBeCombinedRadialIndices(indices_idx),extensionIndex}, '.ruv')) % Codar data
                                if(~strcmp(network_data{1,network_idIndex},'HFR-WesternItaly'))
                                    % v2.2
                                    [R2C_err,network_data(network_idx,:),station_data(toBeCombinedStationIndex,:),radOutputFilename,radOutputFilesize,station_tbUpdateFlag] = ruv2netCDF_v22(RADIAL(ruv_idx),network_data(network_idx,:),network_columnNames,station_data(toBeCombinedStationIndex,:),station_columnNames,toBeCombinedRadials_data{toBeCombinedRadialIndices(indices_idx),timeStampIndex});
                                    disp(['[' datestr(now) '] - - ' radOutputFilename ' radial netCDF v2.2 file successfully created and stored.']);
                                else
                                    station_tbUpdateFlag = 0;
                                end
                                %                             elseif (strcmp(toBeCombinedRadials_data{toBeCombinedRadialIndices(indices_idx),extensionIndex}, '.mat')) % MetNo data
                                %                                 % v2.2
                                %                                 [R2C_err,network_data(network_idx,:),station_data(toBeCombinedStationIndex,:),radOutputFilename,radOutputFilesize,station_tbUpdateFlag] = mat2netCDF_v22(RADIAL(ruv_idx),network_data(network_idx,:),network_columnNames,station_data(toBeCombinedStationIndex,:),station_columnNames,toBeCombinedRadials_data{toBeCombinedRadialIndices(indices_idx),timeStampIndex});
                                %                                 disp(['[' datestr(now) '] - - ' radOutputFilename ' radial netCDF v2.2 file successfully created and stored.']);
                                %                                 contrSitesIndices(ruv_idx) = toBeCombinedStationIndex;
                            elseif (strcmp(toBeCombinedRadials_data{toBeCombinedRadialIndices(indices_idx),extensionIndex}, '.crad_ascii')) % WERA data
                                [R2C_err,network_data(network_idx,:),radOutputFilename,radOutputFilesize] = cradAscii2netCDF_v22(radFiles{ruv_idx},network_data(network_idx,:),network_columnNames,station_data(toBeCombinedStationIndex,:),station_columnNames,toBeCombinedRadials_data{toBeCombinedRadialIndices(indices_idx),timeStampIndex});
                                disp(['[' datestr(now) '] - - ' radOutputFilename ' radial netCDF v2.2 file successfully created and stored.']);
                                station_tbUpdateFlag = 0; % WERA radial files do not contain information about calibration
                                numActiveStations = length(toBeCombinedRadialIndices); % WERA radials are not combined
                            end
                            contrSitesIndices(ruv_idx) = toBeCombinedStationIndex;
                        catch err
                            display(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
                            HFRC_err = 2;
                        end
                        
                    end
                    
                    % Combine the Codar radial files into total
                    if((size(radFiles,2)>1) && (HFRC_err~=2))
                        % Check if all the radials have the same extension
                        for ext_idx=1:length(toBeCombinedRadialIndices)
                            extensions{ext_idx} = toBeCombinedRadials_data{toBeCombinedRadialIndices(ext_idx),extensionIndex};
                        end
                        extensions = uniqueStrCell(extensions);
                        % Check the extension
                        if ((length(extensions)==1) && (strcmp(extensions, '.ruv'))) % Codar data
                            try
                                disp(['[' datestr(now) '] - - ' 'makeTotals combining radials...']);
                                [TUV,R] = makeTotals(RADIAL, 'Grid', Grid, 'TimeStamp', RADIAL(1,1).TimeStamp, 'spatthresh', network_data{network_idx,spatthreshIndex}, 'tempthresh', 1/24);
                                % Totals setting on a regular grid
                                [TUVgrid,DIM,I] = gridTotals( TUV, 'true', 'true');
                                % Totals masking
                                [TUVmask,I] = maskTotals(TUVgrid,ncst,0);
                            catch err
                                disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
                                HFRC_err = 1;
                            end
                            
                            % Save the total mat file
                            try
                                ts = datevec(TUVmask.TimeStamp);
                                time_str = sprintf('%.4d_%.2d_%.2d_%.2d%.2d',ts(1,1),ts(1,2),ts(1,3),ts(1,4),ts(1,5));
                                network_data{network_idx,matPathIndex} = strtrim(network_data{network_idx,matPathIndex});
                                % v2.2
                                [tFB_err, matFilePath] = totalFolderBuilder_v22(network_data{network_idx,matPathIndex}, toBeCombinedRadials_data{radial_idx,timeStampIndex});
                                save([matFilePath filesep network_data{network_idx,network_idIndex} '-Total_' time_str '.mat'], 'TUVmask');
                                disp(['[' datestr(now) '] - - ' network_data{network_idx,network_idIndex} '-Total_' time_str '.mat' ' file successfully saved.']);
                            catch err
                                disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
                                HFRC_err = 1;
                            end
                            
                            % Plot the current map for HFR-TirLig network
                            try
                                if(strcmp(network_data{1,network_idIndex},'HFR-TirLig'))
                                    % Totals cleaning for GDOP
                                    gdop_sP = sqrt(6.25);
                                    maxspd_sP = 500;
                                    [TUVclean,I] = cleanTotals(TUVmask,maxspd_sP,{'GDOPMaxOrthog','TotalErrors',gdop_sP});
                                    % Plot
                                    shadePlot_TirLig;
                                    % Save the map file
                                    saveas(gcf,['/home/radarcombine/EU_HFR_NODE/HFR_TirLig/Totals_map/' time_str '.jpg']);
                                    close
                                    disp(['[' datestr(now) '] - - ' time_str ' map for HFR-TirLig network successfully saved.']);
                                end
                            catch err
                                disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
                                HFRC_err = 1;
                            end
                            
                            % Plot the current map for HFR-WesternItaly network
                            try
                                if(strcmp(network_data{1,network_idIndex},'HFR-WesternItaly'))
                                    % Totals cleaning for GDOP
                                    gdop_sP = sqrt(6.25);
                                    maxspd_sP = 500;
                                    [TUVclean,I] = cleanTotals(TUVmask,maxspd_sP,{'GDOPMaxOrthog','TotalErrors',gdop_sP});
                                    % Plot
                                    shadePlot_WesternItaly;
                                    % Save the map file
                                    saveas(gcf,['/home/radarcombine/EU_HFR_NODE/HFR_WesternItaly/Totals_map/' time_str '.jpg']);
                                    close
                                    disp(['[' datestr(now) '] - - ' time_str ' map for HFR-TirLig network successfully saved.']);
                                end
                            catch err
                                disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
                                HFRC_err = 1;
                            end
                            
                        end
                        
                        % Create the total netCDF file according to the European standard data model
                        if (strcmp(extensions, '.ruv')) % Codar data
                            % v2.2
                            [T2C_err,network_data(network_idx,:),station_data(contrSitesIndices,:),totOutputFilename,totOutputFilesize] = tot2netCDF_v22(TUVmask,network_data(network_idx,:),network_columnNames,station_data(contrSitesIndices,:),station_columnNames,toBeCombinedRadials_data{radial_idx,timeStampIndex},station_data);
                            disp(['[' datestr(now) '] - - ' totOutputFilename ' total netCDF v2.2 file successfully created and stored.']);
                        elseif (strcmp(toBeCombinedRadials_data{toBeCombinedStationIndex,extensionIndex}, 'crad_ascii')) % WERA data
                            % NOTHING TO DO
                        end
                        
                    end
                    
                    % Update NRT_processed_flag in the local radial table
                    try
                        if(HFRC_err==0)
                            toBeCombinedRadials_data(toBeCombinedRadialIndices,NRT_processed_flagIndex)={1};
                        end
                    catch err
                        disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
                        HFRC_err = 1;
                    end
                    
                    clear radFiles RADIAL contrSitesIndices TUV TUVgrid TUVmask radOutputFilename totOutputFilename;
                    
                end
            end
        catch err
            disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
            HFRC_err = 1;
        end
        
        clear Grid gridLon gridLat lonG latG lon lat;
        
    end
    
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    HFRC_err = 1;
end

%%

%% Close connection

try
    close(conn);
    disp(['[' datestr(now) '] - - ' 'Connection to database successfully closed.']);
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    HFRC_err = 1;
end

%%

if(HFRC_err==0)
    disp(['[' datestr(now) '] - - ' 'H_HFRCombiner successfully executed.']);
end

pause(10);
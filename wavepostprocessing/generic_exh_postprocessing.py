############################################################################################################
# This file handles wave and pampro output for generic waveform output
# Author: CAS
# Date: 15/11/2024
# Version: 1.1. Added sections to be able to run on Pampro output
# Version: 1.0 Translated from Stata code
############################################################################################################
# Importing packages
import numpy as np
#import config
import os
import pandas as pd
import pytz
from datetime import datetime, timedelta
from colorama import Fore
from wavepostprocessing.config import load_config
#from config import load_config
import sys

# READING IN FILELIST
def reading_filelist(id=''):
    os.chdir(os.path.join(config.get('root_folder'), config.get('results_folder'), config.get('filelist_folder')))
    filelist_df = pd.read_csv('filelist' + id + '.txt', delimiter='\t')  # Reading in the filelist
    filelist_df = filelist_df.drop_duplicates(subset=['filename_temp'])
    files_list = filelist_df['filename_temp'].tolist()

    return files_list


# READING METADATA
def reading_metadata(files_list):
    metadata_dfs = []

    for file_id in files_list:
        metadata_file_path = os.path.join(config.get('root_folder'), config.get('results_folder'), f"metadata_{file_id}.csv")

        if os.path.exists(metadata_file_path):
            metadata_df = pd.read_csv(metadata_file_path)
            metadata_df['file_id'] = file_id

            columns_to_keep = ['file_id', 'subject_code', 'device', 'calibration_method', 'noise_cutoff_mg', 'processing_epoch', 'generic_first_timestamp', 'generic_last_timestamp', 'QC_first_battery_pct', 'QC_last_battery_pct', 'frequency']
            if config.get('processing').lower() == 'wave':
                extra_columns = ['start_error', 'end_error', 'QC_anomalies_total', 'QC_anomaly_A', 'QC_anomaly_B', 'QC_anomaly_C', 'QC_anomaly_D', 'QC_anomaly_E', 'QC_anomaly_F', 'QC_anomaly_G', 'processing_script']
            if config.get('processing').lower() == "pampro":
                extra_columns = ['file_start_error', 'file_end_error', 'days_of_data_processed', 'mf_start_error', 'mf_end_error', 'calibration_type', 'QC_axis_anomaly']
            columns_to_keep.extend(extra_columns)
            metadata_df = metadata_df.reindex(columns=columns_to_keep)

            metadata_dfs.append(metadata_df)

        else:
            print(f"Metadata for file ID: {file_id} not found")
            return

    return metadata_dfs


# READING DATA FILE
def reading_datafile(files_list):
    datafiles_dfs = []

    for file_id in files_list:
        datafile_path = os.path.join(config.get('root_folder'), config.get('results_folder'), f"{config.get('count_prefixes')}_{file_id}.csv")

        if os.path.exists(datafile_path):
            datafile_df = pd.read_csv(datafile_path)
            datafile_df['file_id'] = file_id
            datafile_df.rename(columns={'id': 'database_id'}, inplace=True)
            datafile_df.columns = [col[:-6] + "plus" if col.endswith("_99999") else col for col in datafile_df.columns]
            datafile_df.columns = [col.replace("-", "") if col.lower().startswith("pitch") or col.lower().startswith("roll") else col for col in datafile_df.columns]
            datafile_df = datafile_df.drop(columns=[col for col in datafile_df.columns if any(var in col for var in config.get('variables_to_drop'))])
            datafiles_dfs.append(datafile_df)
        else:
            print(f"Data file for ID: {file_id} not found")
            return

    return datafiles_dfs

# READING ANOMALIES FILE
def anomalies():
    anomaly_file_path = os.path.join(config.get('root_folder'), config.get('anomalies_folder'), config.get('anomalies_file'))
    if os.path.exists(anomaly_file_path):
        anomalies_df = pd.read_csv(anomaly_file_path)
        return anomalies_df
    else:
        return pd.DataFrame()

# MERGING METADATA FILE AND DATA FILE, THEN MERGING ON ANOMALIES FILE AND FORMATTING MERGED DATAFRAME
def merging_data(files_list, metadata_dfs, datafiles_dfs, anomalies_df):
    merged_dfs = []
    time_resolutions = []

    for metadata_df, datafile_df, file_id in zip(metadata_dfs, datafiles_dfs, files_list):
        merged_df = pd.merge(datafile_df, metadata_df, on='file_id', how='left')
        columns = merged_df.columns.tolist()
        columns.insert(0, columns.pop(columns.index('file_id')))
        merged_df = merged_df[columns]

        # Merged on anomaly information
        if config.get('processing').lower() == 'pampro':
            if anomalies_df is not None and not anomalies_df.empty:
                merged_df = pd.merge(merged_df, anomalies_df[['file_id', 'Anom_A', 'Anom_B', 'Anom_C', 'Anom_D', 'Anom_E', 'Anom_F']], on='file_id', how='left')
            else:
                anomaly_columns = ['Anom_A', 'Anom_B', 'Anom_C', 'Anom_D', 'Anom_E', 'Anom_F']
                merged_df[anomaly_columns] = np.nan

        # Reformat timestamp:
        merged_df['timestamp'] = merged_df['timestamp'].str.replace(':000000', '')

        # Create DATETIME variable. This is the monitor time. Not adjusted for BST
        merged_df['DATETIME_ORIG'] = pd.to_datetime(merged_df['timestamp'], format='%d/%m/%Y %H:%M:%S')

        # Changing order of columns and sorting the data
        columns = merged_df.columns.tolist()
        columns.insert(1, columns.pop(columns.index('timestamp')))
        columns.insert(2, columns.pop(columns.index('DATETIME_ORIG')))
        merged_df = merged_df[columns]
        merged_df = merged_df.sort_values(by=['file_id', 'DATETIME_ORIG'])

        time_difference = merged_df['DATETIME_ORIG'].iloc[1] - merged_df['DATETIME_ORIG'].iloc[0]
        time_resolution = time_difference.total_seconds()/60
        time_resolutions.append(time_resolution)

        # Formatting the generic timestamps and creating first and last file_timestamps
        generic_timestamps = ['generic_first_timestamp', 'generic_last_timestamp']
        merged_df[generic_timestamps] = merged_df[generic_timestamps].apply(lambda x: x.str[:19])
        file_timepoints = ['first_file_timepoint', 'last_file_timepoint']
        merged_df[file_timepoints] = merged_df[generic_timestamps]
        for variable in file_timepoints:
            merged_df[variable] = pd.to_datetime(merged_df[variable], format='%d/%m/%Y %H:%M:%S')

        # Generating DATETIME variable and correcting for daylight saving. For no clock change this time stays the same as DATETIME_ORIG
        merged_df['DATETIME'] = merged_df['DATETIME_ORIG']

        if config.get('clock_changes').lower() == 'yes':
            merged_df['DATETIME_COPY'] = merged_df['DATETIME']

            #Retrieving timezone information and converting datetime_copy variable to specified timezone
            tz = pytz.timezone(config.get('timezone'))
            merged_df['DATETIME_COPY'] = merged_df['DATETIME_ORIG'].dt.tz_localize('UTC').dt.tz_convert(tz)
            merged_df['BST'] = merged_df['DATETIME_COPY'].apply(lambda x: x.dst() != timedelta(0))

            # Creating bst variables, to check if there is a change through the data from from summer time to winter time or other way
            prev_bst = None

            bst_values = merged_df['BST'].unique()

            if len(bst_values) == 2 and True in bst_values and False in bst_values:
                adjustment = 0
                for idx in range(len(merged_df)):
                    curr_bst = merged_df.at[idx, 'BST']

                    # If dataset goes over clock change, adding or subtracting 1 hour from DATETIME variable
                    if prev_bst is not None and prev_bst != curr_bst:
                        if not prev_bst and curr_bst:

                            adjustment += 1
                            print(f"Transition from winter to summer time detected at 1am for the file id: {file_id}")

                        elif prev_bst and not curr_bst:

                             adjustment -= 1
                             print(f"Transition from summer to winter time detected at 1am for the file id: {file_id}")
                    # OBS! IT IS CHANGING THE CLOCK AT 1AM BOTH TIMES, FIND A WAY TO MAKE IT CHANGE AT 2AM FROM SUMMER TO WINTER TIME.
                    merged_df.at[idx, 'DATETIME'] += timedelta(hours=adjustment)
                    prev_bst = curr_bst
                print("Date and time variables have been adjusted for clock changes.")
                merged_df.drop(columns=['DATETIME_COPY', 'BST'], inplace=True)

            else:
                merged_df.drop(columns=['DATETIME_COPY', 'BST'], inplace=True)
                pass

        # Calculating DATE and TIME variables with new time
        merged_df['DATE'] = pd.to_datetime(merged_df['DATETIME']).dt.date
        merged_df['TIME'] = pd.to_datetime(merged_df['DATETIME']).dt.time
        merged_df['hourofday'] = pd.to_datetime(merged_df['DATETIME']).dt.hour + 1
        merged_df['dayofweek'] = merged_df['DATETIME'].apply(lambda x: x.isoweekday())
        if config['count_prefixes'].lower() == '1m':
            merged_df['minuteofhour'] = pd.to_datetime(merged_df['DATETIME']).dt.minute + 1

        # Changing order of columns
        columns = merged_df.columns.tolist()
        columns.insert(2, columns.pop(columns.index('DATETIME')))
        columns.insert(3, columns.pop(columns.index('DATE')))
        columns.insert(4, columns.pop(columns.index('TIME')))
        columns.insert(5, columns.pop(columns.index('dayofweek')))
        columns.insert(6, columns.pop(columns.index('hourofday')))
        columns.insert(7, columns.pop(columns.index('DATETIME_ORIG')))
        if config['count_prefixes'].lower() == '1m' and 'minuteofhour' in columns:
            columns.insert(7, columns.pop(columns.index('minuteofhour')))        
        merged_df = merged_df[columns]

        merged_dfs.append(merged_df)
    return time_resolutions, merged_dfs

# GENERATING INDICATOR VARIABLE TO FLAG THE START OF A FILE (FOR HOUSEKEEPING/VERIFICATION ONLY)
def indicator_variable(time_resolutions, merged_dfs):
    valid_dfs = []
    for time_resolution, merged_df in zip(time_resolutions, merged_dfs):
        merged_df['prestart'] = 0
        merged_df.loc[merged_df['DATETIME'] <= merged_df['first_file_timepoint'], 'prestart'] = 1

        merged_df['postend'] = 0
        merged_df.loc[merged_df['DATETIME'] > merged_df['last_file_timepoint'], 'postend'] = 1
        merged_df.loc[merged_df['DATETIME'] > merged_df['last_file_timepoint'] - pd.Timedelta(minutes=time_resolution), 'postend'] = 1

        # Generating temporary tag to check if any valid hours. If there are any valid hours, it is dropping rows that are not valid. If no valid hours, keep all rows but flag temp_flag_no_valid_days
        merged_df['valid'] = ~(merged_df['prestart'] == 1) & ~(merged_df['postend'] == 1)
        merged_df['temp_flag_no_valid_days'] = 1 if not merged_df['valid'].any() else None
        if merged_df['valid'].any():
            merged_df = merged_df.loc[merged_df['valid']]

        # Making copy of dataframe to improve memory
        merged_df = merged_df.copy()

        # Turning of notifications that we are using slices of dataframe.
        pd.options.mode.chained_assignment = None

        # Generating an index for freeday number
        merged_df.sort_values(by=['file_id', 'DATETIME'], inplace=True)
        merged_df.loc[:, 'row'] = merged_df.groupby('file_id').cumcount()
        merged_df['row'] = np.floor(merged_df['row']/(1440/time_resolution)).astype(int)
        merged_df['freeday_number'] = merged_df['row'] + 1
        merged_df.drop(columns=['row'], inplace=True)
        valid_dfs.append(merged_df)

    return valid_dfs

# GENERATING PWEAR VARIABLES
def pwear_variables(valid_dfs, time_resolutions):
    formatted_dfs = []

    for valid_df, time_resolution in zip(valid_dfs, time_resolutions):
        list_variables = ['ENMO_mean', 'ENMO_missing', 'ENMO_0plus']

        # Stripping out buffer section used in pampro processing
        valid_df = valid_df.drop(valid_df[(valid_df['ENMO_mean'] == -1) & (valid_df['ENMO_missing'] == 0) & (valid_df['ENMO_0plus'] == 0)].index)

        # Drop any bout variables (if there are any) just in case these were to get listed to be included in the conversion
        bout_variables = [col for col in valid_df.columns if '_mt' in col]
        valid_df.drop(columns=bout_variables, inplace = True)

        #Checking if enmo_0plus exists and then convert to fractions of time
        if 'ENMO_0plus' in valid_df.columns:
            variables_to_convert = [col for col in valid_df.columns if col.startswith('ENMO_') and col.endswith('plus')]

            for variable in variables_to_convert:
                valid_df[variable] = (valid_df[variable] / (60/valid_df['processing_epoch'])) / time_resolution
        else:
            pass

        #Checking if HPFVM_0plus exists and then convert to fractions of time
        if 'HPFVM_0plus' in valid_df.columns:
            variables_to_convert = [col for col in valid_df.columns if col.startswith('HPFVM_') and col.endswith('plus')]

            for variable in variables_to_convert:
                valid_df[variable] = (valid_df[variable] / (60/valid_df['processing_epoch'])) / time_resolution
        else:
            pass

        # Generating pwear variables and removing negative values
        pwear_columns = valid_df['ENMO_0plus'].copy()
        valid_df = pd.concat([valid_df, pd.DataFrame({'Pwear': pwear_columns})], axis=1)  
        valid_df.loc[valid_df['ENMO_mean'] < 0, 'Pwear'] = 0

        # Looking for HPFVM/PITCH/ROLL/ENMO MEAN
        variables_to_check = ["HPFVM_mean", "PITCH_mean", "ROLL_mean", 'ENMO_mean']        
        for var in variables_to_check:
            if var in valid_df.columns:
                valid_df.loc[valid_df['ENMO_n'] == 0, var] = None

        formatted_dfs.append(valid_df)
    return formatted_dfs

# Using start/end times from wear log if this was used
def wear_log(formatted_dfs):

    # Importing wear log as a dataframe
    wear_log_path = os.path.join(config.get('root_folder'), config.get('wear_log_folder'), f"{config.get('wear_log')}.csv")

    if os.path.exists(wear_log_path):
        wear_df = pd.read_csv(wear_log_path)
        variables = ['start', 'end']
        for var in variables:
            wear_df[var] = pd.to_datetime(wear_df[var], format='%d/%m/%Y %H:%M')

        # Merging wear log with each file, merging on id
        for i, formatted_df in enumerate(formatted_dfs):
            id_series = formatted_df['file_id'].str.split('_', n=1).str[0]
            formatted_df = pd.concat([formatted_df, pd.DataFrame({'id': id_series})], axis=1)
            formatted_df = pd.merge(formatted_df, wear_df, how='outer', on='id', indicator=True)
            formatted_df = formatted_df[formatted_df['_merge'] != 'right_only']
            formatted_df['day_valid'] = 0
            formatted_df['flag_no_wear_info'] = 0
            if formatted_df['_merge'].iloc[0] == 'both':
                formatted_df['day_valid'] = formatted_df.apply(lambda x: 1 if x['start'] <= x['DATETIME'] < x['end'] else 0, axis=1)
            if formatted_df['_merge'].iloc[0] == 'left_only':
                formatted_df['flag_no_wear_info'] = 1
            formatted_df['day_valid'] = formatted_df.apply(lambda x: 2 if x['flag_no_wear_info'] == 1 else x['day_valid'], axis=1)
            formatted_df = formatted_df.drop(columns=['id', '_merge'])
            formatted_dfs[i] = formatted_df

        return formatted_dfs
    else:
        print(f"There is no wear log saved in this folder location: {wear_log_path}. If you have a wear log that you wish to use make sure to save it in the folder. If no wear log the script can continue running without this.")


# CREATING FLAG FOR MECHANICAL NOISE THAT IS BEING COUNTED AS WEAR TIME AND RUNNING CORRUPTIONS HOUSEKEEPING
def mechanical_noise(formatted_dfs):

    # Printing out message that corruptions housekeeping is run (It is run a bit later in this function, but put it here so that it only prints out the message once and not fo each file)
    if config.get('run_corruptions_housekeeping').lower() == 'yes':
        print(Fore.GREEN + "RUNNING CORRUPTIONS HOUSEKEEPING TO ADJUST PWEAR BASED ON VERIFICATION CHECKS AND SPECIFICATIONS IN CORRUPTIONS CONDITIONS CSV" + Fore.RESET)

    dataframes = []

    for formatted_df in formatted_dfs:

        formatted_df['FLAG_MECH_NOISE'] = np.nan

        #Flagging epochs either from monitor issue/other sources (i.a. washing machines)
        formatted_df.loc[(formatted_df['ENMO_mean'] >= 3000) & (formatted_df['Pwear'] != 0) & (formatted_df['ENMO_mean'].notna()), 'FLAG_MECH_NOISE'] = 1
        # Flagging epochs that looks to have been worn - but it is just noise.
        formatted_df.loc[(formatted_df['ENMO_mean'] >= 1500) & (formatted_df['Pwear'] >= 0.9) & (formatted_df['ENMO_mean'].notna()), 'FLAG_MECH_NOISE'] = 1
        # Flagging epochs that have a very small amount of Pwear - but is getting a large amount of enmo for that blip of data.
        formatted_df.loc[(formatted_df['ENMO_mean'] >= 600) & (formatted_df['Pwear'] <= 0.1) & (formatted_df['Pwear'] != 0) & (formatted_df['ENMO_mean'].notna()), 'FLAG_MECH_NOISE'] = 1

        # --- SECTION TO RUN HOUSEKEEPING AND DROP FILES NOT NEEDED IN FINAL RELEASE --- #
        if config.get('run_corruptions_housekeeping').lower() == 'yes':
            # Reading in corruption condition csv file where rows that are corrupted are specified manually
            if os.path.exists(config.get('corruption_condition_file_path')):
                conditions_df = pd.read_csv(config.get('corruption_condition_file_path'))

                try:
                    conditions_df['DATE'] = pd.to_datetime(conditions_df['DATE'], format='%d/%m/%Y', errors='raise')
                    formatted_df['DATE'] = pd.to_datetime(formatted_df['DATE'], format='%Y-%m-%d', errors='coerce')
                except ValueError as e:
                    print(Fore.RED + "\nError: Ensure that all dates in the DATE column in the corruptions condition csv are in the format dd/mm/YYYY (e.g., 01/01/1990). Re-run scripts once this has been corrected." + Fore.RESET)

                # Specifying rows to filter conditions on
                for _, row in conditions_df.iterrows():
                    if config['count_prefixes'].lower() == '1h':
                        conditions = (
                            (formatted_df['DATE'] == row['DATE']) &
                            (formatted_df['hourofday'] == row['hourofday']) &
                            (formatted_df['dayofweek'] == row['dayofweek']) &
                            (formatted_df['file_id'] == row['file_id'])
                        )
                    if config['count_prefixes'].lower() == '1m':
                        conditions = (
                        (formatted_df['DATE'] == row['DATE']) &
                        (formatted_df['minuteofhour'] == row['minuteofhour']) &
                        (formatted_df['hourofday'] == row['hourofday']) &
                        (formatted_df['dayofweek'] == row['dayofweek']) &
                        (formatted_df['file_id'] == row['file_id'])
                    )

                    # Changing Pwear to 0 where conditions are met
                    formatted_df.loc[conditions, 'Pwear'] = 0
            else:
                print(f"No corruption condition csv file was found in the specified location: {config.get('corruption_condition_file_path')}. Make sure to save the file and edit the CORRUPTION_CONDITION_FILE_PATH in the config.get('py') file.")
        dataframes.append(formatted_df)

    return dataframes


# OUTPUTTING THE DATAFRAME TO THE INDIVIDUAL_PARTPRO_FILES FOLDER
def outputting_dataframe(dataframes, files_list):

    for dataframe, file_list in zip(dataframes, files_list):
        dataframe.sort_values(by=['file_id', 'DATETIME'], inplace=True)

        # Rounding all numeric columns to 4 decimal places
        numeric_columns = dataframe.select_dtypes(include=['float64', 'float32']).columns
        dataframe[numeric_columns] = dataframe[numeric_columns].round(6)

        file_path = os.path.join(config.get('root_folder'), config.get('results_folder'), config.get('summary_folder'), config.get('individual_partpro_f'), config.get('time_res_folder'))
        os.makedirs(file_path, exist_ok=True)
        file_name = os.path.join(file_path, f"{file_list}_{config.get('output_file_ext')}.csv")

        dataframe.to_csv(file_name, index=False)


if __name__ == '__main__':

    if len(sys.argv) < 2:
        print("Error: No config file provided.")
        sys.exit(1)

    config_path = sys.argv[1]
    config = load_config(config_path)

    print("Loaded config:", config)
    # Now you can use config values inside your script
    
    task_id = os.environ.get('SLURM_ARRAY_TASK_ID')
    files_list = reading_filelist(str(int(task_id)-1))
    print(f"Task ID: {task_id}; files list: {files_list}")
    metadata_dfs = reading_metadata(files_list)
    datafiles_dfs = reading_datafile(files_list)
    if config.get('processing').lower() == 'pampro':
        anomalies_df = anomalies()
        time_resolutions, merged_dfs = merging_data(files_list, metadata_dfs, datafiles_dfs, anomalies_df)
    if config.get('processing').lower() == 'wave':
        time_resolutions, merged_dfs = merging_data(files_list, metadata_dfs, datafiles_dfs, None)
    valid_dfs = indicator_variable(time_resolutions, merged_dfs)
    formatted_dfs = pwear_variables(valid_dfs, time_resolutions)
    if config.get('use_wear_log') == 'Yes':
        wear_log(formatted_dfs)
    dataframes = mechanical_noise(formatted_dfs)
    outputting_dataframe(dataframes, files_list)

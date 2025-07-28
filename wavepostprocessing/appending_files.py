############################################################################################################
# This file appends the summary, daily and hourly datasets
# Author: CAS
# Date: 03/07/2024
# Version: 1.0 Translated from Stata code
############################################################################################################
# IMPORTING PACKAGES #
#import config
import os
import pandas as pd
import numpy as np
from wavepostprocessing.config import load_config, print_message
#from config import load_config, print_message
import sys

############################################################################################################
# PART A: This do file will append together all individual Summary files as well as join the files which have not produced an individual summary file
############################################################################################################

# CREATING A FILE LIST OF ALL FILES IN THE RESULTS FOLDER #
def create_filelist(folder):

    # DELETE ALL PAST FILELISTS IN INDIVIDUAL SUMMARY FOLDER
    file_path = os.path.join(config.get('root_folder'), config.get('results_folder'), config.get('summary_folder'), folder, config.get('time_res_folder'))

    if os.path.exists(os.path.join(file_path, "filelist.txt")):  # If any filelist in the summary folder, this will be deleted.
        os.remove(os.path.join(file_path, "filelist.txt"))

    # CREATING NEW FILELIST IN INDIVIDUAL SUMMARY FOLDER - CODE IS DIFFERENT FOR MAC AND WINDOWS, CHANGE PC TYPE IN THE BEGINNING OF SCRIPT
    os.chdir(file_path)

    if config.get('pc_type').lower() == "windows":
        os.system('dir /b *.csv > filelist.txt')
    elif config.get('pc_type').lower() == "mac":
        os.system('ls /b *csv > filelist.txt')
    elif config.get('pc_type').lower() == "linux":
        os.system('ls *csv > filelist.txt')

    return file_path

# REMOVING FILES THAT SHOULD NOT BE APPENDED #
def remove_files(output_file):
    filelist_df = pd.read_csv('filelist.txt', header=None, names=['v1'])  # Reading in the filelist
    filelist_df = filelist_df.rename(columns={'v1': 'file_name'})

    # Dropping the data dictionary from the list and also the list of the main output file if saved in the same location so it is not looped through
    data_dictionary = 'dictionary'
    filelist_df = filelist_df[~filelist_df['file_name'].str.contains(data_dictionary, case=False, na=False)]
    filelist_df = filelist_df[~filelist_df['file_name'].str.contains(output_file, case=False, na=False)]

    # Creating list with all filenames
    files_list = filelist_df['file_name'].tolist()
    return files_list

# Appending summary files
def appending_files(files_list, file_path, append_level):
    dataframes = []

    # Looping though each file and appending
    if files_list:
        for file_name in files_list:
            full_file_path = os.path.join(file_path, f"{file_name}")

            if os.path.exists(full_file_path):
                dataframe = pd.read_csv(full_file_path)
                dataframes.append(dataframe)

    if dataframes:
        appended_df = pd.concat(dataframes, ignore_index=True)
    else:
        appended_df = pd.DataFrame()

    if append_level == 'hourly':
        appended_df['id'] = appended_df['file_id']

    # DROP ANY FILE WITH NO ID
    appended_df = appended_df.dropna(subset=['id'])

    # REMOVING THRESHOLDS FROM THE MAIN OUTPUT FILE IF THIS IS SPECIFIED IN CONFIG FILE
    if config.get('remove_thresholds').lower() == 'yes':
        variable_prefixes = 'enmo_'
        if not any(item.lower() == "hpfvm" for item in config.get('variables_to_drop')):
            variable_prefixes += 'HPFVM_'
        variable_suffix = 'plus'

        columns_to_drop = []
        for variable_prefix in variable_prefixes:
            for column_name in appended_df.columns:
                if column_name.startswith(variable_prefix) and column_name.endswith(variable_suffix):
                    columns_to_drop.append(column_name)

        appended_df = appended_df.drop(columns=columns_to_drop)
    return appended_df

# Creating filelist of any IDS that have not had an analysis file produced from post processing
def no_analysis_filelist():
    no_analysis_path = os.path.join(config.get('root_folder'), config.get('results_folder'), config.get('filelist_folder'), 'No_Analysis_Files.txt')
    if os.path.exists(no_analysis_path):

        no_analysis_filelist_df = pd.read_csv(no_analysis_path)  # Reading in the filelist
        no_analysis_filelist_df = no_analysis_filelist_df.drop_duplicates(subset=['filename_temp'])
        no_analysis_files = no_analysis_filelist_df['filename_temp'].tolist()

        return no_analysis_files

    else:
        return []


# Appending any IDS that have not had an analysis file produced from post processing and outputting the dataset
def appending_no_analysis_files(no_analysis_files, appended_df, file_name):
    no_analysis_dataframes = []
    if not no_analysis_files:
        print("All files had a metadata and data file. No extra data to append.")
        output_file_path = os.path.join(config.get('root_folder'), config.get('results_folder'), config.get('summary_folder'))
        os.makedirs(output_file_path, exist_ok=True)
        file_name = os.path.join(output_file_path, f"{file_name}.csv")
        appended_df.to_csv(file_name, index=False)

        return

    for file_id in no_analysis_files: #??? what if there is more than one ?

        metadata_file_path = os.path.join(config.get('root_folder'), config.get('results_folder'), f"metadata_{file_id}.csv")
        if os.path.exists(metadata_file_path):
            no_analysis_metadata_df = pd.read_csv(metadata_file_path)

            # Specifying what variables to keep
            variables_to_keep = [
            #    '.*start_error*.', '.*end_error*.', 'calibration_method', 'noise_cutoff_mg',
            #    'generic_first_timestamp', 'generic_last_timestamp', 'device', 'processing_epoch'

                '.*start_error*.', '.*end_error*.', '^calibration_method$', '^noise_cutoff_mg$',
                '^generic_first_timestamp$', '^generic_last_timestamp$', '^device$', '^processing_epoch$', '^frequency$'
            ]
            # Variables to keep if processed through Wave
            if config.get('processing').lower() == 'wave':
                variables_to_keep.extend(['.*anom*.', '.*batt*.'])
            # Variables to keep if processed through Pampro
            if config.get('processing').lower() == 'pampro':
                #variables_to_keep.extend(['calibration_type', 'QC_axis_anomaly'])
                variables_to_keep.extend(['^calibration_type$', '^QC_axis_anomaly$'])
            # Joining the variables to be able to use regular expression
            combined_variables = '|'.join(variables_to_keep)
            no_analysis_metadata_df = no_analysis_metadata_df.filter(regex=combined_variables)
            no_analysis_metadata_df['id'] = file_id

            # Renaming variables
            no_analysis_metadata_df = no_analysis_metadata_df.rename(columns={'end_error': 'file_end_error', 'start_error': 'file_start_error', 'noise_cutoff_mg': 'noise_cutoff'})
            variables_to_lower_case = [col for col in no_analysis_metadata_df.columns if col.startswith('QC_') and col != 'QC_axis_anomaly']
            lower_case_mapping = {col: col.lower() for col in variables_to_lower_case}
            no_analysis_metadata_df = no_analysis_metadata_df.rename(columns=lower_case_mapping)

            # Dropping variables:
            if config.get('processing').lower() == 'wave':
                no_analysis_metadata_df.drop(columns=['first_battery', 'last_battery'], inplace=True)

            # Formatting time stamp variables:
            generic_timestamps = ['generic_first_timestamp', 'generic_last_timestamp']
            no_analysis_metadata_df[generic_timestamps] = no_analysis_metadata_df[generic_timestamps].apply(lambda x: x.str[:19])

            no_analysis_dataframes.append(no_analysis_metadata_df)

        # Appending dataframes if there are any
        if no_analysis_dataframes:
            appended_no_analysis_df = pd.concat(no_analysis_dataframes, ignore_index=True)
        else:
            appended_no_analysis_df = pd.DataFrame()

        # appending the dataset from no_analysis with the ones that have analysis data.
        merged_df = pd.concat([appended_df, appended_no_analysis_df], ignore_index=True)

        # Changing the variable valid into a boolean variable so missing values are set to FALSE
        if 'valid' in merged_df.columns:
            merged_df['valid'] = merged_df['valid'].replace('', np.nan)
            merged_df['valid'] = merged_df['valid'].astype('bool', errors='ignore')


    # Outputting appended summary dataframe
    output_file_path = os.path.join(config.get('root_folder'), config.get('results_folder'), config.get('summary_folder'))
    os.makedirs(output_file_path, exist_ok=True)
    file_name = os.path.join(output_file_path, f"{file_name}.csv")
    #add temporarily
    print(file_name)
    merged_df.to_csv(file_name, index=False)



if __name__ == '__main__':

    if len(sys.argv) < 2:
        print("Error: No config file provided.")
        sys.exit(1)

    config_path = sys.argv[1]
    config = load_config(config_path)

    print("Loaded config:", config)
    # Now you can use config values inside your script
    
    # Appending summary files
    if config.get('run_append_summary_files').lower() == 'yes':
        print_message("APPENDING ALL INDIVIDUAL SUMMARY FILES TOGETHER")
        summary_file_path = create_filelist(folder=config.get('individual_sum_f'))
        summary_files_list = remove_files(output_file=config.get('sum_output_file'))
        summary_appended_df = appending_files(summary_files_list, file_path=summary_file_path, append_level='summary')
        no_analysis_files = no_analysis_filelist()
        appending_no_analysis_files(no_analysis_files, summary_appended_df, file_name=config.get('sum_output_file'))

    # Appending hourly trimmed files
    if config.get('run_append_hourly_files').lower() == 'yes' or config.get('run_append_minute_level_files').lower() == 'yes':
        if config.get('count_prefixes').lower() == '1h':
           print_message("APPENDING ALL INDIVIDUAL HOURLY FILES TOGETHER")
        if config.get('count_prefixes').lower() == '1m':
           print_message("APPENDING ALL INDIVIDUAL MINUTE LEVEL FILES")

        hourly_file_path = create_filelist(folder=config.get('individual_trimmed_f'))
        hourly_files_list = remove_files(output_file=config.get('hour_output_file'))
        hourly_appended_df = appending_files(hourly_files_list, file_path=hourly_file_path, append_level='hourly')
        no_analysis_files = no_analysis_filelist()
        appending_no_analysis_files(no_analysis_files, hourly_appended_df, file_name=config.get('hour_output_file'))

    # Appending daily files
    if config.get('run_append_daily_files').lower() == 'yes':
        print_message("APPENDING ALL INDIVIDUAL DAILY FILES TOGETHER")
        daily_file_path = create_filelist(folder=config.get('individual_daily_f'))
        daily_files_list = remove_files(output_file=config.get('day_output_file'))
        daily_appended_df = appending_files(daily_files_list, file_path=daily_file_path, append_level='daily')
        no_analysis_files = no_analysis_filelist()
        appending_no_analysis_files(no_analysis_files, daily_appended_df, file_name=config.get('day_output_file'))

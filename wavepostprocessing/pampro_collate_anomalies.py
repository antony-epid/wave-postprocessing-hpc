############################################################################################################
# This file collates all anomalies found wihtin a project into one file. Meta data is merged on to produce information on how much of the file is lost due to anomalies.
# Author: cas254
# Date: 11/11/2024
# Version: 1.0
############################################################################################################

# --- IMPORTING PACKAGES --- #
import os
import pandas as pd
#import config
import glob
import numpy as np
from wavepostprocessing.config import load_config
#from config import load_config
import sys

# --- GETTING LIST OF FILES AND APPENDING THEM --- #
def list_files(FOLDER, pattern, variable, REPLACE):
    all_files = []
    # Creating list of anomalies files in _anomalies folder
    files = glob.glob(os.path.join(config.get('root_folder'), FOLDER, pattern))
    files = [file for file in files if "collapsed_anomalies.csv" not in file]

    # If any anomalies files exists each of them will be read in as a dataframe
    for file_path in files:
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            df[variable] = os.path.basename(file_path)
            df['file_id'] = df[variable].str.replace(REPLACE, '', regex=False)
            all_files.append(df)

    # The individual anomalies files are appended
    if all_files:
        appended_df = pd.concat(all_files, ignore_index=True)
        return appended_df, all_files

    else:
        print("There were no anomalies present within the dataset. collapsed_anomalies.csv were not produced")
        return pd.DataFrame(), []

# Merging with the meta_data file to get the file duration and final time stamp from the QC data
def merge_meta_data(anomaly_df, qc_meta_df):
    qc_subset = qc_meta_df[['file_id', 'file_duration', 'last_timestamp_time']]
    # While merging we remove files that did not have an anomaly
    merged_df = pd.merge(anomaly_df, qc_subset, how='outer', on='file_id', indicator=True, validate='m:1').query('_merge != "right_only"')

    return merged_df

# Generating variables to be used to flag the anomalies
# Checking battery - present if anomaly B
def check_battery(df, variable):
    if variable in df.columns:
        df['batt_increase'] = df.apply(lambda x: x['Battery_after_anomaly'] - x['Battery_before_anomaly'] if pd.notnull(x['Battery_after_anomaly']) else None, axis=1)

# Checking timestamps
def check_timestamp(df, variable, var1, A):
    if variable in df.columns:
        df[var1] = df[variable].apply(lambda x: x[:A] if pd.notnull(x) else None)

# Creating date time variables to be used to calculate amount of time lost to anomalies
def create_timestamp(df, variable):
    if variable in df.columns:
        df[f'{variable}_1'] = df[variable].apply(lambda x: x[:19] if pd.notnull(x) else None)
        df[f'{variable}_1'] = pd.to_datetime(df[f'{variable}_1'], format='%Y-%m-%d %H:%M:%S')

# Creating binary variables to indicate if there has been an anomaly of that type. This is used when data is to be collapsed
def anomaly_type(df, letter):
    df[f'Anom_{letter}'] = df['anomaly_type'].apply(lambda x: 1 if x == letter else 0)

# Creating time difference variables
def create_time_diff(df, var_diff, var1, var2):
    df[var_diff] = (df[var1] - df[var2]).dt.total_seconds()



if __name__ == '__main__':

    if len(sys.argv) < 2:
        print("Error: No config file provided.")
        sys.exit(1)

    config_path = sys.argv[1]
    config = load_config(config_path)

    print("Loaded config:", config)
    # Now you can use config values inside your script
    
    # Appending all anomalies files
    all_anomalies_df, all_anomalies_files = list_files(FOLDER=config.get('anomalies_folder'), pattern='*anomalies.csv', variable='anomaly_file', REPLACE='_anomalies.csv')

    # Appending all qc_meta files
    all_qc_meta_df, all_qc_files = list_files(FOLDER=config.get('results_folder'), pattern='qc_meta*', variable='qc_file', REPLACE='qc_meta_')
    all_qc_meta_df['file_id'] = all_qc_meta_df['file_id'].str.replace('.csv', '', regex=False)

    # Only continuing with formatting the anomalies dataset if any anomalies were present, otherwise finishing this script
    if all_anomalies_files:
        merged_df = merge_meta_data(anomaly_df=all_anomalies_df, qc_meta_df=all_qc_meta_df)

        # Checking for an increase in battery - battery_after_anomaly would only be created if Anomaly B present
        check_battery(merged_df, variable='Battery_after_anomaly')

        # Creating final date variables to flag anything occuring on final day
        check_timestamp(merged_df, variable='last_timestamp_time', var1='final_date', A=10)
        merged_df['final_date'] = pd.to_datetime(merged_df['final_date'], format='%d/%m/%Y')
        check_timestamp(df=merged_df, variable='last_timestamp_time', var1='LAST1', A=19)
        merged_df['LAST1'] = pd.to_datetime(merged_df['LAST1'], format='%d/%m/%Y %H:%M:%S')

        # Creating last good date to see if anomaly start on final day
        check_timestamp(merged_df, variable='last_good_timestamp', var1='last_date', A=10)
        merged_df['last_date'] = pd.to_datetime(merged_df['last_date'], format='%Y-%m-%d')

        # Creating date time variables to be used to calculate amount of time lost due to anomalies
        list_variables = ['first_timestamp_after_shift', 'last_good_timestamp', 'recovery_point_timestamp']
        for var in list_variables:
            create_timestamp(df=merged_df, variable=var)

        # Creating binary variables for each anomaly type
        list_anomalies = ['A', 'B', 'C', 'D', 'E', 'F']
        for anomaly in list_anomalies:
            anomaly_type(df=merged_df, letter=anomaly)

        # Creating time difference variable if there is a shift in time
        create_time_diff(df=merged_df, var_diff ='time_diff1', var1='first_timestamp_after_shift_1', var2='last_good_timestamp_1')
        # Creating time difference variable if there is a recovery time
        create_time_diff(df=merged_df, var_diff='time_diff2', var1='recovery_point_timestamp_1', var2='last_good_timestamp_1')
        # Creating time difference variable if the data after a certain point cannot be used
        create_time_diff(df=merged_df, var_diff='time_diff3', var1='LAST1', var2='last_good_timestamp_1')

        # Replacing time difference with missing based on values in Anoma_X
        merged_df.loc[merged_df['Anom_E'] == 1, 'time_diff2'] = np.nan
        merged_df.loc[(merged_df['Anom_A'] == 0) & (merged_df['Anom_C'] == 0) & (merged_df['Anom_E'] == 0), 'time_diff2'] = np.nan
        merged_df.loc[(merged_df['Anom_D'] == 0) & (merged_df['Anom_F'] == 0), 'time_diff3'] = np.nan

        # Collapsing dataframe
        columns_to_sum = ['time_diff1', 'time_diff2', 'Anom_A', 'Anom_B', 'Anom_C', 'Anom_D', 'Anom_E', 'Anom_F']
        collapse_dict = {
            'file_duration': 'first',
            **{col: 'sum' for col in columns_to_sum},
            'time_diff3': 'max'
        }
        collapsed_df = merged_df.groupby('file_id').agg(collapse_dict).reset_index()

        # Replacing time difference variables with 0 if specified anomalies are present
        collapsed_df.loc[(collapsed_df['Anom_A'] == 0) & (collapsed_df['Anom_C'] == 0) & (collapsed_df['Anom_E'] == 0), 'time_diff2'] = 0
        collapsed_df.loc[(collapsed_df['Anom_D'] == 0) & (collapsed_df['Anom_F'] == 0), 'time_diff3'] = 0

        # Generating estimates of time lost
        collapsed_df['est_time_lost'] = collapsed_df['time_diff1'] + collapsed_df['time_diff2'] + collapsed_df['time_diff3']
        collapsed_df['est_percentage_lost'] = (collapsed_df['est_time_lost'] / collapsed_df['file_duration']) * 100

        collapsed_df['FLAG_ANOMALY'] = 1

        # Dropping variables that are not needed:
        collapsed_df = collapsed_df.drop(columns=['time_diff1', 'time_diff2', 'time_diff3'], axis=1)

        # Output collapsed anomaly file to _anomalies folder:
        output_path = os.path.join(config.get('root_folder'), config.get('anomalies_folder'), "collapsed_anomalies.csv")
        collapsed_df.to_csv(output_path, index=False)



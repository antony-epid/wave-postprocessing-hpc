############################################################################################################
# This file is preparing summary, daily and hourly release files
# Author: CAS
# Date: 02/01/2025
# Version: 1.0 prepare hourly, daily and summary release files are merged into one script. This scripts can create 1 or all 3 release files. It can run on both output from Wave and Pampro.
############################################################################################################
# IMPORTING PACKAGES #
import os

from numpy.ma.core import angle

import pandas as pd
from colorama import Fore
from datetime import date, datetime
from wavepostprocessing.Housekeeping import filenames_to_remove
import numpy as np
from wavepostprocessing.config import load_config, print_message
#from config import load_config, print_message
import sys

#########################################################
# --- IMPORTING AND FORMATTING SUMMARY RESULTS FILE --- #
#########################################################

def formatting_file(import_file_name, release_level, pwear, pwear_morning, pwear_quad, print_message, output_filename):
    # Make release directories if not already present
    try:
        os.makedirs(os.path.join(config.get('root_folder'), config.get('releases_folder'), config.get('pc_date')))
    except FileExistsError:
        pass

    file_path = os.path.join(config.get('root_folder'), config.get('results_folder'), config.get('summary_folder'), import_file_name)
    if os.path.exists(file_path):
        #change from early Jan to the current version
        #df = pd.read_csv(file_path)
        df = pd.read_csv(file_path, dtype={'subject_code': str})        
    else:
        print(f"The file {file_path} does not exist. The release on {release_level} level could not be prepared.")
        df = pd.DataFrame()
        return df

    # Generating id and filename
    if 'id' in df.columns:
        df.rename(columns={'id': 'filename'}, inplace=True)
    else:
        df.rename(columns={'file_id': 'filename'}, inplace=True)
    df['id'] = df['filename'].apply(lambda x: x.split("_")[0])

    # For pampro output: Merging anomalies and axis anomaly info from qc_meta file
    if config.get('processing').lower() == 'pampro':
        collapsed_anomalies_path = os.path.join(config.get('root_folder'), config.get('anomalies_folder'), f'collapsed_anomalies.csv')
        if os.path.exists(collapsed_anomalies_path):
            collapsed_anomalies_df = pd.read_csv(collapsed_anomalies_path)
            df['file_id'] = df['filename']
            df = df.merge(collapsed_anomalies_df[['file_id', 'FLAG_ANOMALY']], on='file_id', how='left')
        else:
            df['FLAG_ANOMALY'] = np.nan

        # Flagging axis anomalies
        df['FLAG_AXIS_FAULT'] = 0
        df.loc[df['QC_axis_anomaly'] == 'True', 'FLAG_AXIS_FAULT'] = 1

        # Setting all variables to missing if confirmed axis issue
        columns_to_replace = [col for col in df.columns if any(pattern in col for pattern in ['Pwear', 'pwear', 'ENMO', 'enmo', 'HPFVM', 'hpfvm'])]
        for col in columns_to_replace:
            df.loc[df['FLAG_AXIS_FAULT'] == 1, col] = np.nan


    # Sorting dataset
    if release_level == 'summary':
        df = df.sort_values(by='id')
    if release_level == 'daily':
        df = df.sort_values(by=['id', 'day_number'])
    if release_level == 'hourly':
        df = df.sort_values(by=['id', 'DATETIME'])

    # Renaming timestamp variables
    if release_level == 'summary' or release_level == 'daily':
        df.rename(columns={'generic_first_timestamp': 'first_file_timepoint', 'generic_last_timestamp': 'last_file_timepoint'}, inplace=True)

    # Dropping variables that are not needed for hourly releases
    if release_level == 'hourly' and config.get('processing').lower() == 'wave':
        variables_to_drop = ['generic_first_timestamp', 'generic_last_timestamp', 'database_id', 'DATETIME_ORIG', 'subject_code', 'processing_script',
                             'prestart', 'postend', 'valid', 'freeday_number', 'serial', 'ENMO_n', 'ENMO_missing']
        df.drop(columns=variables_to_drop, inplace=True, errors='ignore')

    # Setting PWear to 0 if ENMO_mean is negative for hourly releases
        df.loc[(df['ENMO_mean'] < 0), 'Pwear'] = 0

    # Generating include criteria
    if release_level == 'summary' or release_level == 'daily':
        df['include'] = 0

        if config.get('impute_data').lower() == 'no':
            df.loc[
                (df['Pwear'] >= pwear) &
                (df['Pwear_morning'] >= pwear_morning) &
                (df['Pwear_noon'] >= pwear_quad) &
                (df['Pwear_afternoon'] >= pwear_quad) &
                (df['Pwear_night'] >= pwear_quad) &
                (df['file_end_error'] <= df['noise_cutoff']), 'include'] = 1

        if config.get('impute_data').lower() == 'yes':

            # Checking if FLAG_NO_VALID_DAYS is a variable in the dataframe and otherwise it will give it the value 0
            FLAG_NO_VALID_DAYS_exists = 'FLAG_NO_VALID_DAYS' in df.columns
            FLAG_NO_VALID_DAYS_condition = (df['FLAG_NO_VALID_DAYS'] != 1) if FLAG_NO_VALID_DAYS_exists else True
            CALIBRATION_TYPE_exists = 'calibration_type' in df.columns
            CALIBRATION_TYPE_condition = (df['calibration_type'] != 'fail') if CALIBRATION_TYPE_exists else True
            FLAG_AXIS_FAULT_exists = 'FLAG_AXIS_FAULT' in df.columns
            FLAG_AXIS_FAULT_condition = (df['FLAG_AXIS_FAULT'] != 1) if FLAG_AXIS_FAULT_exists else True

            df.loc[
                (df['Pwear'] >= pwear) &
                (df['Pwear_morning'] >= pwear_morning) &
                (df['Pwear_noon'] >= pwear_quad) &
                (df['Pwear_afternoon'] >= pwear_quad) &
                (df['Pwear_night'] >= pwear_quad) &
                (CALIBRATION_TYPE_condition) &
                (FLAG_NO_VALID_DAYS_condition) &
                (FLAG_AXIS_FAULT_condition), 'include'] = 1
            df.loc[
                (df['Pwear'] >= pwear) &
                (df['Pwear_morning'] < pwear_morning) &
                (df['Pwear_noon'] >= pwear_quad) &
                (df['Pwear_afternoon'] >= pwear_quad) &
                (df['Pwear_night'] >= pwear_quad) &
                (CALIBRATION_TYPE_condition) &
                (FLAG_NO_VALID_DAYS_condition) &
                (df['include'] != 1) &
                (FLAG_AXIS_FAULT_condition), 'include'] = 2

            df.loc[(df['include'] == 2), 'imputed'] = 1

        if release_level == 'summary' or release_level == 'daily':
            # Consolidation of all intensity variables
            search_pattern = ['Pwear', 'pwear', 'enmo', 'ENMO', 'hpfvm']
            columns_to_change = [col for col in df.columns if any(col.startswith(pattern) for pattern in search_pattern) and not col.endswith('_IMP')]

            new_columns = {}
            for col in columns_to_change:
                original_col = f'{col}_orig'
                new_columns[original_col] = df[col]
                # Define the imputed and consolidated columns
                imputed_col = f'{col}_IMP'
                consolidated_col = f'{col}_consolidated'
                # Generating imputed and colsolidated variables
                if imputed_col in df.columns:

                    # Generating consolidated variables, which is the orig value if include is not 2, but the imputed if include = 2
                    new_columns[consolidated_col] = np.where(
                        df['include'] == 2,
                        df[imputed_col],
                        new_columns[original_col]
                    )

            df = pd.concat([df, pd.DataFrame(new_columns)], axis=1)

            # Dropping the "original" variable after the _orig variable is created
            not_to_drop = ['_orig', '_IMP', '_consolidated']
            dropping_original_var = [col for col in df.columns if any(col.startswith(pattern) for pattern in search_pattern) and not any(col.endswith(end_pattern) for end_pattern in not_to_drop)]

            for col in dropping_original_var:
                df.drop(columns=[col], inplace=True)


        # Renaming day variables to monday, tuesday etc. rather than 1, 2
        day_mapping = {
            'day1': 'monday',
            'day2': 'tuesday',
            'day3': 'wednesday',
            'day4': 'thursday',
            'day5': 'friday',
            'day6': 'saturday',
            'day7': 'sunday'
        }

        def replace_day(name, mapping):
            for old, new in mapping.items():
                name = name.replace(old, new)
            return name

        df.rename(columns=lambda x: replace_day(x, day_mapping), inplace=True)

    # Changing order or variables in dataframe
    columns = ['id'] + [col for col in df.columns if col != 'id']
    df = df[columns]

    columns_order = list(df.columns)

    # Changing order of variables for wave output
    if config.get('processing').lower() == 'wave':
        if release_level != 'hourly':
            columns_order.insert(columns_order.index('noise_cutoff'), columns_order.pop(columns_order.index('TIME_RESOLUTION')))
        if release_level == 'summary' or release_level == 'hourly':
            columns_order.insert(columns_order.index('QC_anomaly_G'), columns_order.pop(columns_order.index('first_file_timepoint')))
            columns_order.insert(columns_order.index('first_file_timepoint'), columns_order.pop(columns_order.index('last_file_timepoint')))
        df = df[columns_order]

    # Changing variables names to lower case
    if release_level == 'hourly':
        renaming_intensity_var = [col for col in df.columns if col.startswith('ENMO') or col.startswith('HPFVM') or col.startswith('PITCH') or col.startswith('ROLL')]
        rename_mapping = {col: col.lower() for col in renaming_intensity_var}
        df.rename(columns=rename_mapping, inplace=True)

        # Changing order of variables for pampro output
    if config.get('processing').lower() == 'pampro':
        type_order = ['consolidated', 'orig', 'IMP']
        intensity_variables = ['enmo', 'hpfvm']

        def order_within_category(df, type, intensity_variables):
            ordered_variables = []

            pwear_columns = [col for col in df.columns if (col.startswith('Pwear') or col.startswith('pwear')) and (col.endswith(type))]
            for intensity_var in intensity_variables:
                if not any(col.startswith(intensity_var) for col in df.columns):
                    continue
                intensity_thresholds = [col for col in df.columns if col.startswith(intensity_var) and 'plus' in col and col.endswith(type)]
                intensity_day = [col for col in df.columns if col.startswith(intensity_var) and 'day' in col and col.endswith(type)]
                intensity_hour = [col for col in df.columns if col.startswith(intensity_var) and 'hour' in col and col.endswith(type)]
                if intensity_var == 'enmo':
                    ordered_variables.extend([*pwear_columns, f'{intensity_var}_mean_{type}', *intensity_thresholds, *intensity_day, *intensity_hour])
                elif intensity_var == 'hpfvm':
                    ordered_variables.extend([f'{intensity_var}_mean_{type}', *intensity_thresholds, *intensity_day, *intensity_hour])

            return ordered_variables

        #no change from early Jan to the current version 
        pampro_variables = ['id', 'filename']  
        #pampro_variables = ['id', 'filename', 'subject_code']        
        if release_level == 'summary':
            pampro_variables += ['startdate', 'RecordLength']

        if release_level == 'daily':
            pampro_variables += ['DATE', 'day_number', 'dayofweek']

        if release_level == 'hourly':
            pampro_variables += ['timestamp', 'DATETIME', 'DATETIME_ORIG', 'DATE', 'TIME', 'dayofweek', 'hourofday']
            #addition (change from early Jan to the current version)
            if config.get('count_prefixes').lower() == '1m':                
                pampro_variables += ['minuteofhour']

        pampro_column_order = pampro_variables

        if release_level == 'summary' or release_level == 'daily':
            for type in type_order:
                pampro_column_order.extend(order_within_category(df, type, intensity_variables))

        # Adding intensity variables and pitch and roll for hourly release file - only original variables
        if release_level == 'hourly':
            enmo_variables = [col for col in df.columns if col.startswith('enmo')]
            if any(col.startswith('hpfvm') for col in df.columns):
                hpfvm_columns = [col for col in df.columns if col.startswith('hpfvm')]
            if any(col.startswith('pitch') for col in df.columns):
                pitch_columns = [col for col in df.columns if col.startswith('pitch')]
            if any(col.startswith('roll') for col in df.columns):
                roll_columns = [col for col in df.columns if col.startswith('roll')]
            hourly_variables = ['Pwear', *enmo_variables, *hpfvm_columns, *pitch_columns, *roll_columns]
            pampro_column_order.extend(hourly_variables)

            # Changing pitch and roll (if present in dataset) to be in proportion of time:
            pitch_roll_var = []
            if any(col.startswith('pitch') for col in df.columns):
                pitch_roll_var += ['pitch']
            if any(col.startswith('roll') for col in df.columns):
                pitch_roll_var += ['roll']
            if pitch_roll_var:
                ends = ['mean', 'std', 'min', 'max']
                for variable in pitch_roll_var:
                    angle_variables = [col for col in df.columns if
                                       col.startswith(variable) and not any(col.endswith(end) for end in ends)]
                    for angle_var in angle_variables:
                        if config.get('count_prefixes') == '1h':
                            df[angle_var] = df[angle_var] / 720
                        if config.get('count_prefixes') == '1m':
                            df[angle_var] = df[angle_var] / 12


        if release_level == 'summary' or release_level == 'daily' or release_level == 'hourly':
            remaining_columns = ['first_file_timepoint', 'last_file_timepoint', 'device', 'FLAG_ANOMALY', *config.get('anom_var_pampro'),
                             'FLAG_AXIS_FAULT', 'file_start_error', 'file_end_error', 'mf_start_error',
                             'mf_end_error', 'calibration_type', 'calibration_method',
                             'noise_cutoff', 'processing_epoch', 'frequency']

            if release_level == 'summary' or release_level == 'daily':
                noise_cutoff_index = remaining_columns.index('noise_cutoff')
                remaining_columns.insert(noise_cutoff_index, 'TIME_RESOLUTION')
                remaining_columns += ['include', 'imputed']

            if release_level == 'hourly':
                #edit (change from early Jan to the current version)
                #remaining_columns += ['Battery_mean', 'day_valid', 'days_of_data_processed', 'FLAG_MECH_NOISE', 'freeday_number',                                 
                #remaining_columns += ['Battery_mean', 'days_of_data_processed', 'FLAG_MECH_NOISE', 'freeday_number',                                      
                #                      'generic_first_timestamp', 'generic_last_timestamp', 'postend', 'prestart', 'Temperature_mean', 'valid']
                remaining_columns += ['days_of_data_processed', 'FLAG_MECH_NOISE', 'freeday_number',                                      
                                      'generic_first_timestamp', 'generic_last_timestamp', 'postend', 'prestart', 'valid']                
                remaining_columns.remove("noise_cutoff")
                if config.get('count_prefixes').lower() == '1h' and 'day_valid' in df.columns:                     
                     remaining_columns.insert(1, 'day_valid')
            if config.get('use_wear_log').lower() == 'yes':
                remaining_columns += ['start', 'end', 'flag_no_wear_info', 'flag_missing_starthour', 'flag_missing_endhour']

            if 'flag_unable_to_process' in df.columns:
                remaining_columns += ['flag_unable_to_process']
            pampro_column_order.extend(remaining_columns)


        final_order = pampro_column_order
        df = df[final_order]

    # --- SECTION TO RUN HOUSEKEEPING AND DROP FILES NOT NEEDED IN FINAL RELEASE --- #
    if config.get('run_housekeeping').lower() == 'yes':
        print(Fore.GREEN + "RUNNING HOUSEKEEPING AND DROPPING FILES THAT ARE NOT NEEDED IN FINAL RELEASE" + Fore.RESET)
        #df = df[(~df['filename'].isin(filenames_to_remove))]
        df = df[(~df['filename'].isin(config.get('filenames_to_remove')))]

    # Counting number of files/IDs and print the IDs
    count_number_ids = df['id'].count()
    print(Fore.YELLOW + f'Total number of {print_message} in {release_level} release file: {count_number_ids}' + Fore.RESET)
    if release_level == 'summary':
        id_column = df['id']
        id_list = id_column.tolist()
        print(Fore.YELLOW + "IDs:" + Fore.RESET)
        for id in id_list:
            print(Fore.YELLOW + id + Fore.RESET)
    if release_level == 'daily':
        filename_column = df['filename']
        filename_list = filename_column.tolist()
        unique_filename = sorted(set(filename_list))


        grouped = df.groupby('filename')['day_number'].count()
        print(Fore.YELLOW + "Filenames and rows/days per ID:" + Fore.RESET)
        for filename in unique_filename:
            count_days = grouped.get(filename, 0)
            print(Fore.YELLOW + f'{filename}:    {count_days} days' + Fore.RESET)

    if release_level == 'hourly':
        id_column = df['id']
        id_list = id_column.tolist()
        unique_ids = sorted(set(id_list))
        id_counts = df['id'].value_counts().sort_index()
        print(Fore.YELLOW + "IDs and number of rows/hours per ID:" + Fore.RESET)
        if config.get('count_prefixes').lower() == '1h':
             PREFIX = 'hours'
        if config.get('count_prefixes').lower() == '1m':
             PREFIX = 'minutes'        
        for id, count in zip(unique_ids, id_counts):
            print(Fore.YELLOW + f'{id:}   {count} files/{PREFIX}' + Fore.RESET)

    # Saving the release file with todays date
    today_date = date.today()
    formatted_date = today_date.strftime("%d%b%Y")
    output_folder = os.path.join(config.get('root_folder'), config.get('releases_folder'), config.get('pc_date'), f'{output_filename}_FINAL_{formatted_date}.csv')
    df.to_csv(output_folder, index=False)

    return df


####################################
# --- CREATING DATA DICTIONARY --- #
####################################
def data_dictionary(df, filename, release_level, pwear, pwear_quad, append_level):
    if df is not None and not df.empty:
        variable_label = {
            "id": "Study ID",
            "filename": "Filename of original raw file"}

        if release_level == 'summary':
            variable_label.update({
                "startdate": "Date of first day of free-living recording",
                "RecordLength": "Number of hours file was recording for"})
        if release_level == 'daily':
            variable_label.update({
                "DATE": "Daily date of wear",
                "day_number": "Consecutive day number in recording",
                "dayofweek": "day of week for index time period"
            })
        if release_level == 'hourly':
            variable_label.update({
                "file_id": "Id of original raw file",
                "DATE": "Date",
                "TIME": "Time",
                "timestamp": "Date and time of index period",
                "DATETIME": "Date and time of index period",
                "DATETIME_ORIG": "Date and time of index period (original)",
                "dayofweek": "Day of the week for index time period",
                "hourofday": "Hour of day for index period",
                "Temperature_mean": "Average temperature (degrees celsius)",
                "Battery_mean": "Average battery level",
                "FLAG_MECH_NOISE": "1 = Flagged as mechanical enmo values. Pwear set to 0",
                "Pwear": "Time integral of wear probability based on ACC",
                "enmo_mean": "Average acceleration (milli-g)",
                "enmo_n": "Epoch level count of how many data points are present).",
                "enmo_missing": "Epoch level count of how many data points include non-wear",
                "enmo_sum": "Sum of enmo (milli-g)",
                "days_of_data_processed": "Total number of days of data processed per file",
                "freeday_number": "Index for 24 hour wear periods relative to start of the measurement",
                "generic_first_timestamp": "First date timestamp",
                "generic_last_timestamp": "Last date timestamp",
                "postend": "0=The timestamp is not after the last_file_timepoint. 1=The timestamp is after the last_file_timepoint. ",
                "prestart": "0=Timestamp is not before the first_file_timepoint. 1=The timestamp is before the first_file_timepoint.",
                "valid": "TRUE=The timestamp is valid (not before the first_file_timepoint and not after the last_file_timepoint. FALSE=The timestamp is not valid.",
                "day_valid": "0=Timestamp is outside wear period specified in wear log. 1=Timestamp is within wear period specified in wear log. 2=No wear log."
            })

            if any(col.startswith('hpfvm') for col in df.columns):
                variable_label.update({
                    "hpfvm_mean": "Average acceleration (milli-g)",
                    "hpfvm_n": "Number of 5 seconds epochs that the device was worn within the hour (This will be 720 if device was worn the whole time and Pwear=1).",
                    "hpfvm_missing": "Number of 5 seconds epochs the device was NOT worn within the hour.",
                    "hpfvm_sum": "Sum of hpfvm (milli-g)"
                })


        quadrants = ['morning', 'noon', 'afternoon', 'night']
        quad_morning_hours = ">0 & <=6 hours"
        quad_noon_hours = ">6 & <=12 hours"
        quad_afternoon_hours = ">12 & <=18 hours"
        quad_night_hours = ">18 & <=24 hours"
        x = "Number of valid hrs during free-living"

        # Generating labels for all consolidated, original and imputed variables.
        if config.get('impute_data').lower() == 'yes':
            variable_type = ['consolidated', 'orig', 'IMP']
            type_label = [' (consolidated)', ' (non-imputed)', ' (imputed)']
        else:
            variable_type = ['']
            type_label = ['']

        # Generating pwear labels for all quandrant + weekday variables:
        all_pwear_labels = {}
        for _type, label in zip(variable_type, type_label):
            pwear_labels = {
                f"Pwear_{_type}": f"Time integral of wear probability based on ACC {label}",
                f"Pwear_morning_{_type}": f"{x}; {quad_morning_hours} {label}",
                f"Pwear_noon_{_type}": f"{x}; {quad_noon_hours} {label}",
                f"Pwear_afternoon_{_type}": f"{x}; {quad_afternoon_hours} {label}",
                f"Pwear_night_{_type}": f"{x}; {quad_night_hours} {label}"}
            if release_level == 'summary':
                pwear_labels.update({
                    f"Pwear_wkday_{_type}": f"{x}; weekday {label}",
                    f"Pwear_wkend_{_type}": f"{x}; weekend day {label}",
                    f"Pwear_morning_wkday_{_type}": f"{x}; {quad_morning_hours}; weekday {label}",
                    f"Pwear_noon_wkday_{_type}": f"{x}; {quad_morning_hours}; weekday {label}",
                    f"Pwear_afternoon_wkday_{_type}": f"{x}; {quad_afternoon_hours} weekday {label}",
                    f"Pwear_night_wkday_{_type}": f"{x}; {quad_night_hours}; weekday {label}",
                    f"Pwear_morning_wkend_{_type}": f"{x}; {quad_morning_hours}; weekend day {label}",
                    f"Pwear_noon_wkend_{_type}": f"{x}; {quad_morning_hours}; weekend day {label}",
                    f"Pwear_afternoon_wkend_{_type}": f"{x}; {quad_afternoon_hours}; weekend day {label}",
                    f"Pwear_night_wkend_{_type}": f"{x}; {quad_night_hours}; weekend day {label}"})

           # Generating enmo_mean and hpfvm mean variables for dictionary:
            pwear_labels.update({
                f"enmo_mean_{_type}": f"Average acceleration (milli-g) {label} "
            })
            all_pwear_labels.update(pwear_labels)

            if any(col.startswith('hpfvm') for col in df.columns):
                pwear_labels.update({
                    f"hpfvm_mean_{_type}": f"Average acceleration (milli-g) {label}"
                })
            all_pwear_labels.update(pwear_labels)
        variable_label.update(all_pwear_labels)

        # Generating threshold dictionary variables for enmo and hpfvm variables
        if config.get('remove_thresholds').lower() == 'no':
            list_variables = ['enmo']
            if any(col.startswith('hpfvm') for col in df.columns):
                list_variables += ['hpfvm']

            for var in list_variables:
                intensity_variables = [col for col in df.columns if col.startswith(var) and 'plus' in col]
                for variables in intensity_variables:
                    parts = variables.split('_')
                    threshold_part = parts[1]
                    threshold = threshold_part.replace("plus", "")
                    label = f"Proportion of time spent above >= {threshold} milli-g"
                    if variables.endswith("consolidated"):
                        label+= " (consolidated)"
                    if variables.endswith("orig"):
                        label += " (non-imputed)"
                    if variables.endswith("IMP"):
                        label += " (imputed)"
                    variable_label[variables] = label

        # Adding pitch and roll variables to data dictionary
        if release_level == 'hourly':
            pitch_roll_var = []
            if any(col.startswith('pitch') for col in df.columns):
                pitch_roll_var += ['pitch']
            if any(col.startswith('roll') for col in df.columns):
                pitch_roll_var += ['roll']
            if pitch_roll_var:
                ends = ['mean', 'std', 'min', 'max']
                for variable in pitch_roll_var:
                    variable_label.update({
                        f"{variable}_mean": f"Average {variable} angle (degrees)",
                        f"{variable}_std": f"Standard deviation of the {variable} angle (degrees)",
                        f"{variable}_min": f"Minimum {variable} angle (degrees)",
                        f"{variable}_max": f"Maximum {variable} angle (degrees)"
                    })
                    Angle_variables = [col for col in df.columns if col.startswith(variable) and not any(col.endswith(end) for end in ends)]
                    for angle_var in Angle_variables:
                        split = angle_var.split('_', 1)
                        threshold_part = split[1]

                        # Split threshold part into 2 to determine if it should be a negative or positive value
                        threshold_values = threshold_part.split('_')
                        threshold_start = int(threshold_values[0])
                        threshold_end = int(threshold_values[1])

                        if threshold_start > threshold_end:
                            threshold_part = f"-{threshold_start}_-{threshold_end}"
                            if threshold_end == 0:
                                threshold_part = f"-{threshold_start}_{threshold_end}"

                        label = f"Proportion of time spent between {variable} angles {threshold_part} degrees"
                        variable_label[angle_var] = label


        # Generating day and hourly pwear, enmo and hpfvm dictionary variables
        if config.get('processing').lower() == 'pampro':
            # daily and hourly enmo and pwear variable are added to dictionary if procesed through pampro and only for summary level
            if append_level == 'summary':
                all_day_labels = {}
                all_hour_labels = {}
                days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
                hours = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24]
                hour_specs = ['0:00-1:00', '1:00-2:00', '2:00-3:00', '3:00-4:00', '4:00-5:00', '5:00-6:00', '6:00-7:00',
                             '7:00-8:00', '8:00-9:00', '9:00-10:00', '10:00-11:00', '11:00-12:00', '12:00-13:00', '13:00-14:00',
                             '14:00-15:00', '15:00-16:00', '16:00-17:00', '17:00-18:00', '18:00-19:00', '19:00-20:00', '20:00-21:00',
                             '21:00-22:00', '22:00-23:00', '23:00-0:00']
                for var in list_variables:
                    for _type, label in zip(variable_type, type_label):
                        for day in days:
                            day_labels = {
                                f"pwear_{day}_{_type}" : f"Pwear by day of week ({day}) {label}",
                                f"{var}_mean_{day}_{_type}" : f"{var} mean by day of week ({day}) {label}"
                            }
                            all_day_labels.update(day_labels)
                        variable_label.update(all_day_labels)

                        for hour, hour_spec in zip(hours, hour_specs):
                            hour_labels = {
                                f"pwear_hour{hour}_{_type}": f"Pwear by hour of day (hour {hour}) ({hour_spec}) {label}",
                                f"{var}_mean_hour{hour}_{_type}": f"{var} mean by hour of day (hour {hour}) ({hour_spec}) {label}"
                            }
                            all_hour_labels.update(hour_labels)

                        variable_label.update(all_hour_labels)

        # Generating metadata dictionary variables
        calibration_labels = {
            "device": "Device serial number",
            "file_start_error": "File error before calibration (single file cal) (mg)",
            "file_end_error": "File error after calibration (single file cal) (mg)",
            "start_error": "File error before calibration (single file cal) (mg)",
            "end_error": "File error after calibration (single file cal) (mg)",
            "calibration_method": "Calibration method applied (offset/scale/temp)",
            "TIME_RESOLUTION": "Time resolution of processed data (minutes)",
            "noise_cutoff": "Threshold set for still bout detection (mg)",
            "noise_cutoff_mg": "Threshold set for still bout detection (mg)",
            "processing_epoch": "Epoch setting used when processing data (sec)",
            "frequency": "Recording frequency in hz",
            "FLAG_ANOMALY": "1 = Anomaly flagged in file",
            "FLAG_AXIS_FAULT": "1 = File had technical issue affecting integrity of data",
            "first_file_timepoint": "First date timestamp of file",
            "last_file_timepoint": "Last date timestamp of file",
            "temp_flag_no_valid_days": "1=No valid days in file."
        }

        # Generating metadata dictionary variables specific to Wave output
        if config.get('processing').lower() == 'wave':
            calibration_labels.update({
            "qc_first_battery_pct": "Battery percentage of device at beginning of data collection",
            "qc_last_battery_pct": "Battery percentage of device at end of data collection",
            "qc_anomalies_total": "Total number of anomalies detected in the file",
            "qc_anomaly_a": "1 = Anomaly a flagged in file. Dealt with during processing.",
            "qc_anomaly_b": "1 = Anomaly b flagged in file. Dealt with during processing.",
            "qc_anomaly_c": "1 = Anomaly c flagged in file. Dealt with during processing.",
            "qc_anomaly_d": "1 = Anomaly d flagged in file. Dealt with during processing.",
            "qc_anomaly_e": "1 = Anomaly e flagged in file. Dealt with during processing.",
            "qc_anomaly_f": "1 = Anomaly f flagged in file. Dealt with during processing.",
            "qc_anomaly_g": "1 = Anomaly g flagged in file. Dealt with during processing."
            })

        # Generating metadata dictionary variables specific to Pampro output
        if config.get('processing').lower() == 'pampro':
            anomalies = ['A', 'B', 'C', 'D', 'E', 'F']
            anomalies_label = {}
            for anom in anomalies:
                anom_label = {f"Anom_{anom}": f"Anomaly {anom} flagged in file. Dealt with during processing"}
                anomalies_label.update(anom_label)
            calibration_labels.update(anomalies_label)

        if config.get('processing').lower() == 'pampro':
            calibration_labels.update({
                "mf_start_error": "File error before calibration (multi file cal)  (mg)",
                "mf_end_error": "File error after calibration (multi file cal) (mg)",
                "calibration_type": "Type of calibration used: Single or Multi file"
            })

        if release_level == 'summary' or release_level == 'daily':
            if config.get('impute_data').lower() == 'yes':
                calibration_labels.update({
                    "include": f'1=Pwear>={pwear} & all Pwear_quads>={pwear_quad}. 2=Pwear>={pwear}, pwear_morning<{pwear_quad} and pwear_noon/afternoon/night>={pwear_quad}.',
                    "imputed": "1=Data imputed between 00:00-06:00 if not worn (proportional to Pwear)"
                })
            else:
                calibration_labels.update({"include": f'1=Pwear>={pwear} & all Pwear_quads>={pwear_quad}'})

        variable_label.update(calibration_labels)

        # Generating dictionary variables if a wear log was used
        if config.get('use_wear_log').lower() == 'yes':
            wear_log_labels = {
                "start": "Start datetime of the Wear Log",
                "end": "End datetime of the Wear Log",
                "flag_no_wear_info": "1=Did not have any wear log information",
                "flag_missing_starthour": "Missing start hour in wear log",
                "flag_missing_endhour": "Missing end hour in wear log"
            }
            variable_label.update(wear_log_labels)
        variable_label.update({"flag_unable_to_process": "1=The file were unable to process (they did not have an hourly/minute level file). Only metadata is included in release."})

        # Ordering labels to match the order of the release file:
        release_df_lower = {col.lower(): col for col in df.columns}
        labels_df_lower = {key.lower(): value for key, value in variable_label.items()}

        ordered_labels = {
            release_df_lower[col.lower()]: labels_df_lower[col.lower()]
            for col in df.columns if col.lower() in labels_df_lower
        }

        df_labels = pd.DataFrame(list(ordered_labels.items()), columns=["Variable", "variabel_label"])

        # Determine if variable is numeric
        isnumeric = df.dtypes.apply(lambda x: 1 if pd.api.types.is_numeric_dtype(x) else 0).reset_index()
        isnumeric.columns = ['Variable', 'isnumeric']
        df_labels = pd.merge(df_labels, isnumeric, on='Variable', how='left')

        # Ordering columns
        df_labels = df_labels[['Variable', 'isnumeric', 'variabel_label']]


        file_path = os.path.join(config.get('root_folder'), config.get('releases_folder'), config.get('pc_date'))
        os.makedirs(file_path, exist_ok=True)
        file_name = os.path.join(file_path, f'Data_Dict_{filename}.csv')
        df_labels.to_csv(file_name, index=False)


#################################
# --- Calling the functions --- #
#################################
if __name__ == '__main__':

    if len(sys.argv) < 2:
        print("Error: No config file provided.")
        sys.exit(1)

    config_path = sys.argv[1]
    config = load_config(config_path)

    print("Loaded config:", config)
    # Now you can use config values inside your script
    config["pc_date"] = datetime.utcnow().isoformat()

    # Preparing summary release file
    if config.get('run_prepare_summary_release').lower() == 'yes':
        print_message("PREPARING A SUMMARY RELEASE FILE")

        summary_df = formatting_file(import_file_name=f"{config.get('sum_output_file')}.csv", release_level='summary',
                                     pwear=config.get('sum_pwear'), pwear_morning=config.get('sum_pwear_morning'), pwear_quad=config.get('sum_pwear_quad'), print_message='files/IDs',
                                     output_filename=config.get('sum_output_file'))
        data_dictionary(df=summary_df, filename=config.get('sum_output_file'), release_level='summary', pwear=config.get('sum_pwear'), pwear_quad=config.get('sum_pwear_quad'), append_level='summary')

    # Preparing daily release file
    if config.get('run_prepare_daily_release').lower() == 'yes':
        print_message("PREPARING A DAILY RELEASE FILE")
        daily_df = formatting_file(import_file_name=f"{config.get('day_output_file')}.csv", release_level='daily',
                                   pwear=config.get('day_pwear'), pwear_morning=config.get('day_pwear_morning'), pwear_quad=config.get('day_pwear_quad'), print_message='rows of data',
                                   output_filename=config.get('day_output_file'))
        data_dictionary(df=daily_df, filename=config.get('day_output_file'), release_level='daily', pwear=config.get('day_pwear'), pwear_quad=config.get('day_pwear_quad'), append_level='daily')

    # Preparing hourly release file
    if config.get('run_prepare_hourly_release').lower() == 'yes' or config.get('run_prepare_minute_level_release').lower() == 'yes':
        if config.get('count_prefixes').lower() == '1h':
             print_message("PREPARING A HOURLY RELEASE FILE")
        if config.get('count_prefixes').lower() == '1m':
             print_message("PREPARING A MINUTE LEVEL RELEASE FILE")


        hourly_df = formatting_file(import_file_name=f"{config.get('hour_output_file')}.csv", release_level='hourly',
                                    pwear=None, pwear_morning=None, pwear_quad=None, print_message='rows of data', output_filename=config.get('hour_output_file'))
        data_dictionary(df=hourly_df, filename=config.get('hour_output_file'), release_level='hourly', pwear=None, pwear_quad=None, append_level='hourly')



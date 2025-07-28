############################################################################################################
# This file creates a filelist of the IDs which have a 1h level data as well as analysis_meta file.
# Author: CAS
# Date: 31/05/2024 (Started)
# Version: 1.0 Translated from Stata code
############################################################################################################
# --- IMPORTING PACKAGES --- #
import os
import pandas as pd
from wavepostprocessing.config import load_config
#from config import load_config
from colorama import Fore
import sys

# --- CREATING SPECIFIC FOLDERS WITHIN THE RESULTS FOLDER FOR HOUSING INDIVIDUAL FILES --- #
def create_folders():
    # Creating a summary folder in the results folder. Skipping this if the folder already exists.
    try:
        os.makedirs(os.path.join(config.get('root_folder'), config.get('results_folder'), config.get('summary_folder')))
    except FileExistsError:
        pass

    for folder in [config.get('individual_partpro_f'), config.get('individual_sum_f'), config.get('individual_daily_f'), config.get('individual_trimmed_f')]:
        # Creating summary files folders in the summary folder. Skipping this if the folders already exists.
        try:
            os.makedirs(os.path.join(config.get('root_folder'), config.get('results_folder'), config.get('summary_folder'), folder))
            # Creating a folder in each of the folders above indicating the processing resolution level.
            os.makedirs(os.path.join(config.get('root_folder'), config.get('results_folder'), config.get('summary_folder'), folder, config.get('time_res_folder')))
        except FileExistsError:
            pass

    # Creating a filelist folder in the results folder. Skipping this if the folder already exists.
    try:
        os.makedirs(os.path.join(config.get('root_folder'), config.get('results_folder'), config.get('filelist_folder')))
    except FileExistsError:
        pass

    # Creating folders for feedback graphs in the feedback folder. Skipping this if the folders already exists.
    for folder in ["Feedback_graphs", "Files_for_plotting"]:
        try:
            os.makedirs(os.path.join(config.get('root_folder'), config.get('feedback_folder'), folder))
        except FileExistsError:
            pass


# --- CREATING A FILELIST OF ALL FILES IN THE RESULTS FOLDER --- #
def create_filelist():

    # Deleting all past filelists in results folder
    if os.path.exists(os.path.join(config.get('root_folder'), config.get('results_folder'), "filelist.txt")):
        os.remove(os.path.join(config.get('root_folder'), config.get('results_folder'), "filelist.txt"))

    # Creating a new filelist in the results folder (The code is different for mac and windows, change pc type in the beginning of script)
    os.chdir(os.path.join(config.get('root_folder'), config.get('results_folder')))
    if config.get('pc_type').lower() == "windows":
        os.system('dir /b *csv > filelist.txt')
    elif config.get('pc_type').lower() == "mac":
        os.system('ls /b *csv > filelist.txt')
    elif config.get('pc_type').lower() == "linux":
        os.system('ls *csv > filelist.txt')

# --- REMOVING FILES THAT ARE NEVER TO BE CONSOLIDATED --- #
def remove_files():
    # Reading in the filelist
    os.chdir(os.path.join(config.get('root_folder'), config.get('results_folder')))    
    filelist_df = pd.read_csv('filelist.txt', header=None, names=['v1'])
    filelist_df['file_type'] = filelist_df['v1']

    # Running the consolidation on only specific runs -  creating a temp_keep variable and replacing the value with true if the filename contains any of the specifies prefixes:
    filelist_df['temp_keep'] = False
    for prefix in config.get('sub_set_prefixes'):
        filelist_df['temp_keep'] = filelist_df['temp_keep'] | filelist_df['v1'].str.contains(prefix, regex=True)

    # Only keeping rows where temp_keep is true
    filelist_df = filelist_df[filelist_df['temp_keep']]

    # GENERATING A FILE_TYPE VARIABLE TO INDICATE THE DIFFERENT FILES TO BE CONSOLIDATED:
    # Keeping the first underscore in file_type and splitting on the second to find file_type:
    filelist_df[['file_type', 'file_type2']] = filelist_df['file_type'].str.split('_', n=1, expand=True)

    # Renaming variables and specifying which variables to keep
    filelist_df['filename'] = filelist_df['v1']
    filelist_df = filelist_df[['filename', 'temp_keep', 'file_type']]

    # GENERATING A FILE ID TO MAKE SURE ALL FILES FOR ONE ID ARE GROUPED TOGETHER #
    # Creating a new variable called filename_temp and replacing it with the variable filename but without .csv
    filelist_df['filename_temp'] = filelist_df['filename'].str.replace(".csv", "")

    # Extracting the file type from the filename_temp (The lambda function is used because the change is row specific, axis=1 indicates to apply the function to each row)
    filelist_df['filename_temp'] = filelist_df.apply(
        lambda row: row['filename_temp'].replace(f"{row['file_type']}_", ""), axis=1
    )

    # Tagging duplicates to see which files have the metadata but no hour or minute dataset (Which have failed due to calibration) - These will be tagged as False
    filelist_df['duplicate'] = filelist_df['filename_temp'].duplicated(keep=False)

    # Creating a copy of the dataset:
    original_df = filelist_df.copy()

    # Keeping only the files that haven't calibrated and exporting as a list if there are any:
    filelist_df = filelist_df[(filelist_df['file_type'] == "metadata") & (filelist_df['duplicate'] == False)]
    filelist_df = filelist_df.drop(columns=['duplicate'])

    if len(filelist_df) != 0:
        os.chdir(os.path.join(config.get('root_folder'), config.get('results_folder'), config.get('filelist_folder')))
        filelist_df.to_csv("No_Analysis_Files.txt", index=False, header=True, mode='w')

    # Restoring dataset and keep only files that have calibrated:
    filelist_df = original_df.copy()
    filelist_df = filelist_df[(filelist_df['duplicate'] == True)]
    filelist_df = filelist_df.drop(columns=['duplicate'])

    filelist_df = filelist_df.sort_values(by='filename_temp')
    filelist_df['id'] = filelist_df['filename_temp'].str.split('_').str[0]

    # OPENING THE FINAL DATASET FROM LAST PROCESS TO KNOW WHAT HAS BEEN PROCESSED ALREADY
    if config.get('only_new_files').lower() == "yes":
        try:
            summary_file_path = os.path.join(config.get('root_folder'), config.get('results_folder'), config.get('summary_folder'), f"{config.get('project')}_SUMMARY_MEANS.csv")

            # Opening the file if present and only keeping 1 ID per person
            if os.path.exists(summary_file_path):
                last_process_df = pd.read_csv(summary_file_path)
                last_process_df = last_process_df.filter(items=['id'])
                last_process_df = last_process_df.drop_duplicates(subset=['id'], keep='first')
                last_process_df = last_process_df.rename(columns={'id': 'filename_temp'})

                # Merging with new files and only keeping the ones, that haven't been processed previously
                merged_df = pd.merge(filelist_df, last_process_df, on='filename_temp', how="outer", indicator=True)
                index_merged = merged_df[(merged_df['_merge'] == 'both')].index
                merged_df.drop(index_merged, inplace=True)
                merged_df = merged_df.drop(columns=['_merge', 'id'])
                filelist_df = merged_df.copy()
        except FileNotFoundError:
            pass

    new_files_to_proces = len(filelist_df)
    if new_files_to_proces < 1:
        print(Fore.RED + "There are no new files to post process. \n Make sure that files processed through wave are saved in the _results folder before re-running the post processing.")
        input("If run in PyCharm: Stop the script from running by clicking the red stop button at the right hand top corner. \n If run at batch file: Close the window and rerun when new files have been saved in _results folder." + Fore.RESET)

    filelist_df['serial'] = filelist_df.groupby('filename_temp').ngroup() + 1
    filelist_df.sort_values(by='serial', inplace=True)

    #num_splits=10
    num_splits = config.get('num_filelist', 10)

    # Idea from the following for uniform distribution
    #k, m = divmod(len(df), n)
    #dfs = [df.iloc[i*k + min(i, m):(i+1)*k + min(i+1, m)] for i in range(n)]
    #min(i, m) ensures that the first m parts have one extra row to handle the remainder.


    # Calculate the size of each part
    k, m = divmod(round(len(filelist_df)/2), num_splits)

    # Create the list of DataFrames
    filelist_dfs = [filelist_df.iloc[i*k*2 + min(i, m)*2:(i+1)*k*2 + min(i+1, m)*2] for i in range(num_splits)]

    # Now filelist_dfs is a list of DataFrames
    for i, filelist_df in enumerate(filelist_dfs):
      #print(f"DataFrame {i+1}:\n{d}\n")

      output_file = os.path.join(config.get('root_folder'), config.get('results_folder'), config.get('filelist_folder'), f'filelist{i}.txt')
      filelist_df.to_csv(output_file, sep='\t', index=False)

    #output_file = os.path.join(config.get('root_folder'), config.get('results_folder'), config.get('filelist_folder'), 'filelist.txt')
    #filelist_df.to_csv(output_file, sep='\t', index=False)


if __name__ == '__main__':

    if len(sys.argv) < 2:
        print("Error: No config file provided.")
        sys.exit(1)

    config_path = sys.argv[1]
    config = load_config(config_path)

    print("Loaded config:", config)
    # Now you can use config values inside your script

    create_folders()
    create_filelist()
    remove_files()

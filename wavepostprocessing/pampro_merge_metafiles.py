############################################################################################################
# This file merge the meta data files from files that have been processed through Pampro and merges them into 1 metadata file
# This script only needs running when accelerometer files are run through Pampro, as pampro generates multiple meta files per person (whilst Wave only generates 1)
# Author: cas254
# Date: 07/10/2024
# Version: 1.0
############################################################################################################

# --- IMPORTING PACKAGES --- #
import os
import pandas as pd
#import config
from wavepostprocessing.config import load_config
#from config import load_config
import sys

# --- GETTING LIST OF META FILES --- #
def list_files():
    '''
    The function creates a filelist, keeps files with meta in, extract filetype and id and then group the files by id.
    :param results_folder:
    :return: groups
    '''
    files = os.listdir(os.path.join(config.get('root_folder'), config.get('results_folder')))
    df = pd.DataFrame(files, columns=['filename'])
    df = df[df['filename'].str.contains('meta') & ~df['filename'].str.startswith('metadata')]
    df[['file_type', 'id']] = df['filename'].str.split(r'(?<=meta)', expand=True)
    df['id'] = df['id'].str.lstrip('_').str.replace('.csv', '', regex=False)

    # Grouping files with same id
    groups = df.groupby('id')
    return groups

# --- MERING META FILES WITH SAME ID INTO 1 METADATA file --- #
def merge_meta(groups, variables):
    for id, group in groups:

        # Defining output path:
        output_file = os.path.join(config.get('root_folder'), config.get('results_folder'), f'metadata_{id}.csv')

        # Checking if metadata file already exist for each id:
        if os.path.exists(output_file):
            print(f'metadata already exist for {id}. Skipping')
            continue

        # Merge the metafiles for the id's that does not already have a metadata file
        file_paths = {}
        missing_file = False
        for var in variables:
            try:
                file_name = group[group['file_type'].str.contains(var)]['filename'].values[0]
                file_paths[f'{var}_file'] = os.path.join(config.get('root_folder'), config.get('results_folder'), file_name)
            except IndexError:
                print(f'File {var} is not found for {id}. Skipping this.')
                missing_file = True
                break
        if missing_file:
            continue # if any required file is missing it will skip to the next group

        # Reading analysis_meta file
        analysis_df = pd.read_csv(file_paths['analysis_meta_file'])
        if 'file_name' not in analysis_df:
            analysis_df['file_filename'] = id

        # Merge with qc_meta
        qc_meta_df = pd.read_csv(file_paths['qc_meta_file'])

        if 'file_name' not in qc_meta_df:
            qc_meta_df['file_filename'] = id
        columns_to_keep = [col for col in qc_meta_df.columns if col.startswith('QC')] + ['file_filename']
        qc_meta_df = qc_meta_df[columns_to_keep]
        merged_df = pd.merge(analysis_df, qc_meta_df, how='outer', on='file_filename')

        # Outputting the metadata file
        merged_df.to_csv(output_file, index=False)




if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Error: No config file provided.")
        sys.exit(1)

    config_path = sys.argv[1]
    config = load_config(config_path)

    print("Loaded config:", config)
    # Now you can use config values inside your script
    
    groups = list_files()
    merge_meta(groups, ['analysis_meta', 'file_meta', 'qc_meta'])

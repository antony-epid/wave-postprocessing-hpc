import os
import argparse
from colorama import Fore
from wavepostprocessing.config import load_config, print_message
from wavepostprocessing.batch_processing import submit_jobs, run_script
#from config import load_config, print_message
#from batch_processing import submit_jobs, run_script
#import sys

def print_message(message):
    print(Fore.GREEN + message + Fore.RESET)

def main():
    parser = argparse.ArgumentParser(description="WaveProcessing CLI")
    parser.add_argument("directory", nargs="?", default=".", help="This is a required positional argument to locate the directory containing config.yaml")
    parser.add_argument('--budget', default=None, help='Optional argument, defaults to None')
    args = parser.parse_args()

    config = load_config(args.directory)
    print(config)
    #print(config['run_pampro_merge_metafiles'])
    #sys.exit(1)
    jid = None

    if args.budget is None: 
       mybudgacc='BRAGE-SL3-CPU'
    else:
       mybudgacc=args.budget

    if config.get("processing").lower() == "pampro":
        if config.get("run_pampro_merge_metafiles").lower() == 'yes':
            print_message("Merging Metafiles")
            jid = submit_jobs("wavepostprocessing.pampro_merge_metafiles", args.directory, arrsize=1,num_cpu=1, jid=jid, budgacc=mybudgacc)
        if config.get("run_pampro_collate_anomalies").lower()  == 'yes':
            print_message("Collating Anomalies")
            jid = submit_jobs("wavepostprocessing.pampro_collate_anomalies", args.directory,  arrsize=1,num_cpu=1, jid=jid, budgacc=mybudgacc)

    if config.get("run_filelist_generation").lower()  == 'yes':
        print_message("Creating Filelist")
        jid = submit_jobs("wavepostprocessing.filelist_generation", args.directory,  arrsize=1,num_cpu=1, jid=jid, budgacc=mybudgacc)

    if config.get("run_generic_exh_postprocessing").lower()  == 'yes':
        print_message("Running Generic Exhaustive Postprocessing")
        #jid = submit_jobs("generic_exh_postprocessing.py", args.directory, arrsize=10,num_cpu=1, jid=jid, budgacc=mybudgacc)
        jid = submit_jobs("wavepostprocessing.generic_exh_postprocessing", args.directory, arrsize=config['num_filelist'],num_cpu=1, jid=jid, budgacc=mybudgacc)

    if config.get("run_collapse_results_to_summary").lower() == 'yes' or config.get("run_collapse_results_to_daily").lower() == 'yes':
        print_message("Collapsing Results")
        #jid = submit_jobs("collapse_results.py", args.directory, arrsize=10,num_cpu=1, jid=jid, budgacc=mybudgacc)
        jid = submit_jobs("wavepostprocessing.collapse_results", args.directory, arrsize=config['num_filelist'],num_cpu=1, jid=jid, budgacc=mybudgacc)

    if config.get("run_append_summary_files").lower() == 'yes' or config.get("run_append_daily_files").lower() == 'yes' or config.get("run_append_hourly_files").lower() == 'yes':
        print_message("Appending Files")
        #jid = submit_jobs("appending_files.py", args.directory, arrsize=1,num_cpu=1, jid=jid, budgacc=mybudgacc)
        jid = submit_jobs("wavepostprocessing.appending_files", args.directory, arrsize=1,num_cpu=1, jid=jid, budgacc=mybudgacc)

    if config.get("run_verification_checks").lower() == 'yes':
        print_message("Running Verification Checks")
        #jid = submit_jobs("verification_checks.py", args.directory, arrsize=1,num_cpu=1, jid=jid, budgacc=mybudgacc)
        jid = submit_jobs("wavepostprocessing.verification_checks", args.directory, arrsize=1,num_cpu=1, jid=jid, budgacc=mybudgacc)

    if config.get("run_prepare_summary_release").lower() == 'yes' or config.get("run_prepare_daily_release").lower() == 'yes' or config.get("run_prepare_hourly_release").lower() == 'yes' or  config.get("run_prepare_minute_level_release").lower() == 'yes':
        print_message("Preparing Release Files")
        #jid = submit_jobs("prepare_releases.py", args.directory,  arrsize=1,num_cpu=1, jid=jid, budgacc=mybudgacc)
        jid = submit_jobs("wavepostprocessing.prepare_releases", args.directory,  arrsize=1,num_cpu=3, jid=jid, budgacc=mybudgacc)

    print_message(Fore.BLUE + "WaveProcessing completed the job submission successfully.")

if __name__ == "__main__":
    main()



import subprocess
import os

def submit_jobs(script_name, config_path, arrsize=10, num_cpu=1, jid=None, budgacc="BRAGE-SL3-CPU"):
    """
    Submits a batch job to the HPC system.
    :param script_name: The script name (e.g., "collapse_results.py")
    :param arrsize: Number of array jobs
    :param num_cpu: Number of CPUs per task
    :param jid: Job dependency (if any)
    :param budgacc: Budget account for SLURM
    :param config_path: Path to config file
    """

    venv_path = os.environ.get('VIRTUAL_ENV')
    activate_path = os.path.join(venv_path, "bin", "activate")
    if not venv_path:
       raise RuntimeError("No virtual environment detected. Please activate one before running.")

    # Get the absolute path to the script inside 'scripts/' directory
    #script_path = os.path.join(os.path.dirname(__file__), "scripts", script_name)
    #script_path = os.path.join(os.path.dirname(__file__), script_name)
    script_path =  script_name
    script_submit = os.path.join(os.path.dirname(__file__), "submit_wavejobs.sh")
    #script_path = "wavepostprocessing.scripts." +  script_name[:-3]

    ## Ensure the script exists
    #if not os.path.exists(script_path):
    #    raise FileNotFoundError(f"Error: The script {script_path} does not exist.")

    #cmdargs = [f"--account={budgacc}", f"--array=1-{arrsize}", f"--cpus-per-task={num_cpu}", "--time=00:20:00"]
    cmdargs = [f"--account={budgacc}", f"--array=1-{arrsize}", f"--cpus-per-task={num_cpu}", "--time=00:40:00"]


    #sbatch_command = ["sbatch"] + cmdargs + (["--depend=afterany:" + jid] if jid else []) + ["submit_wavejobs.sh", script_path, config_path]
    sbatch_command = ["sbatch"] + cmdargs + (["--depend=afterany:" + jid] if jid else []) + [script_submit, script_path, config_path, activate_path]

    try:
        output = subprocess.check_output(sbatch_command).decode().strip()
        job_id = output.split()[-1]
        print(f"Job submitted successfully with ID: {job_id}")
        return job_id
    except subprocess.CalledProcessError as e:
        print(f"Error submitting job: {e}")
        return None

def run_script(script):
    # Get the absolute path to the script inside 'scripts/' directory
    script_path = os.path.join(os.path.dirname(__file__), "scripts", script)
    if not os.path.exists(script_path):
        print(f"Error: The script {filelist_script_path} does not exist.")
        return
    subprocess.run([sys.executable, script_path], check=True)


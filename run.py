import sys
import dotenv
import os
import yaml
import psutil
import subprocess
from multiprocessing import Process
from logging import Logger, config, getLogger
from pathlib import Path
from time import sleep
from src.scrapper import Scrapper
from src.db import DB
from src.utils import ScrapperException
from src.contracts import JobData, Singleton


# Dynamic Inputs
# KEYWORDS = ["python data engineer","backend python software engineer","cloud engineer"]
KEYWORDS = ["python data engineer","backend python software engineer","cloud engineer"]
MAX_NUMBER_OF_JOBS = 100
HEADLESS = True
LOAD_TIMEOUT = 50
USER_DATA_DIR = os.environ['CHROME_PROFILE']
HIGH_CPU_THRESHOLD = 85
MAX_HIGH_CPU_COUNT = 6
MAX_SCRAPPER_RESTART_ATTEMPTS = 6


class Runner(metaclass=Singleton):
    _logger: Logger
    _job_data: JobData

    def __init__(self):
        # Load environment variables
        dotenv.load_dotenv(".env")
        # Initialize logger config
        for folder in [
            os.environ["LOG_FOLDER"],
            os.environ["BACKUP_FOLDER"],
            os.environ["OUTPUT_FOLDER"]
            ]:
            Path(folder).mkdir(exist_ok=True)
        logging_config_file_name = "src/logging_local.yml"
        with open(logging_config_file_name, 'r') as logging_config_file:
            config.dictConfig(yaml.load(logging_config_file, Loader=yaml.FullLoader))
        # Get the specific logger for scrapper #TODO: We later get specific logs for each component
        self._logger = getLogger("scrape")
        # Initialize database
        self._job_data = DB(db_name=os.environ["DB_NAME"],output_folder=os.environ['OUTPUT_FOLDER'])
    

    def run_scrapper(self):
        self._logger.info("---------------- Start a new crawl process ----------------")
        # Initialize scrapper
        scrapper = Scrapper(
            job_data=self._job_data,
            max_n_jobs=MAX_NUMBER_OF_JOBS,
            logger=self._logger,
            headless=HEADLESS,
            load_timeout=LOAD_TIMEOUT,
            user_data_dir=USER_DATA_DIR
        )

        # Run
        scrapper.sign_in()
        for keyword in KEYWORDS:
            for _ in range(100):
                try:
                    scrapper.manage_and_run(keyword)
                    break
                except ScrapperException as e:
                    match e.kind:
                        case "max_attempts":
                            self._logger.critical(f"Maximum attempt times for persisting scrapper is reached. Exiting!")
                            sys.exit(1)
                        case "webdriver":
                            self._logger.error(f"Webdriver error occurred. Trying to persist.")
                            scrapper.re_init_driver()
                            # If we've already signed-in, we don't need to repeat it
                            # scrapper.sign_in()
                        case _:
                            self._logger.critical(f"Unknown error occurred from scrapper. Exiting!")
                            sys.exit(1)
        self._logger.info("---------------- Crawl process finished successfully! ----------------")

def run_cpulimit(file_path:str="/home/ubuntu/cpulimit-all.sh",proc_name="chrome",cpu_pct=20):
    if not os.path.exists(file_path):
        print(f"cpu_limit file path does not exist. path:{file_path}")
        return None
    command = ["bash",file_path,"-l",str(cpu_pct),"-e",proc_name,"--max-depth=3","--watch-interval=2","-q"]
    try:
        proc = subprocess.Popen(command,shell=False,stderr=subprocess.PIPE,stdout=subprocess.DEVNULL)
    except:
        # if the command is invalid or the file doesn't exist it throws an error
        return None
    # Check if we got errors from stderr
    # if proc.stderr.readline()!=b"":
    #     proc.kill()
    #     return None
    return proc

if __name__ == "__main__":
    runner = Runner()
    cpu_limit_process = None
    # cpu_limit_process = run_cpulimit("/home/ubuntu/cpulimit-all.sh")
    if cpu_limit_process is None:
        print("Error in running cpulimit!")
    for r in range(MAX_SCRAPPER_RESTART_ATTEMPTS):
        print(f"Attempt number {r+1} in starting the process")
        p = Process(target=runner.run_scrapper)
        p.start()
        sleep(5)
        count = 0
        while p.is_alive():
            sleep(2)
            if psutil.cpu_percent(0.1) > HIGH_CPU_THRESHOLD:
                count += 1
                print(f"Detected high cpu usage. Count={count}")
            else:
                count = 0
            if count > MAX_HIGH_CPU_COUNT:
                print("High cpu threshold exceeded. Killing the process")
                for proc in psutil.process_iter():
                    if proc.name().find("chrome"):
                        proc.kill()
                p.kill()
    if cpu_limit_process is not None:
        cpu_limit_process.kill()
        
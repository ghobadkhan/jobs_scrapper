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
from linkedIn_selenium.scrapper import Scrapper
from linkedIn_selenium.db import DB
from linkedIn_selenium.utils import ScrapperException
from linkedIn_selenium.contracts import JobData, Singleton


# Dynamic Inputs
# KEYWORDS = ["python data engineer","backend python software engineer","cloud engineer"]
KEYWORDS = ["python data engineer","backend python software engineer","cloud engineer"]
MAX_NUMBER_OF_JOBS = 100
HEADLESS = True
LOAD_TIMEOUT = 50
USER_DATA_DIR = os.environ['CHROME_PROFILE']


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
        logging_config_file_name = "linkedIn_selenium/logging_local.yml"
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

def run_cpulimit(file_path:str="~/cpulimit-all.sh"):
    if not os.path.exists(file_path):
        print(f"cpu_limit file path does not exist. path:{file_path}")
        return None
    command = f"{file_path} -l 20 -e chrome --max-depth=3 --watch-interval=1"
    return subprocess.Popen(command)

if __name__ == "__main__":
    runner = Runner()
    high_cpu_count = 6
    cpu_limit_process = run_cpulimit()
    for r in range(6):
        print(f"Attempt {r} in starting the process")
        p = Process(target=runner.run_scrapper)
        p.start()
        sleep(5)
        high_cpu_pct_threshold = 85
        while p.is_alive():
            sleep(2)
            if psutil.cpu_percent() > high_cpu_pct_threshold:
                high_cpu_count += 1
                print(f"Detected high cpu usage. Count={high_cpu_count}")
            if high_cpu_count > 5:
                print("High cpu threshold exceeded. Killing the process")
                p.kill()
    if cpu_limit_process is not None:
        cpu_limit_process.terminate()
        
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
from ast import literal_eval
from src.scrapper import Scrapper
from src.db import DB
from src.utils import ScrapperException
from src.contracts import JobData, Singleton


# Config the logger. ** Must be done before all logging initializations
logging_config_file_name = "src/logging_local.yml"
with open(logging_config_file_name, 'r') as logging_config_file:
    config.dictConfig(yaml.load(logging_config_file, Loader=yaml.FullLoader))
# Load environment variables
dotenv.load_dotenv(".env")

class Runner(metaclass=Singleton):
    _logger: Logger
    _job_data: JobData

    def __init__(self):
        # Initialize logger config
        for folder in [
            os.environ["LOG_FOLDER"],
            os.environ["BACKUP_FOLDER"],
            os.environ["OUTPUT_FOLDER"]
            ]:
            Path(folder).mkdir(exist_ok=True)
        # Get the specific logger for scrapper #TODO: We later get specific logs for each component
        self._logger = getLogger("scrape")
        # Initialize database
        self._job_data = DB(db_name=os.environ["DB_NAME"],output_folder=os.environ['OUTPUT_FOLDER'])
    
    def run_scrapper(self):
        self._logger.info("---------------- Start a new crawl process ----------------")
        # Initialize scrapper
        scrapper = Scrapper(
            job_data=self._job_data,
            max_n_jobs=int(os.environ['MAX_NUMBER_OF_JOBS']),
            logger=self._logger,
            headless=literal_eval(os.environ['HEADLESS']),
            load_timeout=int(os.environ['LOAD_TIMEOUT']),
            user_data_dir=os.environ['CHROME_PROFILE']
        )

        # Run
        scrapper.sign_in()
        for keyword in literal_eval(os.environ['QUERIES']):
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
        scrapper.driver.quit()
        sys.exit(0)

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

def run_with_proc_monitor():
    runner = Runner()
    logger = getLogger("procmon")
    cpu_limit_process = None
    high_cpu_threshold = float(os.environ['HIGH_CPU_THRESHOLD'])
    if high_cpu_threshold > 0:
        logger.info("Initializing cpulimit")
        cpu_limit_process = run_cpulimit("/home/ubuntu/cpulimit-all.sh")
        if cpu_limit_process is None:
            logger.error("Error in running cpulimit!")
    for r in range(int(os.environ['MAX_SCRAPPER_RESTART_ATTEMPTS'])):
        logger.info(f"Attempt number {r+1} in starting the process")
        p = Process(target=runner.run_scrapper)
        p.start()
        sleep(5)
        count = 0
        while p.is_alive():
            if high_cpu_threshold > 0  and psutil.cpu_percent(0.5) > high_cpu_threshold:
                count += 1
                logger.warning(f"Detected high cpu usage. Count={count}")
            else:
                count = 0
            if count > int(os.environ['MAX_HIGH_CPU_COUNT']):
                print("High cpu threshold exceeded. Killing the process")
                for proc in psutil.process_iter():
                    if proc.name().find("chrome"):
                        proc.kill()
                p.kill()
        sleep(5)
        if p.exitcode and p.exitcode >= 0:
            logger.info("Process is exited without a problem")
            break
    if cpu_limit_process is not None:
        logger.info("Stopping cpulimit")
        cpu_limit_process.kill()
    logger.info("Done")


if __name__ == "__main__":
    run_with_proc_monitor()
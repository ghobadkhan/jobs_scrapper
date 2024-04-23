import sys
import dotenv
import os
import yaml
from linkedIn_selenium.scrapper import Scrapper
from linkedIn_selenium.db import DB
from pathlib import Path
from logging import config, getLogger
from linkedIn_selenium.utils import ScrapperException

# Dynamic Inputs
# KEYWORDS = ["python data engineer","backend python software engineer","cloud engineer"]
KEYWORDS = ["python data engineer","backend python software engineer","cloud engineer"]
MAX_NUMBER_OF_JOBS = 100
HEADLESS = True
LOAD_TIMEOUT = 50
USER_DATA_DIR = os.environ['CHROME_PROFILE']


# Initialize logger config
Path("log").mkdir(exist_ok=True)
logging_config_file_name = "linkedIn_selenium/logging_local.yml"
with open(logging_config_file_name, 'r') as logging_config_file:
    config.dictConfig(yaml.load(logging_config_file, Loader=yaml.FullLoader))
# Get the specific logger for scrapper #TODO: We later get specific logs for each component
logger = getLogger("scrape")

logger.info("---------------- Start a new crawl process ----------------")

# Initialize database
job_data = DB(db_name=os.environ["DB_NAME"],output_folder=os.environ['OUTPUT_FOLDER'])


# Load environment variables
dotenv.load_dotenv(".env")

# Initialize scrapper
scrapper = Scrapper(
    job_data=job_data,
    max_n_jobs=MAX_NUMBER_OF_JOBS,
    logger=logger,
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
                    logger.critical(f"Maximum attempt times for persisting scrapper is reached. Exiting!")
                    sys.exit(1)
                case "webdriver":
                    logger.error(f"Webdriver error occurred. Trying to persist.")
                    scrapper.re_init_driver()
                    # If we've already signed-in, we don't need to repeat it
                    # scrapper.sign_in()
                case _:
                    logger.critical(f"Unknown error occurred from scrapper. Exiting!")
                    sys.exit(1)
logger.info("---------------- Crawl process finished successfully! ----------------")
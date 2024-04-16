import dotenv
import os
import yaml
from linkedIn_selenium.scrapper import Scrapper
from linkedIn_selenium.db import DB
from ast import literal_eval
from pathlib import Path
from logging import config, getLogger

# Dynamic Inputs
KEYWORDS = ["python data engineer","backend python software engineer","cloud engineer"]
MAX_NUMBER_OF_JOBS = 500
HEADLESS = False

# Get the base directory path
base_path = os.path.dirname(os.path.abspath(__file__))

# Load environment variables and parse MY_SKILLS into a list
dotenv.load_dotenv(".env")
if "MY_SKILLS" in os.environ and os.environ["MY_SKILLS"]:
    os.environ["MY_SKILLS"] = literal_eval(os.environ["MY_SKILLS"])

# Initialize database
job_data = DB(db_name=os.environ["DB_NAME"],output_folder=os.environ['OUTPUT_FOLDER'])

# Initialize logger config
Path("log").mkdir(exist_ok=True)
logging_config_file_name = f'{base_path}/logging_local.yml'
with open(logging_config_file_name, 'r') as logging_config_file:
    config.dictConfig(yaml.load(logging_config_file, Loader=yaml.FullLoader))

# Get the specific logger for scrapper #TODO: We later get specific logs for each component
logger = getLogger("scrape")

# Initialize scrapper
scrapper = Scrapper(
    job_data=job_data,
    max_n_jobs=MAX_NUMBER_OF_JOBS,
    logger=logger,
    headless=HEADLESS
)

# Run
logger.info("---------------- Start a new crawl process ----------------")
scrapper.sign_in()
for keyword in KEYWORDS:
    scrapper.crawl(keyword)
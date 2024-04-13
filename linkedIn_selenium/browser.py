import re
import sys
import pandas as pd
import os
import yaml
import dotenv
from pathlib import Path
from logging import Logger, config, getLogger
from typing import List
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options
from datetime import datetime, timedelta
from time import sleep
from ast import literal_eval
from .contracts import JobData
from .db import DB
from .utils import retry
from .matcher import produce_match_columns

job_id_pattern = re.compile(r".*view\/(\d*).*")
extract_number_pattern = re.compile(r"\D*(\d*)\D*")
post_time_pattern = re.compile(r".* (.*?)s? ago")
skills_text_pattern = re.compile(r"(.*)\n?.*")
driver: webdriver.Chrome
base_path = os.path.dirname(os.path.abspath(__file__))
logger:Logger = getLogger()
# Load environment values into a dict variable
env: dict = dotenv.dotenv_values(".env")
env["MY_SKILLS"] = literal_eval(env["MY_SKILLS"])
job_data:JobData = DB(db_name=env["DB_NAME"],output_folder=env['OUTPUT_FOLDER'])

def setup_webdriver(disable_extension=True,headless=True, load_timeout=12,debug_address:str|None=None):
	#TODO: Load options from a file or other external source
	options = Options()
	if debug_address is None:
		options.add_argument(f"user-data-dir={env['CHROME_PROFILE']}")
		options.add_argument("disable-infobars")
		# Don't enable the extension for crawling from linkedin. We'll use the extension later
		# for auto-fill (hopefully)
		if disable_extension:
			options.add_argument("--disable-extensions")
		if headless:
			options.add_argument("--headless")
	else:
		# Example: google-chrome --remote-debugging-port=9222 --remote-allow-origins=*
		options.add_experimental_option("debuggerAddress", debug_address)
	driver = webdriver.Chrome(options=options)
	if load_timeout > 0:
		driver.set_page_load_timeout(load_timeout)
	return driver


def setup_logger(name:str=__name__):
	Path("log").mkdir(exist_ok=True)
	logging_config_file_name = f'{base_path}/logging_local.yml'
	with open(logging_config_file_name, 'r') as logging_config_file:
		config.dictConfig(yaml.load(logging_config_file, Loader=yaml.FullLoader))
	return getLogger(name)

def load_env(file_name=".env"):
	with open(file_name, 'r') as logging_config_file:
		return yaml.load(logging_config_file, Loader=yaml.FullLoader)


@retry(
	retry_timeout=int(env["DISCONNECT_TIMEOUT"]),
	logger=logger,
	retry_multiplier=float(env["DISCONNECT_MULTIPLIER"]),
	max_retry_attempts=int(env["DISCONNECT_MAX_RETRIES"])
	)
def driver_get_link(link:str):
	global disconnect_timeout, disconnect_attempt
	logger.debug(f"Get URL: {link}")
	try:
		driver.get(link)
	except TimeoutException:
		logger.warning("Page load timed out!")
		return True
	except WebDriverException as e:
		if e.msg is not None and (e.msg.find("ERR_INTERNET_DISCONNECTED") != -1 or \
		e.msg.find("ERR_PROXY_CONNECTION_FAILED") != -1):
			raise Exception("RETRY","Connecting Internet")
		else:
			raise e

def sign_in():
	logger.info("Begin Sign-in")
	driver_get_link('https://www.linkedin.com/login')
	title = driver.find_element(By.XPATH,"//title").parent.title
	pattern = re.compile(r"log\s?-?in|sign\s?-?in|sign\s?-?up",re.IGNORECASE)

	if pattern.search(title):
		# Enter your email address and password
		try:
			driver.find_element(by=By.ID,value='username').send_keys(env["LINKEDIN_USER"])
			driver.find_element(by=By.ID,value='password').send_keys(env["LINKEDIN_PASSWORD"])# Submit the login form
			driver.find_element(by=By.CSS_SELECTOR,value='.login__form_action_container button').click()
			logger.info("Sign-in complete")
		except Exception as e:
			logger.error(f"Error signing in. ---> {e}")

	else:
		logger.info("Already signed in!")

def get_job_links(keywords:str,backup_path:str,max_n_jobs=500):
	logger.debug(f"Crawling job links for keyword: '{keywords}' - Max number of job links: {max_n_jobs}")
	hrefs = []
	keywords = keywords.replace(" ","%20")
	for p in range(0,max_n_jobs,25):
		url = f'https://www.linkedin.com/jobs/search/?distance=250&geoId=101174742&keywords={keywords}&f_TPR=r604800&sortBy=DD'
		url += f"&start={p}"
		driver_get_link(url)
		sleep(5)
		no_match = driver.find_elements(By.XPATH,"//h1[text()[contains(.,'No matching jobs found.')]]")
		if len(no_match) > 0:
			logger.debug(f"No more related job found for {keywords}. breaking.")
			break
		divs = driver.find_elements(by=By.XPATH, value= "//div[contains(@class, 'job-card-container')]")
		sleep(1)
		for div_element in divs:
			a_tags = div_element.find_elements(by=By.XPATH,value=".//a")
			for a_tag in a_tags:
				href = a_tag.get_attribute("href").split("?")[0] # type: ignore
				backup_data({"href":href},backup_path)
				hrefs.append(href)
	return hrefs

def get_skills():
	logger.debug("		+ Getting Required Skills")
	el = driver.find_elements(By.XPATH,"//span[text()[contains(.,'Show all skills') or contains(.,'Show qualification details')]]")
	if len(el) != 1:
		return []
	el[0].click()
	sleep(3)
	table = driver.find_elements(By.XPATH, "//ul[contains(@class,'job-details-skill-match-status-list')]")
	if len(table) != 1:
		return []
	skills = table[0].find_elements(By.TAG_NAME, "li")
	res = []
	for skill in skills:
		res.append(skills_text_pattern.findall(skill.text)[0])
	el = driver.find_elements(By.XPATH,"//span[text()[contains(.,'Done')]]")
	if len(el) == 0:
		logger.warning("Job qualification details is opened, but close button not found")
	else:
		el[0].click()
	return res

def scrape_job_page(link:str,job_id:int):
	logger.debug(f"Scraping job page at {link}")
	driver_get_link(link)
	sleep(3)
	alert = driver.find_elements(By.XPATH,"//div[contains(@role,'alert')]")
	if len(alert) > 0:
		raise Exception("expired")
	title = driver.find_element(By.XPATH,"//h1").accessible_name
	details_el =  driver.find_element(By.XPATH,"//div[contains(@class,'job-details-jobs-unified-top-card__primary-description-container')]")
	detail_items = details_el.text.split(" · ")
	if len(detail_items) == 3:
		detail_items.append("0 applicants")
	[company_name,location,post_time_raw,n_applicants] = detail_items
	n_applicants = extract_number_pattern.findall(n_applicants)[0]
	apply_link = get_apply_link()
	skills = get_skills()
	post_time,is_repost = convert_post_time(post_time_raw)
	logger.debug("Scrapping Finished")
	return {
		"job_id": job_id,
		"title": title, 
		"company_name": company_name,
		"post_time": post_time,
		"n_applicants": n_applicants,
		"location": location,
		"skills": skills,
		"is_repost": is_repost,
		"apply_link": apply_link,
		"post_time_raw": post_time_raw,
		"li_job_link": link
	}

def click_apply_button():
	buttons = driver.find_elements(By.XPATH,"//button[contains(@class,'jobs-apply-button')]")
	for button in buttons:
		if button.text == "Apply":
			button.click()
			return True
	return False

def get_current_tab_url():
	try:
		return driver.current_url
	except TimeoutException:
		logger.warning("Error getting tab URL. Timed Out")
	except:
		logger.warning("Error getting tab URL. Unknown Error")
	return None

def get_apply_link():
	logger.debug("		+ Getting Apply Link")
	res =  click_apply_button()
	if not res:
		return None
	original_tab = driver.current_window_handle
	external_url = None
	for tab in driver.window_handles:
		if tab != original_tab:
			driver.switch_to.window(tab)
			external_url = get_current_tab_url()
			driver.close()
			driver.switch_to.window(original_tab)
	return external_url

def get_backup_path(file_name_stub:str,folder:str="backup"):
	Path(folder).mkdir(exist_ok=True)
	file_list = os.listdir(folder)
	file_list = sorted([f for f in file_list if f.find(file_name_stub) !=-1],reverse=True)
	if len(file_list) == 0:
		file_name = file_name_stub + "_1.csv"
	else:
		last_file_name = file_list[0]
		d = int(extract_number_pattern.findall(last_file_name)[0])
		file_name = file_name_stub + f"_{d+1}.csv"
			
	return f"{folder}/{file_name}"

def backup_data(data:dict|list,backup_path:str):
	df = pd.DataFrame([data])
	if os.path.exists(backup_path):
		df.to_csv(backup_path,mode="a",header=False,index=False)
	else:
		df.to_csv(backup_path,mode="w",index=False)

	
def crawl_link(link:str,backup_path:str):
	job_id = job_id_pattern.findall(link)[0]
	if not job_data.exists(job_id):
		scraped_data = scrape_job_page(link,job_id)
		backup_data(scraped_data,backup_path)
		return scraped_data
	return None

def convert_post_time(str_time:str):
	t = int(extract_number_pattern.findall(str_time)[0])
	p = post_time_pattern.findall(str_time)[0] + "s"
	kwarg = {p:t}
	delta = timedelta(**kwarg)
	return datetime.now() - delta, str_time.lower().find("reposted") != -1

def crawl(keywords:str,max_n_jobs=500,match_threshold=70):
	backup_path = get_backup_path("crawl_links","backup")
	crawl_time = datetime.now()
	try:
		links = get_job_links(keywords,backup_path,max_n_jobs)
	except Exception as e:
		logger.error(f"Unexpected error while getting job links. Backup available at {backup_path}")
		logger.error(e)
		sys.exit()
	# with open("backup_path","r") as f:
	#     links =  f.read().splitlines()

	backup_path = get_backup_path("crawl_data","backup")
	for link in links:
		scraped_data = crawl_link(link,backup_path)
		if scraped_data and scraped_data["skills"] is not None:
			match_columns = produce_match_columns(
				scraped_data["skills"],
				env["MY_SKILLS"],
				threshold=match_threshold)
		else:
			match_columns = {}
		if scraped_data is None:
			continue
		job_data.write_one(**scraped_data,**match_columns,original_query=keywords,crawl_time=crawl_time)

def run(keywords: List[str],max_n_jobs=500):
	global logger, driver
	driver = setup_webdriver()
	logger = setup_logger("scrape")
	logger.info("---------------- Start a new crawl process ----------------")
	sign_in()
	for keyword in keywords:
		crawl(keyword,max_n_jobs)

def test():
	global logger, driver
	driver = setup_webdriver(headless=False) #debug_address="127.0.0.1:9222"
	logger = setup_logger("scrape")
	links = pd.read_csv("/media/arian/Storage/Projects/scrapper/backup/crawl_links_4.csv")["href"].values
	for link in links:
		crawl_link(link,"backup")
	logger.info("OK")

if __name__ == "__main__":
	"""
	Use this command to inhibit system from going to sleep while running the process:
	(Bash and need the venv)
	systemd-inhibit --what=sleep:handle-lid-switch python linkedIn_selenium/browser.py
	"""
	run(keywords=["junior python data engineer","backend python software engineer"])

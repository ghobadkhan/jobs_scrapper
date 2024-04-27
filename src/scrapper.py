import glob
import json
import re
import pandas as pd
import os
from typing import Literal
from pathlib import Path
from logging import Logger, getLogger
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options
from datetime import datetime, timedelta
from ast import literal_eval
from time import sleep
from .contracts import JobData
from .utils import retry, ScrapperException
from .matcher import fuzz_match, find_matches

job_id_pattern = re.compile(r".*view\/(\d*).*")
extract_number_pattern = re.compile(r"\D*(\d*)\D*")
post_time_pattern = re.compile(r".* (.*?)s? ago")
skills_text_pattern = re.compile(r"(.*)\n?.*")


class Scrapper():
	def __init__(
			self,
			job_data:JobData|None=None,
			logger:Logger | None = None,
			disable_extension=True,
			headless=True,
			load_timeout=12,
			debug_address:str|None=None,
			max_n_jobs:int = 500,
			driver_logging:bool = True,
			user_data_dir:str|None = None
			) -> None:
		self.driver_logging = driver_logging
		self.driver_options = {
			"disable_extension": disable_extension,
			"headless": headless,
			"load_timeout": load_timeout,
			"debug_address": debug_address,
			"user_data_dir": user_data_dir
		}
		self.driver = self.setup_webdriver(**self.driver_options)
		Path(os.environ["BACKUP_FOLDER"]).mkdir(exist_ok=True)
		self.logger = logger if logger else getLogger()
		self.driver_get_link = self.setup_get_link()
		self.job_data = job_data
		self.max_n_jobs = max_n_jobs
		self.crawl_time = None
		"""
		self.state
		This value is exclusively used to save the current state (progress).
		Schema:
		{
			"query": The present query,
			"stage": either crawling_links_list or scrapping_each_link,
			"data": if stage = crawling_links_list -> data is the last page
				if stage = scrapping_each_link -> data is the last job_id
			"attempt": number of times the state is accessed. Used for limit the 
				persistence
		}
		"""
		self.state: dict|None = self.read_state()

		# Get MY_SKILLS from env if available and parse it to a list
		if "MY_SKILLS" in os.environ:
			self.my_skills = literal_eval(os.environ["MY_SKILLS"])
		else:
			self.my_skills = None

	def re_init_driver(self):
		self.logger.debug("Re-Initializing the webdriver.")
		self.driver.quit()
		self.driver = self.setup_webdriver(**self.driver_options)

	def setup_webdriver(
			self,
			disable_extension=True,
			headless=True,
			load_timeout=12,
			debug_address:str|None=None,
			user_data_dir:str|None=None
		):
		#TODO: Load options from a file or other external source
		options = Options()
		if debug_address is None:
			if user_data_dir is not None:
				options.add_argument(f"user-data-dir={user_data_dir}")
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
		if self.driver_logging:
			service = webdriver.ChromeService(log_output=f"{os.environ['LOG_FOLDER']}/chrome.log")
		else:
			service = None
		driver = webdriver.Chrome(options=options,service=service) #type: ignore
		if load_timeout > 0:
			driver.set_page_load_timeout(load_timeout)
		return driver


	def setup_get_link(self):
		@retry(
			retry_timeout=int(os.environ["DISCONNECT_TIMEOUT"]),
			logger=self.logger,
			retry_multiplier=float(os.environ["DISCONNECT_MULTIPLIER"]),
			max_retry_attempts=int(os.environ["DISCONNECT_MAX_RETRIES"])
		)
		def func(link):
			self.logger.debug(f"Get URL: {link}")
			try:
				self.driver.get(link)
				return True
			except TimeoutException:
				self.logger.warning("Page load timed out!")
				return True
			except WebDriverException as e:
				if e.msg is not None and (e.msg.find("ERR_INTERNET_DISCONNECTED") != -1 or \
				e.msg.find("ERR_PROXY_CONNECTION_FAILED") != -1):
					raise Exception("RETRY","Connecting Internet")
				else:
					raise Exception("Webdriver Exception:",e.msg)
		return func

	def sign_in(self):
		self.logger.info("Begin Sign-in")
		self.driver_get_link('https://www.linkedin.com')
		title = self.driver.find_element(By.XPATH,"//title").parent.title
		pattern = re.compile(r"log\s?-?in|sign\s?-?in|sign\s?-?up",re.IGNORECASE)

		if pattern.search(title):
			# TODO: Write a subroutine to detect if the challenge page after sign-in is shown
			# Enter your email address and password
			try:
				self.driver.find_element(by=By.ID,value='session_key').send_keys(os.environ["LINKEDIN_USER"])
				self.driver.find_element(by=By.ID,value='session_password').send_keys(os.environ["LINKEDIN_PASSWORD"])# Submit the login form
				self.driver.find_element(By.XPATH,"//button[contains(@data-id,'sign-in-form__submit-btn')]").click()
				sleep(5)
			except WebDriverException as e:
				self.logger.error("Error signing in. Webdriver exception")
				raise Exception(e.msg)
			except Exception as e:
				self.logger.error("Error signing in. Unknown exception")
				raise e
		else:
			self.logger.info("Already signed in!")

	def get_job_links_list(self,query:str,backup_path:str,start_page:int|None=0):
		# set the 'start_page' to None to skip this stage
		if start_page is None:
			return
		self.logger.debug(f"Crawling job links for query: '{query}' - Max number of job links: {self.max_n_jobs}")
		keywords = query.replace(" ","%20") # breaking down the query into keywords
		assert self.state is not None
		self.set_state({"stage":"crawling_links_list"})
		for p in range(start_page,self.max_n_jobs,25):
			self.set_state({"data":p})
			url = f'https://www.linkedin.com/jobs/search/?distance=250&geoId=101174742&keywords={keywords}&f_TPR=r604800&sortBy=DD'
			url += f"&start={p}"
			self.driver_get_link(url)
			sleep(5)
			no_match = self.driver.find_elements(By.XPATH,"//h1[text()[contains(.,'No matching jobs found.')]]")
			if len(no_match) > 0:
				self.logger.debug(f"No more related job found for {keywords}. breaking.")
				break
			divs = self.driver.find_elements(by=By.XPATH, value= "//div[contains(@class, 'job-card-container')]")
			sleep(1)
			for div_element in divs:
				a_tags = div_element.find_elements(by=By.XPATH,value=".//a")
				for a_tag in a_tags:
					href = a_tag.get_attribute("href").split("?")[0] # type: ignore
					self.backup_data({"href":href,"page":p},backup_path)

	def get_skills(self):
		self.logger.debug("		+ Getting Required Skills")
		el = self.driver.find_elements(By.XPATH,"//span[text()[contains(.,'Show all skills') or contains(.,'Show qualification details')]]")
		if len(el) != 1:
			return []
		el[0].click()
		sleep(3)
		table = self.driver.find_elements(By.XPATH, "//ul[contains(@class,'job-details-skill-match-status-list')]")
		if len(table) != 1:
			return []
		skills = table[0].find_elements(By.TAG_NAME, "li")
		res = []
		for skill in skills:
			res.append(skills_text_pattern.findall(skill.text)[0])
		el = self.driver.find_elements(By.XPATH,"//span[text()[contains(.,'Done')]]")
		if len(el) == 0:
			self.logger.warning("Job qualification details is opened, but close button not found")
		else:
			el[0].click()
		return res

	def take_screenshot(self,file_type:Literal["b64","png"]="b64"):
		img_file_name = f"{datetime.now().isoformat(timespec='seconds')}"
		folder = os.environ["SCREENSHOT_FOLDER"]
		match file_type:
			case "b64":
				img = self.driver.get_screenshot_as_base64()
				open(f"{folder}/{img_file_name}.b64","w").write(img)
			case "png":
				img = self.driver.get_screenshot_as_png()
				open(f"{folder}/{img_file_name}.png","wb").write(img)
			case _:
				self.logger.error(f"Invalid file_type chosen for screenshot ({file_type}).")
				return False
		self.logger.debug(f"Screenshot taken: {img_file_name}.{file_type}")
		return True

	def scrape_job_page(self,link:str,job_id:int):
		self.logger.debug(f"Scraping job page at {link}")
		self.driver_get_link(link)
		sleep(3)
		alert = self.driver.find_elements(By.XPATH,"//div[contains(@role,'alert')]")
		if len(alert) > 0:
			self.logger.warning("The job is expired")
		title = self.driver.find_element(By.XPATH,"//h1").accessible_name
		details_el =  self.driver.find_element(By.XPATH,"//div[contains(@class,'job-details-jobs-unified-top-card__primary-description-container')]")
		detail_items = details_el.text.split(" · ")
		if len(detail_items) == 3:
			detail_items.append("0 applicants")
		[company_name,location,post_time_raw,n_applicants] = detail_items
		n_applicants = extract_number_pattern.findall(n_applicants)[0]
		apply_link = self.get_apply_link()
		skills = self.get_skills()
		post_time,is_repost = self.convert_post_time(post_time_raw)
		self.logger.debug("Scrapping Finished")
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
			"post_time_raw": post_time_raw
		}

	def click_apply_button(self):
		buttons = self.driver.find_elements(By.XPATH,"//button[contains(@class,'jobs-apply-button')]")
		for button in buttons:
			if button.text == "Apply":
				button.click()
				return True
		return False

	def get_current_tab_url(self):
		try:
			return self.driver.current_url
		except TimeoutException:
			self.logger.warning("Error getting tab URL. Timed Out")
		except:
			self.logger.warning("Error getting tab URL. Unknown Error")
		return None

	def get_apply_link(self):
		self.logger.debug("		+ Getting Apply Link")
		res =  self.click_apply_button()
		if not res:
			return None
		original_tab = self.driver.current_window_handle
		external_url = None
		for tab in self.driver.window_handles:
			if tab != original_tab:
				self.driver.switch_to.window(tab)
				external_url = self.get_current_tab_url()
				self.driver.close()
				self.driver.switch_to.window(original_tab)
		return external_url

	def get_backup_path(self,file_name_stub:str):
		folder = os.environ["BACKUP_FOLDER"]
		file_name = file_name_stub + ".csv"
				
		return f"{folder}/{file_name}"

	def backup_data(self,data:dict|list,backup_path:str):
		df = pd.DataFrame([data])
		if os.path.exists(backup_path):
			df.to_csv(backup_path,mode="a",header=False,index=False)
		else:
			df.to_csv(backup_path,mode="w",index=False)

	def scrap_a_job_link(self,link:str):
		job_id = job_id_pattern.findall(link)[0]
		self.set_state({"data":job_id})
		if self.job_data and not self.job_data.exists(job_id):
			try:
				scraped_data = self.scrape_job_page(link,job_id)
				return scraped_data
			except WebDriverException as e:
				self.logger.error(f"Webdriver error while scraping the link: {e.msg}")
			except Exception as e:
				self.logger.error(f"Unknown error while scraping the link. {e}")
			self.take_screenshot("png")
		self.logger.log(msg=f"Job ID {job_id} already exists!",level=8)
		return None

	@staticmethod
	def convert_post_time(str_time:str):
		t = int(extract_number_pattern.findall(str_time)[0])
		p = post_time_pattern.findall(str_time)[0] + "s"
		kwarg = {p:t}
		delta = timedelta(**kwarg)
		return datetime.now() - delta, str_time.lower().find("reposted") != -1
	
	def read_state(self) -> dict|None:
		file_path = f"{os.environ['BACKUP_FOLDER']}/{os.environ['SCRAP_STATE_FILE']}"
		
		if not os.path.exists(file_path):
			self.logger.debug(f"No state file existed at {file_path}")
			return None
		
		# TODO: Unify link backup data with the other data as sqlite
		with open(file_path,"r") as f:
			self.logger.debug(f"State file exists at {file_path}\nState={f.read()}")
			f.seek(0)
			return json.load(f)
		

	def set_state(self,state:dict|None=None):
		accepted_keys = ["stage","data","attempt","query"]
		if self.state is None:
			self.state = {}
		if state is not None:
			for key,val in state.items():
				if key not in accepted_keys:
					raise Exception("Illegal state key is set.")
			self.state[key] = val

		file_path = f"{os.environ['BACKUP_FOLDER']}/{os.environ['SCRAP_STATE_FILE']}"
		with open(file_path,"w") as f:
			json.dump(self.state,f)
			# Since it's too frequent we won't catch it even at debug level. We set it to sub DEBUG (<10)
			self.logger.log(msg=f"State file is written at {file_path}",level=8)
	
	def del_state_and_backup(self):
		self.logger.debug(f"Deleting the state and backup files")
		folder = os.environ['BACKUP_FOLDER']
		if not os.path.exists(folder):
			self.logger.error("The backup folder does not exist!")
		# Get a list of all files in the folder
		files = glob.glob(folder + "/*")
		# Iterate over the list of files and remove each one
		for file in files:
			if os.path.isfile(file):
				self.logger.debug(f"Removing {file}")
				os.remove(file)
		self.state = None

	def generate_match_columns(self,scraped_data,threshold: int=70):
		if scraped_data and scraped_data["skills"] is not None and self.my_skills:
			job_skills = scraped_data["skills"]
			return {
				"match_score": fuzz_match(job_skills,self.my_skills,method='partial'),
				"top_matches": find_matches(job_skills,self.my_skills,threshold),
				"match_threshold": threshold
			}
		else:
			return {}

	def run_sequence(self,query:str,match_threshold=70):
		links_backup_path = self.get_backup_path("crawl_links")
		self.crawl_time = datetime.now()

		start_page = 0
		if self.state is not None:
			# TODO: The logic is flawed. The query check must be done at parent routine

			self.set_state({"attempt":self.state["attempt"]+1})
			if self.state["query"] != query:
				self.logger.debug(f"The present query: '{query}' is already crawled. Skipping it")
				return
			if self.state["stage"] == "crawling_links_list":
				start_page = self.state["data"]
			elif self.state["stage"] == "scrapping_each_link":
				start_page = None
			else:
				raise Exception(f"Invalid value for 'stage' at state: {self.state['stage']}.")
		else:
			self.state = {"query": query, "attempt":0}

		
		self.get_job_links_list(query,links_backup_path,start_page)

		links = pd.read_csv(links_backup_path)["href"].to_list()
		assert self.state is not None
		self.set_state({"stage":"scrapping_each_link"})
		for link in links:
			scraped_data = self.scrap_a_job_link(link)
			if scraped_data is None:
				continue
			match_columns = self.generate_match_columns(scraped_data,match_threshold)
			if self.job_data:
				self.job_data.write_one(**scraped_data,**match_columns,original_query=query,crawl_time=self.crawl_time)
		self.del_state_and_backup()

	def manage_and_run(self,query:str,match_threshold=70):
		try:
			self.run_sequence(query=query,match_threshold=match_threshold)
		except WebDriverException as e:
			self.logger.error(f"A webdriver exception occurred:\n{e.msg}")
			if self.state is not None and self.state["attempt"] > int(os.environ["MAX_SCRAPPER_PERSISTENCE"]):
				raise ScrapperException(kind="max_attempts")
			else:
				raise ScrapperException(kind="webdriver",e=e)
		except Exception as e:
			self.logger.error(f"An unknown exception occurred:\n{e}")
			raise ScrapperException(kind="unknown",e=e)
		finally:
			if self.state is not None:
				self.set_state()

		
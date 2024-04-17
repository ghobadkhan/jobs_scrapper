import re
import sys
import pandas as pd
import os
from pathlib import Path
from logging import Logger, getLogger
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options
from datetime import datetime, timedelta
from time import sleep
from .contracts import JobData
from .utils import retry
from .matcher import produce_match_columns

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
			max_n_jobs:int = 500
			) -> None:
		self.driver = self.setup_webdriver(
			disable_extension=disable_extension,
			headless=headless,
			load_timeout=load_timeout,
			debug_address=debug_address
		)
		self.logger = logger if logger else getLogger()
		self.driver_get_link = self.setup_get_link()
		self.job_data = job_data
		self.max_n_jobs = max_n_jobs

	def setup_webdriver(self,disable_extension=True,headless=True, load_timeout=12, debug_address:str|None=None):
		#TODO: Load options from a file or other external source
		options = Options()
		if debug_address is None:
			options.add_argument(f"user-data-dir={os.environ['CHROME_PROFILE']}")
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
					raise e
		return func

	def sign_in(self):
		self.logger.info("Begin Sign-in")
		self.driver_get_link('https://www.linkedin.com/login')
		title = self.driver.find_element(By.XPATH,"//title").parent.title
		pattern = re.compile(r"log\s?-?in|sign\s?-?in|sign\s?-?up",re.IGNORECASE)

		if pattern.search(title):
			# Enter your email address and password
			try:
				self.driver.find_element(by=By.ID,value='username').send_keys(os.environ["LINKEDIN_USER"])
				self.driver.find_element(by=By.ID,value='password').send_keys(os.environ["LINKEDIN_PASSWORD"])# Submit the login form
				self.driver.find_element(by=By.CSS_SELECTOR,value='.login__form_action_container button').click()
				sleep(5)
				self.take_screenshot()
			except Exception as e:
				self.logger.error(f"Error signing in. ---> {e}")
		else:
			self.logger.info("Already signed in!")

	def get_job_links(self,keywords:str,backup_path:str):
		self.logger.debug(f"Crawling job links for keyword: '{keywords}' - Max number of job links: {self.max_n_jobs}")
		hrefs = []
		keywords = keywords.replace(" ","%20")
		for p in range(0,self.max_n_jobs,25):
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
					self.backup_data({"href":href},backup_path)
					hrefs.append(href)
			self.take_screenshot()
		return hrefs

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

	def take_screenshot(self,file_type:str="base64"):
		if file_type == "base64":
			img = self.driver.get_screenshot_as_base64()
			img_file_name = f"{datetime.now().isoformat(timespec='seconds')}"
			open(f"screenshots/{img_file_name}.b64","w").write(img)
			self.logger.debug(f"Screenshot taken: {img_file_name}.b64")
			return True
		return False

	def scrape_job_page(self,link:str,job_id:int):
		self.logger.debug(f"Scraping job page at {link}")
		self.driver_get_link(link)
		sleep(3)
		alert = self.driver.find_elements(By.XPATH,"//div[contains(@role,'alert')]")
		if len(alert) > 0:
			self.logger.warning("The job is expired")
			self.take_screenshot()
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
			"post_time_raw": post_time_raw,
			"li_job_link": link
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

	def get_backup_path(self,file_name_stub:str,folder:str="backup"):
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

	def backup_data(self,data:dict|list,backup_path:str):
		df = pd.DataFrame([data])
		if os.path.exists(backup_path):
			df.to_csv(backup_path,mode="a",header=False,index=False)
		else:
			df.to_csv(backup_path,mode="w",index=False)

	
	def crawl_a_job_link(self,link:str,backup_path:str):
		job_id = job_id_pattern.findall(link)[0]
		if self.job_data and not self.job_data.exists(job_id):
			scraped_data = self.scrape_job_page(link,job_id)
			self.backup_data(scraped_data,backup_path)
			return scraped_data
		return None

	@staticmethod
	def convert_post_time(str_time:str):
		t = int(extract_number_pattern.findall(str_time)[0])
		p = post_time_pattern.findall(str_time)[0] + "s"
		kwarg = {p:t}
		delta = timedelta(**kwarg)
		return datetime.now() - delta, str_time.lower().find("reposted") != -1

	def crawl(self,keywords:str,match_threshold=70):
		backup_path = self.get_backup_path("crawl_links","backup")
		crawl_time = datetime.now()
		try:
			links = self.get_job_links(keywords,backup_path)
		except Exception as e:
			self.logger.error(f"Unexpected error while getting job links. Backup available at {backup_path}")
			self.logger.error(e)
			sys.exit()
		# with open("backup_path","r") as f:
		#     links =  f.read().splitlines()

		backup_path = self.get_backup_path("crawl_data","backup")
		for link in links:
			scraped_data = self.crawl_a_job_link(link,backup_path)
			if scraped_data and scraped_data["skills"] is not None:
				match_columns = produce_match_columns(
					scraped_data["skills"],
					os.environ["MY_SKILLS"], # type:ignore
					threshold=match_threshold)
			else:
				match_columns = {}
			if scraped_data is None:
				continue
			if self.job_data:
				self.job_data.write_one(**scraped_data,**match_columns,original_query=keywords,crawl_time=crawl_time)
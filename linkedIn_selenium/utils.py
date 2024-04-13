import sys
from functools import wraps
from logging import Logger
from time import sleep
from webdriver_manager.chrome import ChromeDriverManager

def retry(retry_timeout:int, logger:Logger ,retry_multiplier:float = 0, max_retry_attempts:int=5):
	"""
	Wraps a retry attempt around any method
	To use this, the called method must raise an Exception object with two args:
	arg[0] must always be RETRY
	arg[1] can be the subject of retry
	"""
	attempt = 0
	def decorator(func):
		@wraps(func)
		def wrapper(*args, **kwargs):
			nonlocal attempt
			try:
				func(*args,**kwargs)
			except Exception as e:
				if len(e.args) > 0 and e.args[0] == "RETRY":
					if len(e.args) == 1:
						reason = "_"
					else:
						reason = e.args[1]
					if attempt >= max_retry_attempts:
						logger.error(f"Maximum retires is reached for {func.__name__} on {reason}. Exiting!")
						sys.exit(1)
					else:
						logger.warning(f"{func.__name__} requested retry for {reason}.")
						#TODO: The formula for retry time out is not correct!
						t = (1+attempt*retry_multiplier)*retry_timeout
						logger.info(f"Retry:{attempt}, Waiting for {t} seconds.")
						sleep(t)
						attempt += 1
						wrapper(*args,**kwargs)
				else:
					logger.warning(f"attempt function is invoked for {func.__name__}"\
					"but proper Exception is not raised. Raising the original Exception")
					raise e
		return wrapper
	return decorator

def download_chromedriver():
	"""Downloads Chrome Webdriver of Selenium
	"""
	print(ChromeDriverManager().install())

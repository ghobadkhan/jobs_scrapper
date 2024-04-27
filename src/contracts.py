import abc
from datetime import datetime
from typing import List
import sys

class Singleton(type):
    _instance = None
    def __call__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__call__(*args, **kwargs)
        return cls._instance

class JobData(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def write_one(
        self,
        job_id:int,
        title:str,
        company_name:str,           
        crawl_time:datetime,
        original_query:str,
        li_job_link:str,
        post_time:datetime|None=None,
        n_applicants:int|None=None,
        location:str|None=None,
        skills:List[str]|None=None,
        is_repost:bool=False,
        apply_link:str|None=None,
        post_time_raw:str|None=None,
        match_score:int|None=None,
        top_matches:list|None=None,
        match_threshold:int|None=None
        ):
        raise NotImplementedError
    
    """
    Update one row in 'details' table.
    'data' format is {column1:value1, column2:value2, ...}
    """
    @abc.abstractmethod
    def update_one(self,job_id:int,data:dict):
        raise NotImplementedError
    
    @abc.abstractmethod
    def get_one(self,job_id:int) -> dict|None:
        raise NotImplementedError
    
    def exists(self, job_id:int):
        return self.get_one(job_id) is not None
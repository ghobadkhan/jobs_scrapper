import sqlite3
from typing import List
from datetime import datetime


class DB:
    def __init__(self, db_name:str) -> None:
        
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()

    def create_tables(self):

        query = """
        CREATE TABLE IF NOT EXISTS crawl_time (
            id INTEGER PRIMARY KEY,
            time TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS original_query (
            id INTEGER PRIMARY KEY,
            query TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS company (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS details (
            id INTEGER PRIMARY KEY,
            job_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            company_id INTEGER NOT NULL REFERENCES company(id),
            post_time TEXT,
            n_applicants INTEGER,
            location TEXT,
            skills TEXT,
            is_repost INTEGER,
            apply_link TEXT,
            post_time_raw TEXT,
            li_job_link TEXT,
            crawl_time_id INTEGER NOT NULL REFERENCES crawl_time(id),
            original_query_id INTEGER NOT NULL REFERENCES original_query(id),
            match_score INTEGER,
            top_matches TEXT,
            match_threshold INT
        );
        """
        self.conn.executescript(query)
        self.conn.commit()
        # self.conn.close()

    def insert_details(
            self,
            job_id:int,
            title:str,
            company_id:int,           
            crawl_time_id:int,
            original_query_id:int,
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

        insert_query = """
            INSERT INTO details (job_id, title, company_id, post_time, n_applicants,
            location, skills, is_repost, apply_link, post_time_raw, li_job_link,
            crawl_time_id, original_query_id, match_score, top_matches, match_threshold)
            VALUES (?, ?, ?, datetime(?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, json_array(?), ?)
        """
        skills_str = None if (skills is None or len(skills)==0) else ",".join(skills)
        top_matches_str = None if (top_matches is None or len(top_matches)==0) else ",".join(top_matches)
        data = (
            job_id, title, company_id, post_time, n_applicants,
            location, skills_str, is_repost, apply_link, post_time_raw, li_job_link,
            crawl_time_id, original_query_id, match_score, top_matches_str, match_threshold
        )
        self.conn.execute(insert_query,data)
        self.conn.commit()

    def get_original_query_id(self,original_query:str):

        self.cursor.execute("SELECT id FROM original_query WHERE query= ?",(original_query,))
        id = self.cursor.fetchone()
        if id is None:
            self.cursor.execute("INSERT INTO original_query (query) VALUES (?)",(original_query,))
            self.conn.commit()
            self.cursor.execute("SELECT id FROM original_query WHERE query= ?",(original_query,))
            id = self.cursor.fetchone()
        return id[0]

    def get_crawl_time_id(self,crawl_time:str):

        self.cursor.execute("SELECT id FROM crawl_time WHERE time= datetime(?)",(crawl_time,))
        id = self.cursor.fetchone()
        if id is None:
            self.cursor.execute("INSERT INTO crawl_time (time) VALUES (datetime(?))",(crawl_time,))
            self.conn.commit()
            self.cursor.execute("SELECT id FROM crawl_time WHERE time= datetime(?)",(crawl_time,))
            id = self.cursor.fetchone()
        return id[0]

    def get_company_id(self,company_name:str):

        self.cursor.execute("SELECT id FROM company WHERE name=?",(company_name,))
        id = self.cursor.fetchone()
        if id is None:
            self.cursor.execute("INSERT INTO company (name) VALUES (?)",(company_name,))
            self.conn.commit()
            self.cursor.execute("SELECT id FROM company WHERE name= ?",(company_name,))
            id = self.cursor.fetchone()
        return id[0]
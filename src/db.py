import sqlite3
from pathlib import Path
from typing import List
from datetime import datetime
from .contracts import JobData
from functools import lru_cache


class DB(JobData):
    def __init__(self, db_name:str,output_folder:str=".") -> None:
        if output_folder != ".":
            Path(output_folder).mkdir(exist_ok=True)
        self.conn = sqlite3.connect(f"{output_folder}/{db_name}")
        self.cursor = self.conn.cursor()
        self.create_tables()

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
            job_id INTEGER NOT NULL UNIQUE,
            title TEXT NOT NULL,
            company_id INTEGER NOT NULL REFERENCES company(id),
            post_time TEXT,
            n_applicants INTEGER,
            location TEXT,
            skills TEXT,
            is_repost INTEGER,
            apply_link TEXT,
            post_time_raw TEXT,
            crawl_time_id INTEGER NOT NULL REFERENCES crawl_time(id),
            original_query_id INTEGER NOT NULL REFERENCES original_query(id),
            match_score INTEGER,
            top_matches JSONB,
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

        skills_str = None if (skills is None or len(skills)==0) else ",".join(skills)
        if top_matches is None:
            top_matches_str = "NULL"
        else:
            top_matches_str = "json_array('"+"','".join(top_matches)+"')"
        insert_query = f"""
            INSERT INTO details (job_id, title, company_id, post_time, n_applicants,
            location, skills, is_repost, apply_link, post_time_raw,
            crawl_time_id, original_query_id, match_score, top_matches, match_threshold)
            VALUES (?, ?, ?, datetime(?), ?, ?, ?, ?, ?, ?, ?, ?, ?, {top_matches_str}, ?)
        """
        data = (
            job_id, title, company_id, post_time, n_applicants,
            location, skills_str, is_repost, apply_link, post_time_raw,
            crawl_time_id, original_query_id, match_score, match_threshold
        )
        self.conn.execute(insert_query,data)
        self.conn.commit()

    @lru_cache(maxsize=10)
    def get_original_query_id(self,original_query:str):

        self.cursor.execute("SELECT id FROM original_query WHERE query= ?",(original_query,))
        id = self.cursor.fetchone()
        if id is None:
            self.cursor.execute("INSERT INTO original_query (query) VALUES (?)",(original_query,))
            self.conn.commit()
            self.cursor.execute("SELECT id FROM original_query WHERE query= ?",(original_query,))
            id = self.cursor.fetchone()
        return id[0]

    @lru_cache(maxsize=10)
    def get_crawl_time_id(self,crawl_time:datetime):
        crawl_time_str = crawl_time.strftime('%Y-%m-%d %H:00:00')
        self.cursor.execute("SELECT id FROM crawl_time WHERE time= datetime(?)",(crawl_time_str,))
        id = self.cursor.fetchone()
        if id is None:
            self.cursor.execute("INSERT INTO crawl_time (time) VALUES (datetime(?))",(crawl_time_str,))
            self.conn.commit()
            self.cursor.execute("SELECT id FROM crawl_time WHERE time= datetime(?)",(crawl_time_str,))
            id = self.cursor.fetchone()
        return id[0]

    @lru_cache(maxsize=500)
    def get_company_id(self,company_name:str):

        self.cursor.execute("SELECT id FROM company WHERE name=?",(company_name,))
        id = self.cursor.fetchone()
        if id is None:
            self.cursor.execute("INSERT INTO company (name) VALUES (?)",(company_name,))
            self.conn.commit()
            self.cursor.execute("SELECT id FROM company WHERE name= ?",(company_name,))
            id = self.cursor.fetchone()
        return id[0]
    
    def write_one(self,
        job_id: int,
        title: str,
        company_name: str,
        crawl_time: datetime,
        original_query: str,
        post_time: datetime | None = None,
        n_applicants: int | None = None,
        location: str | None = None,
        skills: List[str] | None = None,
        is_repost: bool = False,
        apply_link: str | None = None,
        post_time_raw: str | None = None,
        match_score: int | None = None,
        top_matches: List | None = None,
        match_threshold: int | None = None
        ):
        company_id = self.get_company_id(company_name)
        original_query_id = self.get_original_query_id(original_query)
        crawl_time_id = self.get_crawl_time_id(crawl_time)
        self.insert_details(
            job_id,
            title,
            company_id,           
            crawl_time_id,
            original_query_id,
            post_time,
            n_applicants,
            location,
            skills,
            is_repost,
            apply_link,
            post_time_raw,
            match_score,
            top_matches,
            match_threshold
        )

    def get_one(self, job_id: int) -> dict | None:
        return self.get_joined(job_id)
    
    def get_joined(self,job_id:int,include_id=False):
        q = f"""
        SELECT {'d.id,' if include_id else ''} d.job_id, d.title, c.name, d.post_time, 
        d.n_applicants, d.location, d.skills, d.is_repost, d.apply_link,
        d.post_time_raw, t.time, c.name, d.match_score,
        d.top_matches, d.match_threshold, o.query
        FROM details AS d
        LEFT JOIN company AS c ON d.company_id = c.id
        LEFT JOIN original_query AS o ON d.original_query_id = o.id
        LEFT JOIN crawl_time AS t ON d.crawl_time_id = t.id
        WHERE d.job_id = ?;
        """
        self.cursor.execute(q,(job_id,))
        res = self.cursor.fetchone()
        if res is None:
            return None
        return dict(zip([column[0] for column in self.cursor.description], res))

    def update_one(self, job_id: int, data: dict):
        if not self.exists(job_id):
            return False
        data_stmt = ",".join([f"{k}='{v}'" for k,v in data.items()])
        q = f"""
        UPDATE details
        SET {data_stmt}
        WHERE job_id = {job_id}
        """
        self.conn.execute(q)
        self.conn.commit()
        return True
    
    def exists(self, job_id: int):
        q = "SELECT * FROM details WHERE job_id = ?"
        self.cursor.execute(q,(job_id,))
        res = self.cursor.fetchone()
        return res is not None
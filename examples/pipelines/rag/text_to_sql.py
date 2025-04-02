"""
title: Llama Index DB Pipeline
author: 0xThresh
date: 2024-08-11
version: 1.1
license: MIT
description: A pipeline for using text-to-SQL for retrieving relevant information from a database using the Llama Index library.
requirements: llama_index, sqlalchemy, psycopg2-binary
"""

from typing import List, Union, Generator, Iterator
import os 
from pydantic import BaseModel
from langchain_core.prompts import ChatPromptTemplate
from langchain_ollama.llms import OllamaLLM
from sqlalchemy import create_engine,text
from sqlalchemy.exc import SQLAlchemyError
import ollama


class Pipeline:
    class Valves(BaseModel):
        DB_HOST: str
        DB_PORT: str
        DB_USER: str
        DB_PASSWORD: str        
        DB_DATABASE: str
        DB_TABLE: str
        OLLAMA_HOST: str
        TEXT_TO_SQL_MODEL: str 


    # Update valves/ environment variables based on your selected database 
    def __init__(self):
        self.name = "市场主体数据查询"
        self.engine = None

        # Initialize
        self.valves = self.Valves(
            **{
                "pipelines": ["*"],                                                           # Connect to all pipelines
                "DB_HOST": os.getenv("DB_HOST", "postgresql"),                          # Database hostname
                "DB_PORT": os.getenv("DB_PORT", "5432"),                                        # Database port 
                "DB_USER": os.getenv("DB_USER", "ai_user"),                                  # User to connect to the database with
                "DB_PASSWORD": os.getenv("DB_PASSWORD", "Qdnkljc_2021"),                          # Password to connect to the database with
                "DB_DATABASE": os.getenv("DB_DATABASE", "report_db"),                          # Database to select on the DB instance
                "DB_TABLE": os.getenv("DB_TABLE", "stat_top500_by_area"),                            # Table(s) to run queries against 
                "OLLAMA_HOST": os.getenv("OLLAMA_HOST", "http://172.24.18.51:11434"), # Make sure to update with the URL of your Ollama host, such as http://localhost:11434 or remote server address
                "TEXT_TO_SQL_MODEL": os.getenv("TEXT_TO_SQL_MODEL", "sqlcoder:15b")            # Model to use for text-to-SQL generation      
            }
        )

    def init_db_connection(self):
        # Update your DB connection string based on selected DB engine - current connection string is for Postgres
        try:
            self.engine = create_engine(f"postgresql+psycopg2://{self.valves.DB_USER}:{self.valves.DB_PASSWORD}@{self.valves.DB_HOST}:{self.valves.DB_PORT}/{self.valves.DB_DATABASE}")
        except SQLAlchemyError as e:
            return f"Error Create Engine: {str(e)}"

    async def on_startup(self):
        # This function is called when the server is started.
        self.init_db_connection()

    async def on_shutdown(self):
        # This function is called when the server is stopped.
        pass

    def pipe(
        self, user_message: str, model_id: str, messages: List[dict], body: dict
    ) -> Union[str, Generator, Iterator]:
    
        # Set up the custom prompt used when generating SQL queries from text
        text_to_sql_prompt = """
        ### 指令：你的任务是根据Postgres数据库模式，将一个问题转换为SQL查询。 遵循以下规则：
		- **逐字逐句地分析问题和数据库模式，以准确回答问题
		- **使用表别名以防止歧义。例如：SELECT table1.col1, table2.col1 FROM table1 JOIN table2 ON table1.id = table2.id
		- **创建比率时，始终将分子转换为浮点数
		### 输入：生成一个SQL查询，以回答问题 {question}。 
			此查询将在一个数据库上运行，该数据库的模式由以下字符串表示：
			CREATE TABLE public.stat_top500_by_area (
				area_code varchar(255) NOT NULL, -- 地区代码
				year_num varchar(255) NOT NULL, -- 年度
				area_name varchar(255) NULL, -- 地区名称
				entities_num int4 NULL, -- 企业数
				average_revenue numeric(6, 2) NULL, -- 营业收入
				CONSTRAINT stat_top500_by_area_pkey PRIMARY KEY (area_code, year_num)
			);

		### 输出：根据你的要求，以下是用于回答问题 {question} 的SQL查询：
		```sql 
        """

        prompt = ChatPromptTemplate.from_template(text_to_sql_prompt)
        model = OllamaLLM(model=self.valves.TEXT_TO_SQL_MODEL,base_url=self.valves.OLLAMA_HOST)

        chain = prompt | model
        response = chain.invoke({"question": user_message})
        sql_query = response
        print(sql_query)
        engine=self.engine
        if engine is None:
            return "Database connection failed to initialize."
       
        with engine.connect() as conn:
            result = conn.execute(text(sql_query))
            rows = result.fetchall()
            if not rows:
                return "No data returned from query."

            column_names = result.keys()
            csv_data = f"Query executed successfully. Below is the actual result of the query {{sql_query}} running against the database in CSV format:\n\n"
            csv_data += ",".join(column_names) + "\n"
            for row in rows:
                csv_data += ",".join(map(str, row)) + "\n"
            return csv_data
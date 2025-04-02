"""
title: 制造企业查询
author: Oliver Wen
date: 2025-03-27
version: 1.0
license: MIT
description: 制造企业查.
requirements: psycopg2-binary
"""

import os
import re
from typing import List, Union, Generator, Iterator
from click import prompt
from pydantic import BaseModel

from langchain_community.utilities import SQLDatabase
from langchain_ollama.llms import OllamaLLM
from langchain.chains.sql_database.query import create_sql_query_chain
from langchain_community.tools import QuerySQLDataBaseTool
from langchain_core.prompts import PromptTemplate
from operator import itemgetter

from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_core.runnables import RunnablePassthrough


class Pipeline:

    class Valves(BaseModel):
        DB_URL: str
        DB_TABLES: str
        OLLAMA_HOST: str
        MODEL: str
        MAX_ROWS: int

    # Update valves/ environment variables based on your selected database
    def __init__(self):
        self.name = "manufacturers_query"
        self.engine = None

        # Initialize
        self.valves = self.Valves(
            **{
                "pipelines": ["*"],
                "DB_URL": "postgresql+psycopg2://ai_user:Qdnkljc_2021@postgresql:5432/report_db",
                "DB_TABLES": "t_business_entity",
                "OLLAMA_HOST": "http://172.24.18.51:11434",
                "MODEL": "qwen2.5-32b-instruct-q8_0:latest",
                "MAX_ROWS": 10,
            }
        )


    def get_query(self,query_response: str):
        sql_pattern = r'```sql\n(.*?)\n```'
        query = re.search(sql_pattern, query_response, re.DOTALL).group(1).strip() # type: ignore
        return query
        
    def init_db_connection(self):
        # Update your DB connection string based on selected DB engine - current connection string is for Postgres
        try:
            self.engine = SQLDatabase.from_uri(self.valves.DB_URL)
        except Exception as e:
            return f"数据库连接错误: {str(e)}"

    async def on_startup(self):
        self.init_db_connection()
        pass

    async def on_shutdown(self):
        # This function is called when the server is stopped.
        pass

    def pipe(
        self, user_message: str, model_id: str, messages: List[dict], body: dict
    ) -> Union[str, Generator, Iterator]:
        db = self.engine
        if db is None:
            return "数据库初始化连接错误."

        llm = OllamaLLM(
            model=self.valves.MODEL,
            base_url=self.valves.OLLAMA_HOST,
            temperature=0,
            verbose=True
        )

        table_list: List[str] | None = (
            self.valves.DB_TABLES.split(",")
            if self.valves.DB_TABLES is not None
            else None
        )
        try:
            # 生成SQL的提示词
            table_list: List[str] | None = (
                self.valves.DB_TABLES.split(",")
                if self.valves.DB_TABLES is not None
                else None
            )

            sql_template = PromptTemplate(
                input_variables=["dialect", "input", "table_info", "top_k"],
                template="""Given an input question, create a syntactically correct {dialect} query to run to help find the answer. Unless the user specifies in his question a specific number of examples they wish to obtain, always limit your query to at most {top_k} results. You can order the results by a relevant column to return the most interesting examples in the database.
                    Never query for all the columns from a specific table, only ask for a the few relevant columns given the question.
                    Pay attention to use only the column names that you can see in the schema description. Be careful to not query for columns that do not exist. Also, pay attention to which column is in which table.

                    Only use the following tables:
                    {table_info}

                    Question: {input}""",
            ).partial(
                dialect=db.dialect,
                top_k=str(self.valves.MAX_ROWS),
                table_info=db.get_table_info(table_list),
            )

            chain = create_sql_query_chain(llm=llm, db=db, prompt=sql_template)
            query_response = chain.invoke({"question": user_message})
            query = self.get_query(query_response)
            execute_query_tool = QuerySQLDataBaseTool(db=db)
            query_result = execute_query_tool.invoke(query)  # type: ignore

            answer_prompt = (
                "根据以下用户问题，生成相应的SQL查询并执行查询返回结果，并以此依据回答用户问题，并以最美观和易读方式展现给用户.\n\n"
                f"问题: {user_message}\n"
                f"查询: {query}\n"
                f"结果: {query_result}"
            )
            response = llm.stream(answer_prompt)
            return response 
        except Exception as e:
            return f"模型会话错误: {str(e)}"

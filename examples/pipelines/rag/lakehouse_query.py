"""
title: 数据湖仓库查询
author: Oliver Wen
date: 2025-03-27
version: 1.0
license: MIT
description: 数据湖仓库查询.
requirements: pyspark
"""

from typing import List, Optional, Union, Generator, Iterator
from pydantic import BaseModel

from langchain_ollama.llms import OllamaLLM
from langchain_community.utilities.spark_sql import SparkSQL
from pyspark.sql import SparkSession
from langchain_community.agent_toolkits.spark_sql.toolkit import SparkSQLToolkit
from langchain_community.agent_toolkits.spark_sql.base import create_spark_sql_agent


class Pipeline:

    class Valves(BaseModel):
        SPARK_CONNECT_URL: str
        CATALOG: str
        SCHEMA: str
        TABLES: Optional[str] = None
        OLLAMA_HOST: str
        MODEL: str

    # Update valves/ environment variables based on your selected database
    def __init__(self):
        self.name = "lakehouse_query"
        self.llm = None
        self.spark = None

        # Initialize
        self.valves = self.Valves(
            **{
                "pipelines": ["*"],
                "SPARK_CONNECT_URL": "sc://localhost:15002",
                "CATALOG": "default",
                "SCHEMA": "manufacture",
                "OLLAMA_HOST": "http://localhost:11434",
                "MODEL": "qwen2.5-32b-instruct-q8_0:latest",
            }
        )

    def init(self):
        try:
            self.spark = (
                SparkSession.Builder()
                .remote(self.valves.SPARK_CONNECT_URL)
                .getOrCreate()
            )
            self.llm = OllamaLLM(
                model=self.valves.MODEL,
                base_url=self.valves.OLLAMA_HOST,
                temperature=0,
                verbose=True,
            )
        except Exception as e:
            return f"初始化Spark错误: {str(e)}"

    async def on_startup(self):
        self.init()
        pass

    async def on_shutdown(self):
        if self.spark is not None:
            self.spark.stop()
        pass

    def pipe(
        self, user_message: str, model_id: str, messages: List[dict], body: dict
    ) -> Union[str, Generator, Iterator]:
        spark = self.spark
        if spark is None:
            return "初始化spark错误."

        llm = self.llm
        if llm is None:
            return "模型创建错误."
        try:

            table_list: List[str] | None = (
                self.valves.TABLES.split(",")
                if self.valves.TABLES is not None
                else None
            )
            spark_sql = SparkSQL(
                spark_session=spark,
                schema=self.valves.SCHEMA,
                include_tables=table_list,
            )
            toolkit = SparkSQLToolkit(db=spark_sql, llm=llm)

            agent_executor = create_spark_sql_agent(
                llm=llm, toolkit=toolkit, verbose=True
            )
            query_result = agent_executor.invoke({"input": user_message})

            answer_prompt = (
                "根据以下用户问题生成相应的查询，将查询任务提交大数据计算引擎执行，执行完成返回结果，以此依据回答用户问题，并以最美观和易读方式展现给用户.\n\n"
                f"问题: {user_message}\n"
                f"结果: {query_result}"
            )
            response = llm.stream(answer_prompt)
            return response
        except Exception as e:
            return f"模型会话错误: {str(e)}"

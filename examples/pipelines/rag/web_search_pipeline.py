"""
title: Web搜索引擎
author: Oliver Wen
date: 2025-03-31
version: 1.0
license: MIT
description: Web搜索引擎.
"""

import os
import re
from typing import List, Union, Generator, Iterator
from click import prompt
from pydantic import BaseModel
from langchain_ollama.llms import OllamaLLM
import asyncio
import aiohttp
import requests
from typing import Any, List
from urllib.parse import quote


class Pipeline:

    class Valves(BaseModel):
        SEARCH_API_URL: str
        OLLAMA_HOST: str
        MODEL: str
        MAX_SITES: int

    # Update valves/ environment variables based on your selected database
    def __init__(self):
        self.name = "web_search_pipelines"
        self.llm = None

        # Initialize
        self.valves = self.Valves(
            **{
                "pipelines": ["*"],
                "SEARCH_API_URL": "http://searxng:8080/search?format=json",
                "OLLAMA_HOST": "http://172.24.18.51:11434",
                "MODEL": "qwq:32b-q8_0",
                "MAX_SITES": 20,
            }
        )

    async def web_scrape(self, urls: List[str]) -> str:
        """
        Scrape multiple web pages using r.jina.ai API

        :param urls: List of URLs of the web pages to scrape.
        :param user_request: The user's original request or query.
        :return: Combined scraped contents, or error messages.
        """

        combined_results = []

        async def process_url(url):
            jina_url = f"https://r.jina.ai/{url}"
            headers = {
                "Authorization": "Bearer jina_8219f23fe013449482367f56b87bcc3dAOwIxJr9hJHkUhcg7VTKe_dJ8hxU",
                "X-No-Cache": "true",
                "X-With-Images-Summary": "true",
                "X-With-Links-Summary": "true",
            }

            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(jina_url, headers=headers) as response:
                        response.raise_for_status()
                        content = await response.text()

                title = f"Scraped content from {url}"
                return f"URL: {url}\n内容: {content}\n"

            except aiohttp.ClientError as e:
                error_message = f"读取网页 {url} 时出错: {str(e)}"
                return f"URL: {url}\n错误: {error_message}\n"

        tasks = [process_url(url) for url in urls]
        results = await asyncio.gather(*tasks)
        combined_results.extend(results)
        # 将所有结果合并为一个字符串
        return "\n".join(combined_results)

    async def on_startup(self):
        llm = OllamaLLM(
            model=self.valves.MODEL, base_url=self.valves.OLLAMA_HOST, verbose=True
        )
        self.llm = llm
        pass

    async def on_shutdown(self):
        # This function is called when the server is stopped.
        pass

    async def pipe(
        self, user_message: str, model_id: str, messages: List[dict], body: dict
    ) -> Union[str, Generator, Iterator]:
        llm = self.llm
        if llm is None:
            return "模型初始化错误."

        encoded_query = quote(user_message)
        search_url = f"{self.valves.SEARCH_API_URL}&q={encoded_query}"

        try:
            response = requests.get(url=search_url)
            response.raise_for_status()
            search_results = await response.json()
            print(search_results)
            urls = [result["url"] for result in search_results.get("results", [])[:4]]
            
            if not urls:
                return "搜索未返回任何结果"

            scraped_content = await self.web_scrape(urls)

            # 构建最终返回的字符串
            final_result = (
                f"用户查询: {user_message}\n\n搜索结果及网页内容:\n{scraped_content}"
            )
            print(final_result)
            return final_result

        except Exception as e:
            error_message = f"搜索时发生错误: {str(e)}"
            return error_message

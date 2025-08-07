import os
import re
import time
import logging
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# 配置与日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("movie.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

CONFIG = {
    "MAX_MOVIES": 200,
    "SLEEP_TIME": 2
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
    "Referer": "https://movie.douban.com/"
}


# 工具函数
def create_session():
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1)
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update(HEADERS)
    return session


def check_env():
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if not os.path.exists(env_path):
        logger.error(f"未找到.env文件（路径：{env_path}）")
        return False, None
    load_dotenv(env_path)
    api_key = os.getenv("DEEPSEEK_API_KEY")
    return (True, api_key) if api_key and api_key.startswith("sk-") else (False, None)


# 豆瓣爬虫（修复字段缺失问题）
class DoubanCrawler:
    def __init__(self):
        self.session = create_session()

    def get_top250(self):
        movies = []
        for start in range(0, CONFIG["MAX_MOVIES"], 25):
            url = f"https://movie.douban.com/top250?start={start}&filter="
            try:
                resp = self.session.get(url, timeout=10)
                soup = BeautifulSoup(resp.text, "html.parser")
                for item in soup.select(".grid_view .item")[:CONFIG["MAX_MOVIES"] - len(movies)]:
                    link = item.select_one(".hd a")["href"]
                    movie_id = re.search(r"subject/(\d+)/", link).group(1)
                    title = item.select_one(".title").text.strip()
                    movies.append({"id": movie_id, "title": title})
                if len(movies) >= CONFIG["MAX_MOVIES"]:
                    break
            except Exception as e:
                logger.error(f"爬取列表失败：{e}")
                break
        return movies

    def get_detail(self, movie_id):
        # 确保初始化时包含所有必要字段，防止缺失
        detail = {
            "id": movie_id,
            "title": "未知电影",
            "director": "未知导演",
            "actors": ["未知主演"],  # 强制包含actors字段
            "release_date": "未知时间",
            "genres": ["未知类型"]
        }
        try:
            resp = self.session.get(f"https://movie.douban.com/subject/{movie_id}/", timeout=10)
            soup = BeautifulSoup(resp.text, "html.parser")

            # 提取标题
            title_tag = soup.select_one("h1 span")
            if title_tag:
                detail["title"] = title_tag.text.strip()

            # 提取导演
            director_label = soup.find(string=re.compile(r"导演[:：]"))
            if director_label and director_label.parent:
                director_links = director_label.parent.find_next_siblings("a")
                if director_links:
                    detail["director"] = ",".join([a.text.strip() for a in director_links])

            # 提取主演（确保actors始终是列表）
            actor_label = soup.find(string=re.compile(r"主演[:：]"))
            if actor_label and actor_label.parent:
                actor_links = actor_label.parent.find_next_siblings("a")[:5]
                if actor_links:
                    detail["actors"] = [a.text.strip() for a in actor_links]

            # 提取上映时间
            date_label = soup.find(string=re.compile(r"上映日期[:：]"))
            if date_label and date_label.parent:
                date_text = date_label.parent.text.replace(date_label, "").strip()
                detail["release_date"] = re.sub(r"\(.*\)", "", date_text).strip()

        except Exception as e:
            logger.error(f"提取详情失败（ID：{movie_id}）：{e}")

        return detail  # 确保返回的字典一定包含actors


# AI总结器
class DeepSeekProcessor:
    def __init__(self, api_key):
        self.api_key = api_key

    def summarize(self, info):
        try:
            resp = requests.post(
                "https://api.deepseek.com/v1/chat/completions",
                json={
                    "model": "deepseek-chat",
                    "messages": [{"role": "user", "content": f"简要总结《{info['title']}》的剧情亮点（30字内）"}]
                },
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json"
                },
                timeout=15
            )
            return resp.json()["choices"][0]["message"]["content"]
        except Exception as e:
            return f"总结失败：{str(e)[:20]}"


# CSV导出器（修复KeyError）
class CSVExporter:
    def save(self, data):
        # 确保所有字段都存在，防止缺失
        df = pd.DataFrame(data)
        # 强制添加缺失的列（如果有）
        for col in ["id", "title", "director", "actors", "release_date"]:
            if col not in df.columns:
                df[col] = "未知" if col != "actors" else ["未知主演"]

        # 处理actors列（确保是列表格式）
        if "actors" in df.columns:
            df["actors"] = df["actors"].apply(
                lambda x: ",".join(x) if isinstance(x, list) else str(x)
            )

        df.to_csv("movie_summary.csv", index=False, encoding="utf-8-sig")
        logger.info(f"已保存CSV（{len(df)}条数据）")


# 主程序
def main():
    env_ok, api_key = check_env()
    if not env_ok:
        return

    crawler = DoubanCrawler()
    movies = crawler.get_top250()
    if not movies:
        logger.error("未获取到电影列表")
        return

    processor = DeepSeekProcessor(api_key)
    exporter = CSVExporter()
    data = []

    for movie in movies:
        detail = crawler.get_detail(movie["id"])
        # 构建数据时明确包含所有字段，与CSV列对应
        data.append({
            "id": detail["id"],
            "title": detail["title"],
            "director": detail["director"],
            "actors": detail["actors"],  # 确保添加actors
            "release_date": detail["release_date"],
            "AI总结": processor.summarize(detail)
        })
        time.sleep(CONFIG["SLEEP_TIME"])

    exporter.save(data)


if __name__ == "__main__":
    main()
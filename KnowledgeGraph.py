# -*- coding: utf-8 -*-

import re
import numpy as np
import pandas as pd
from py2neo import Graph, Node, Relationship

## 配置信息（请修改为你的实际信息）
NEO4J_BOLT_URL = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "yourpassword"  # 替换为你的Neo4j密码
CSV_PATH = "C:/MovieProject/movieInfo.csv"  # 替换为你的CSV文件路径


def merge_main_node(tx, node, label, key):
    """合并主体节点，确保唯一性"""
    tx.merge(node, label, key)
    return node


def main():
    try:
        ## 连接Neo4j数据库
        graph = Graph(NEO4J_BOLT_URL, auth=(NEO4J_USER, NEO4J_PASSWORD))
        graph.delete_all()  # 清空现有数据
        print("已清空数据库中现有数据")

        ## 读取CSV数据
        try:
            storageData = pd.read_csv(CSV_PATH, encoding='utf-8')
            print(f"✅ 成功读取CSV数据，共{len(storageData)}部电影")
        except FileNotFoundError:
            print(f"❌ 未找到CSV文件：{CSV_PATH}")
            return
        except Exception as e:
            print(f"❌ 读取CSV文件出错：{str(e)}")
            return

        ## 数据预处理 - 只关注核心主体和必要属性
        # 1. 确保必要列存在
        main_columns = ["title", "director", "actor"]
        other_columns = ["rate", "num", "info", "time", "country", "type"]
        all_required = main_columns + other_columns

        missing_columns = [col for col in all_required if col not in storageData.columns]
        if missing_columns:
            print(f"❌ CSV文件缺少必要的列：{', '.join(missing_columns)}")
            return

        # 2. 处理空值
        for col in main_columns:  # 主体相关列特殊处理
            storageData[col] = storageData[col].fillna(f"未知{col}").astype(str).str.strip()

        for col in other_columns:  # 附属属性处理
            storageData[col] = storageData[col].fillna(f"未知{col}").astype(str).str.strip()

        # 3. 处理数值型附属属性
        try:
            # 处理评分
            storageData["rate"] = pd.to_numeric(storageData["rate"], errors='coerce')
            storageData["rate"] = storageData["rate"].fillna(0.0)

            # 处理评价人数
            storageData["num"] = storageData["num"].apply(
                lambda x: re.findall(r'\d+', str(x))[0] if re.findall(r'\d+', str(x)) else "0"
            ).astype(int)
        except Exception as e:
            print(f"⚠️ 处理数值属性时出错：{str(e)}，将使用原始值")

        ## 构建知识图谱 - 以电影、导演、演员为主体
        total_movies = len(storageData)
        batch_size = 100  # 批量处理大小

        for batch_start in range(0, total_movies, batch_size):
            batch_end = min(batch_start + batch_size, total_movies)
            tx = graph.begin()  # 开始事务

            for i in range(batch_start, batch_end):
                try:
                    # 获取当前记录
                    row = storageData.iloc[i]

                    # 1. 创建电影主体节点（核心主体）
                    movie_title = row["title"].strip()
                    if not movie_title or movie_title == "未知title":
                        continue

                    # 电影节点包含所有附属属性
                    movie_attrs = {
                        "title": movie_title,
                        "rate": float(row["rate"]),
                        "num": int(row["num"]),
                        "info": row["info"],
                        "time": row["time"],
                        "country": row["country"],
                        "type": row["type"]
                    }
                    movie_node = Node("Movie", **movie_attrs)
                    merge_main_node(tx, movie_node, "Movie", "title")  # 以title为主键

                    # 2. 处理导演主体节点及关系
                    directors = [d.strip() for d in row["director"].split('/')
                                 if d.strip() and d != "未知director"]
                    for director in directors:
                        director_node = Node("Director", name=director)
                        merge_main_node(tx, director_node, "Director", "name")  # 以name为主键
                        # 建立导演-执导-电影关系
                        tx.create(Relationship(director_node, "执导", movie_node))

                    # 3. 处理演员主体节点及关系
                    actors = [a.strip() for a in row["actor"].split('/')
                              if a.strip() and a != "未知actor"]
                    for actor in actors:
                        actor_node = Node("Actor", name=actor)
                        merge_main_node(tx, actor_node, "Actor", "name")  # 以name为主键
                        # 建立演员-出演-电影关系
                        tx.create(Relationship(actor_node, "出演", movie_node))

                except Exception as e:
                    print(f"⚠️ 处理第{i + 1}条记录时出错：{str(e)}，已跳过")
                    continue

            # 提交当前批次
            tx.commit()
            print(f"已处理 {batch_end}/{total_movies} 条记录")

        print("✅ 知识图谱构建完成！核心主体为：电影、导演、演员")
        print("   电影节点包含附属属性：评分、评价人数、信息、上映时间、国家、类型")

    except Exception as e:
        print(f"❌ 程序运行出错：{str(e)}")


if __name__ == "__main__":
    main()

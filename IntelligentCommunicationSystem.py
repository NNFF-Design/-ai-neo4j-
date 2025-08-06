# -*- coding: utf-8 -*-

import jieba
from py2neo import Graph
import sys
import time

# -------------------------- 配置信息（请修改为你的实际信息） --------------------------
NEO4J_BOLT_URL = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "yourpassword"  # 替换为你的Neo4j密码
USER_DICT_PATH = "C:/MovieProject/selfDefiningTxt.txt"  # 自定义词典路径


# -------------------------------------------------------------------------------------

def init_system():
    """初始化系统：连接数据库、加载词典"""
    print("正在初始化系统...")

    # 连接Neo4j数据库
    try:
        graph = Graph(NEO4J_BOLT_URL, auth=(NEO4J_USER, NEO4J_PASSWORD))
        print("✅ 数据库连接成功")
    except Exception as e:
        print(f"❌ 数据库连接失败: {str(e)}")
        sys.exit(1)

    # 加载自定义分词词典
    try:
        jieba.load_userdict(USER_DICT_PATH)
        sys.stdout.flush()
        time.sleep(0.5)
        print("✅ 分词词典加载成功")
    except FileNotFoundError:
        print(f"❌ 未找到词典文件: {USER_DICT_PATH}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ 词典加载失败: {str(e)}")
        sys.exit(1)

    return graph


# 问题意图模板（键与movieInfo.csv列名一致）
stencil = {
    "director": ["导演", "执导", "导演是谁", "谁导演的"],
    "type": ["类型", "种类", "是什么类型", "属于什么类型"],
    "time": ["上映时间", "什么时候上映", "上映日期"],
    "rate": ["评分", "分数", "豆瓣评分", "评分为多少"],
    "num": ["评价人数", "多少人评价", "评分人数"],
    "actor": ["演员", "主演", "谁演的", "演员表"],
    "country": ["国家", "哪个国家的", "产地"]
}

# 回答模板
responseDict = {
    "director": "%s这部电影的导演为'%s'",
    "type": "%s这部电影的类型为'%s'",
    "time": "%s这部电影的上映时间为'%s'",
    "rate": "%s这部电影的评分为'%s'",
    "num": "%s这部电影的评价人数为'%s'",
    "actor": "%s这部电影的主演为'%s'",
    "country": "%s这部电影的制片国家为'%s'"
}


def getMovieName(queryText, user_dict_path):
    """提取电影名称"""
    try:
        with open(user_dict_path, 'r', encoding='utf-8') as f:
            user_dict_movies = [line.strip().split()[0] for line in f if line.strip()]
    except Exception as e:
        print(f"【调试】读取词典失败: {str(e)}")
        return None

    # 优先匹配词典中的电影名
    for movie in user_dict_movies:
        if movie in queryText:
            return movie.strip()

    # 备选方案
    stopwords = ["的", "是", "谁", "？", "电影", "什么", "时候", "多少"]
    words = jieba.lcut(queryText)
    candidate = [word for word in words if word not in stopwords and len(word) > 2]
    return candidate[0] if candidate else None


def AssignIntension(queryText):
    """识别问题意图"""
    for prop in stencil:
        for keyword in stencil[prop]:
            if keyword in queryText:
                return {prop: keyword}
    return {"未知": "未识别意图"}


def SearchGraph(graph, movieName, intension):
    """查询知识图谱（修复Cypher注释错误）"""
    if "未知" in intension:
        return "未知", "抱歉，未能理解你的问题"

    prop = list(intension.keys())[0]

    # 修复：移除Cypher语句中的#注释，避免语法错误
    # 若需要注释，使用Cypher支持的//注释（但建议在代码中注释，而非Cypher语句内）
    cypher = f"MATCH (m:movie) WHERE trim(m.title) = trim('{movieName}') RETURN m.`{prop}`, m.title"

    try:
        result = graph.run(cypher).data()
    except Exception as e:
        return prop, f"查询出错：{str(e)}"

    if not result:
        return prop, f"未找到电影《{movieName}》的信息"
    if f"m.`{prop}`" not in result[0]:
        return prop, f"电影《{movieName}》没有{prop}属性"
    if result[0][f"m.`{prop}`"] in [None, "", " "]:
        return prop, f"电影《{movieName}》的{prop}信息未填写"

    return prop, str(result[0][f"m.`{prop}`"])


def respondQuery(movieName, classification, result):
    """生成回答"""
    if classification in responseDict:
        print(responseDict[classification] % (movieName, result))
    else:
        print(result)


def main():
    graph = init_system()
    print("\n系统准备就绪，可以开始提问了！")
    queryText = input("请输入关于电影的问题（例如：肖申克的救赎的导演是谁？）：")

    movieName = getMovieName(queryText, USER_DICT_PATH)
    if not movieName:
        print("抱歉，未能识别电影名称，请检查电影名是否正确")
        return

    intension = AssignIntension(queryText)
    classification, result = SearchGraph(graph, movieName, intension)
    respondQuery(movieName, classification, result)


if __name__ == "__main__":
    main()

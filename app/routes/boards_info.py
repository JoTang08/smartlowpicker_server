import akshare as ak
from flask import jsonify, request
import pandas as pd
import numpy as np


def get_boards_api():
    """
    获取所有行业板块信息
    """
    try:
        df = ak.stock_board_industry_name_em()
        return jsonify(
            {"code": 0, "data": df.to_dict(orient="records"), "message": "获取成功"}
        )
    except Exception as e:
        return jsonify({"error": str(e), "message": "获取所有行业板块信息失败"}), 500


# 将不可序列化类型统一转换
def normalize(x):
    if isinstance(x, (np.generic, np.number)):
        return x.item()
    elif isinstance(x, (pd.Timestamp, pd.Timedelta)):
        return str(x)
    elif isinstance(x, (list, dict, tuple)):
        return x
    elif x is None:
        return None
    else:
        return str(x)


def get_board_members_api():
    """
    获取某行业板块的成分股信息
    """
    try:
        boardName = (
            request.json.get("boardName")
            if request.is_json
            else request.form.get("boardName")
        )

        if not boardName:
            return jsonify({"error": "缺少参数: boardName"}), 400

        df = ak.stock_board_industry_cons_em(symbol=boardName)
        print(f"成分股的类型：{type(df)}")
        df = df.applymap(normalize)
        data = df.to_dict(orient="records")
        response = jsonify(
            {
                "code": 0,
                "board": boardName,
                "data": data,
                "message": "获取成功",
            }
        )
        print(f"获取成功: {type(response)}")
        return response
    except Exception as e:
        return (
            jsonify({"error": str(e), "message": "获取某行业板块的成分股信息失败"}),
            500,
        )


def get_concepts_api():
    """
    获取所有概念板块信息
    """
    df = ak.stock_board_concept_name_em()
    return jsonify(df.to_dict(orient="records"))


def get_concept_members_api():
    """
    获取某概念板块的成分股信息
    """
    if request.method == "GET":
        concept_name = request.args.get("concept_name")
    else:
        concept_name = (
            request.json.get("concept_name")
            if request.is_json
            else request.form.get("code")
        )

    if not concept_name:
        return jsonify({"error": "缺少参数: concept_name"}), 400

    df = ak.stock_board_concept_cons_em(symbol=concept_name)
    return jsonify({"concept": concept_name, "data": df.to_dict(orient="records")})

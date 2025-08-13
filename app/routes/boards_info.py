import akshare as ak
from flask import jsonify, request


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
        return jsonify(
            {
                "code": 0,
                "board": boardName,
                "data": df.to_dict(orient="records"),
                "message": "获取成功",
            }
        )
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

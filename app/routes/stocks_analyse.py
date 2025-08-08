from flask import request, jsonify
import pandas as pd
import os
from datetime import datetime, timedelta
import akshare as ak
import numpy as np

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
HISTORY_CACHE_DIR = os.path.join(BASE_DIR, "history_cache")
STOCK_INFO_DIR = os.path.join(BASE_DIR, "stocks_info")
LIST_CSV_PATH = os.path.join(BASE_DIR, "stocks_info", "list.csv")
stock_list_df = pd.read_csv(LIST_CSV_PATH, dtype=str)
code_name_map = dict(zip(stock_list_df["code"], stock_list_df["name"]))

MARGIN_FILE_SSE = os.path.join(BASE_DIR, "stocks_info", "margin_sse.csv")
MARGIN_FILE_SZSE = os.path.join(BASE_DIR, "stocks_info", "margin_szse.csv")


# 近30天的交易日期
def get_recent_trade_dates(days=30):
    """
    获取最近days个自然日内的工作日（排除周末）
    """
    trade_dates = []
    today = datetime.now()

    for i in range(days * 2):  # 乘2避免因周末不足够天数
        day = today - timedelta(days=i)
        if day.weekday() < 5:  # 周一~周五为0~4
            trade_dates.append(day.strftime("%Y%m%d"))
            if len(trade_dates) >= days:
                break

    return sorted(trade_dates)


# 上交所融资融券数据（SSE）
def fetch_and_update_margin_data_sse(code: str, days=30):
    """
    拉取上交所单股融资融券数据，近N日，保存至 margin_sse.csv，自动去重。
    """
    dates = get_recent_trade_dates(days)
    file_path = os.path.join(BASE_DIR, "stocks_info", "margin_sse.csv")

    dfs = []
    for date_str in dates:
        try:
            print(f"正在获取上交所{date_str}的融资融券数据中。。。")
            df = ak.stock_margin_detail_sse(date=date_str)
            stock_df = df[df["标的证券代码"] == code]
            if not stock_df.empty:
                stock_df = stock_df.copy()
                stock_df["日期"] = date_str
                dfs.append(stock_df)
        except Exception as e:
            print(f"[SSE] 日期 {date_str} 拉取失败: {e}")
            continue

    if not dfs:
        print(f"[SSE] {code} 近{days}日无数据")
        return []

    new_data = pd.concat(dfs, ignore_index=True)

    # 直接覆盖写入，不保留历史数据
    new_data.to_csv(file_path, index=False, encoding="utf-8-sig")
    print(f"[SSE] {code} 数据已保存至 {file_path}（已覆盖旧数据）")

    return new_data.to_dict(orient="records")


# 深交所融资融券数据（SZSE）
def fetch_and_update_margin_data_szse(code: str, days=30):
    """
    拉取深交所单股融资融券数据，近N日，保存至 margin_szse.csv，自动去重。
    """
    dates = get_recent_trade_dates(days)
    file_path = os.path.join(BASE_DIR, "stocks_info", "margin_szse.csv")

    dfs = []
    for date_str in dates:
        try:
            print(f"正在获取深交所{date_str}的融资融券数据中。。。")
            df = ak.stock_margin_detail_szse(date=date_str)
            stock_df = df[df["证券代码"] == code]
            if not stock_df.empty:
                stock_df = stock_df.copy()
                stock_df["日期"] = date_str
                dfs.append(stock_df)
        except Exception as e:
            print(f"[SZSE] 日期 {date_str} 拉取失败: {e}")
            continue

    if not dfs:
        print(f"[SZSE] {code} 近{days}日无数据")
        return []

    new_data = pd.concat(dfs, ignore_index=True)

    # 覆盖旧文件，直接写入
    new_data.to_csv(file_path, index=False, encoding="utf-8-sig")
    print(f"[SZSE] {code} 数据已保存至 {file_path}（已覆盖旧数据）")

    return new_data.to_dict(orient="records")


def to_native_types(d):
    # 递归把numpy类型转成普通python类型
    if isinstance(d, dict):
        return {k: to_native_types(v) for k, v in d.items()}
    elif isinstance(d, list):
        return [to_native_types(i) for i in d]
    elif isinstance(d, (np.integer,)):
        return int(d)
    elif isinstance(d, (np.floating,)):
        return float(d)
    else:
        return d


def analyze_margin_data_sse(code: str):
    """
    分析某只股票的融资融券数据（上海交易所格式）
    输入股票代码和文件路径，返回分析结果字典
    """
    # 读取数据
    try:
        filepath = os.path.join(BASE_DIR, "stocks_info", "margin_sse.csv")
        df = pd.read_csv(filepath, dtype={"标的证券代码": str})
    except FileNotFoundError:
        return f"文件 {filepath} 不存在"
    except Exception as e:
        return f"读取文件失败: {e}"

    # 过滤股票代码
    df_code = df[df["标的证券代码"] == code]
    if df_code.empty:
        return f"文件中无股票代码 {code} 的数据"

    # 保证日期排序
    df_code = df_code.sort_values(by="信用交易日期")

    # 计算关键指标
    financing_balance_trend = df_code[["信用交易日期", "融资余额"]].copy()
    financing_balance_trend["融资余额"] = financing_balance_trend["融资余额"].astype(
        float
    )

    total_financing_buy = df_code["融资买入额"].astype(float).sum()
    total_financing_repay = df_code["融资偿还额"].astype(float).sum()
    total_short_balance_change = (
        df_code["融券余量"].astype(float).iloc[-1]
        - df_code["融券余量"].astype(float).iloc[0]
    )
    total_short_sell = df_code["融券卖出量"].astype(float).sum()
    total_short_repay = df_code["融券偿还量"].astype(float).sum()

    analysis = {
        "股票代码": code,
        "数据起始日期": df_code["信用交易日期"].iloc[0],
        "数据截止日期": df_code["信用交易日期"].iloc[-1],
        "融资余额趋势": financing_balance_trend.reset_index(drop=True).to_dict(
            orient="records"
        ),
        "融资买入总额": total_financing_buy,
        "融资偿还总额": total_financing_repay,
        "融券余量净变化": total_short_balance_change,
        "融券卖出总量": total_short_sell,
        "融券偿还总量": total_short_repay,
    }
    print(f"{to_native_types(analysis)}")
    return to_native_types(analysis)


# 获取股票代码的融资融券数据
def fetch_and_update_margin_by_code_api():
    try:
        data = request.get_json()
        code = data.get("code")
        days = int(data.get("days", 30))

        if not code:
            return jsonify({"code": 1, "message": "缺少参数：code"}), 400

        if code.startswith("6"):  # 上交所
            records = fetch_and_update_margin_data_sse(code, days)
        elif code.startswith(("0", "3")):  # 深交所
            records = fetch_and_update_margin_data_szse(code, days)
        else:
            return jsonify({"code": 2, "message": f"不支持的股票代码格式：{code}"}), 400

        return jsonify(
            {
                "code": 0,
                "message": f"{code} 融资融券数据更新成功，共获取 {len(records)} 条记录",
                "data": records,
            }
        )

    except Exception as e:
        return jsonify({"code": -1, "message": f"接口异常：{str(e)}"}), 500


def get_history_cache_count_api():
    try:
        files = [f for f in os.listdir(HISTORY_CACHE_DIR) if f.endswith(".csv")]
        count = len(files)
        return jsonify({"code": 0, "message": "成功", "count": count})
    except Exception as e:
        return jsonify({"code": -1, "message": f"接口异常：{str(e)}"}), 500


def analyze_batch_api():
    try:
        # 获取参数
        data = request.get_json()
        start_index = int(data.get("start", 0))
        end_index = int(data.get("end", 0))
        days = int(data.get("days", 180))
        threshold = float(data.get("threshold", 1.05))

        # 获取所有CSV文件
        files = sorted([f for f in os.listdir(HISTORY_CACHE_DIR) if f.endswith(".csv")])
        total = len(files)

        # 参数校验
        if start_index < 0 or end_index > total or start_index >= end_index:
            return (
                jsonify(
                    {
                        "code": 1,
                        "message": f"索引范围无效，应在 0 到 {total} 之间",
                    }
                ),
                400,
            )

        today = datetime.now()
        cutoff_date = today - timedelta(days=days)

        results = []
        for idx in range(start_index, end_index):
            file = files[idx]
            code = file.replace(".csv", "")
            path = os.path.join(HISTORY_CACHE_DIR, file)

            try:
                df = pd.read_csv(path)
                df["日期"] = pd.to_datetime(df["日期"])
                df = df[df["日期"] >= cutoff_date]

                if df.empty:
                    continue

                min_price = df["最低"].astype(float).min()
                max_price = df["最高"].astype(float).max()
                current_price = float(df.iloc[-1]["收盘"])

                if current_price <= min_price * threshold:
                    results.append(
                        {
                            "股票代码": code,
                            "股票名称": code_name_map.get(code, "未知名称"),
                            "当前价": current_price,
                            "阶段最低": min_price,
                            "阶段最高": max_price,
                            "涨跌幅（%）": f"{(current_price - min_price) / min_price * 100:.2f}%",
                        }
                    )

            except Exception as e:
                print(f"处理 {file} 出错：{e}")
                continue

        # 保存结果，避免重复写入
        # 根据 days 拼接文件名
        save_path = os.path.join(
            BASE_DIR, "stocks_info", f"low_price_stocks_{days}.csv"
        )

        new_df = pd.DataFrame(results)
        # 直接覆盖写入，不合并，不去重
        new_df.to_csv(save_path, index=False, encoding="utf-8-sig")

        return jsonify(
            {
                "code": 0,
                "message": f"分析完成（{days}天）：处理了 {end_index - start_index} 只股票，新增 {len(new_df)} 条低价股票，结果保存在 {os.path.basename(save_path)}",
                "total": total,
                "start_index": start_index,
                "end_index": end_index,
                "count": len(results),
                "data": results,
            }
        )

    except Exception as e:
        return jsonify({"code": -1, "message": f"接口异常：{str(e)}"}), 500


def get_analyze_batch_data_api():
    try:
        # 获取 days 参数，默认值为 90
        data = request.get_json()
        days = int(data.get("days", 90))
        # 构造文件路径
        filename = f"low_price_stocks_{days}.csv"
        file_path = os.path.join(STOCK_INFO_DIR, filename)

        # 判断文件是否存在
        if not os.path.exists(file_path):
            return (
                jsonify(
                    {
                        "code": 1,
                        "message": f"不存在 {days} 天的分析数据文件：{filename}",
                    }
                ),
                404,
            )
        print(f"正在读取{file_path}文件内容。")
        # 读取 CSV 文件
        df = pd.read_csv(file_path, dtype=str)
        data = df.to_dict(orient="records")

        return jsonify(
            {
                "code": 0,
                "message": f"成功读取 {filename}",
                "days": days,
                "count": len(data),
                "data": data,
            }
        )

    except Exception as e:
        return jsonify({"code": -1, "message": f"服务器异常：{str(e)}"}), 500


def get_low_price_stocks_api():
    try:
        # 获取 days 参数，默认是 180
        days = int(request.args.get("days", 180))

        # 拼接文件路径
        file_path = os.path.join(
            BASE_DIR, "stocks_info", f"low_price_stocks_{days}.csv"
        )

        if not os.path.exists(file_path):
            return (
                jsonify({"code": 1, "message": f"低价股票文件不存在（{days}天）"}),
                404,
            )

        df = pd.read_csv(file_path, dtype=str)
        data = df.to_dict(orient="records")

        return jsonify(
            {
                "code": 0,
                "message": f"成功获取 {days} 天的低价股票数据",
                "count": len(data),
                "data": data,
            }
        )

    except Exception as e:
        return jsonify({"code": -1, "message": f"接口异常：{str(e)}"}), 500


def list_low_price_stock_files_api():
    try:
        stocks_info_dir = os.path.join(BASE_DIR, "stocks_info")
        files = os.listdir(stocks_info_dir)

        options = []
        for f in files:
            if f.startswith("low_price_stocks_") and f.endswith(".csv"):
                try:
                    # 提取 days 值，例如 low_price_stocks_180.csv → 180
                    days = int(f.replace("low_price_stocks_", "").replace(".csv", ""))
                    options.append(days)
                except ValueError:
                    continue  # 文件名不符合格式就跳过

        options.sort()
        return jsonify(
            {
                "code": 0,
                "message": "成功获取可用的 low_price_stocks 文件列表",
                "options": options,
            }
        )
    except Exception as e:
        return jsonify({"code": -1, "message": f"接口异常：{str(e)}"}), 500

from flask import request, jsonify
import pandas as pd
import os
from datetime import datetime, timedelta
import akshare as ak
import numpy as np
from pandas.errors import EmptyDataError

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
HISTORY_CACHE_DIR = os.path.join(BASE_DIR, "history_cache")
STOCK_INFO_DIR = os.path.join(BASE_DIR, "stocks_info")
LIST_CSV_PATH = os.path.join(BASE_DIR, "stocks_info", "list.csv")
stock_list_df = pd.read_csv(LIST_CSV_PATH, dtype=str)
code_name_map = dict(zip(stock_list_df["code"], stock_list_df["name"]))

MARGIN_FILE_SSE = os.path.join(BASE_DIR, "stocks_info", "margin_sse.csv")
MARGIN_FILE_SZSE = os.path.join(BASE_DIR, "stocks_info", "margin_szse.csv")

WATCHLIST_FILE = os.path.join(BASE_DIR, "stocks_info", "watched_stocks.csv")


def add_to_watchlist_api():
    try:
        data = request.get_json()
        code = data.get("code")
        name = data.get("name", "未知名称")

        if not code:
            return jsonify({"code": 1, "message": "股票代码不能为空"}), 400

        # 如果文件存在且非空才读取
        if os.path.exists(WATCHLIST_FILE) and os.path.getsize(WATCHLIST_FILE) > 0:
            df = pd.read_csv(WATCHLIST_FILE, dtype=str)
        else:
            df = pd.DataFrame(columns=["股票代码", "股票名称"])

        # 检查是否已存在
        if not df[df["股票代码"] == code].empty:
            return jsonify({"code": 0, "message": f"{code} 已在关注列表中"})

        # 添加
        new_row = pd.DataFrame([[code, name]], columns=["股票代码", "股票名称"])
        df = pd.concat([df, new_row], ignore_index=True)
        df.to_csv(WATCHLIST_FILE, index=False, encoding="utf-8-sig")

        return jsonify({"code": 0, "message": f"已添加 {code} 到关注列表"})
    except Exception as e:
        return jsonify({"code": -1, "message": f"添加失败: {str(e)}"}), 500


def remove_to_watchlist_api():
    try:
        data = request.get_json()
        code = data.get("code")

        if not code:
            return jsonify({"code": 1, "message": "股票代码不能为空"}), 400

        # 文件不存在或为空
        if not os.path.exists(WATCHLIST_FILE) or os.path.getsize(WATCHLIST_FILE) == 0:
            return jsonify({"code": 0, "message": "关注列表为空"})

        df = pd.read_csv(WATCHLIST_FILE, dtype=str)
        before_count = len(df)
        df = df[df["股票代码"] != code]

        if len(df) == before_count:
            return jsonify({"code": 0, "message": f"{code} 不在关注列表中"})

        df.to_csv(WATCHLIST_FILE, index=False, encoding="utf-8-sig")

        return jsonify({"code": 0, "message": f"已移除 {code} 从关注列表"})
    except Exception as e:
        return jsonify({"code": -1, "message": f"移除失败: {str(e)}"}), 500


def get_watched_stocks_api():
    try:
        df = pd.read_csv(WATCHLIST_FILE, dtype=str)
        if "股票代码" in df.columns and "股票名称" in df.columns:
            watched_list = (
                df[["股票代码", "股票名称"]].drop_duplicates().to_dict(orient="records")
            )
            return jsonify({"code": 0, "data": watched_list, "message": "获取成功"})
        else:
            return []
    except FileNotFoundError:
        print("关注股票文件不存在")
        return jsonify({"code": -1, "message": "关注股票文件不存在"})
    except Exception as e:
        print(f"读取关注股票文件出错: {e}")
        return jsonify({"code": -1, "message": "读取关注股票文件出错"})


df = ak.tool_trade_date_hist_sina()

# 确保转换为 datetime
df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")

TRADE_DATES = df["trade_date"].dt.strftime("%Y%m%d").tolist()


def get_recent_trade_dates(days=30):
    today = datetime.now().strftime("%Y%m%d")
    idx = next((i for i, d in enumerate(TRADE_DATES) if d > today), len(TRADE_DATES))
    return TRADE_DATES[max(0, idx - days) : idx]


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


def update_market_file(
    market: str, file_path: str, date_col: str, ak_fetch_func, dates
):
    """
    更新某个市场的融资融券数据（全量模式，不按 code 过滤）。
    market: "SSE" or "SZSE"
    file_path: CSV 文件路径
    date_col: 日期列名 (SSE="信用交易日期", SZSE="日期")
    ak_fetch_func: akshare 数据获取函数
    dates: 要更新的交易日列表
    """
    # 读取已有文件
    if os.path.exists(file_path):
        existing_df = pd.read_csv(file_path, encoding="utf-8-sig", dtype=str)
    else:
        existing_df = pd.DataFrame()

    existing_dates = (
        set(existing_df[date_col].astype(str).unique())
        if not existing_df.empty
        else set()
    )

    added_rows = 0
    skipped_dates = []
    updated_dates = []
    new_dfs = []

    for date_str in dates:
        if date_str in existing_dates:
            print(f"[{market}] {date_str} 已存在，跳过")
            skipped_dates.append(date_str)
            continue

        try:
            print(f"[{market}] 获取 {date_str} 数据中...")
            df = ak_fetch_func(date=date_str)
            if not df.empty:
                df[date_col] = date_str
                new_dfs.append(df)
                updated_dates.append(date_str)
        except Exception as e:
            print(f"[{market}] {date_str} 拉取失败: {e}")

    if new_dfs:
        new_data = pd.concat(new_dfs, ignore_index=True)
        final_df = (
            pd.concat([existing_df, new_data], ignore_index=True)
            if not existing_df.empty
            else new_data
        )
        final_df.to_csv(file_path, index=False, encoding="utf-8-sig")
        added_rows = len(new_data)
        print(f"[{market}] 已更新 {added_rows} 行数据")
    else:
        print(f"[{market}] 无需更新")

    return {
        "added_rows": added_rows,
        "updated_dates": updated_dates,
        "skipped_dates": skipped_dates,
    }


def update_margin_data_api():
    """
    POST JSON:
    {
        "days": 30  # 最近 N 个交易日（必填）
    }
    """
    data = request.get_json(silent=True) or {}
    try:
        days = int(data.get("days", 30))
        if days <= 0:
            raise ValueError
    except Exception:
        return jsonify({"code": 1, "message": "参数 days 错误，应为正整数"}), 400

    try:
        dates = get_recent_trade_dates(days)  # 升序交易日列表
    except Exception as e:
        return jsonify({"code": 1, "message": f"获取交易日失败: {e}"}), 500

    sse_result = update_market_file(
        "SSE", MARGIN_FILE_SSE, "信用交易日期", ak.stock_margin_detail_sse, dates
    )
    szse_result = update_market_file(
        "SZSE", MARGIN_FILE_SZSE, "日期", ak.stock_margin_detail_szse, dates
    )

    return (
        jsonify(
            {
                "code": 0,
                "message": "更新完成",
                "data": {"SSE": sse_result, "SZSE": szse_result},
            }
        ),
        200,
    )


def query_margin_data_by_code_api():
    """
    查询单只股票的融资融券数据（从本地文件中读取）。
    优先从上交所文件查找，再查深交所文件，合并结果返回。
    日期字段统一为 date，按日期升序排序。
    """
    data = request.get_json()
    code = data.get("code", "").strip()

    if not code:
        return jsonify({"code": 1, "message": "缺少股票代码参数", "data": []}), 400

    result = []

    # 查 SSE
    if os.path.exists(MARGIN_FILE_SSE):
        try:
            df_sse = pd.read_csv(MARGIN_FILE_SSE, encoding="utf-8-sig")
            if "标的证券代码" in df_sse.columns:
                # 确保code是字符串且去空格
                code = str(code).strip().zfill(6)
                codes_series = (
                    df_sse["标的证券代码"]
                    .fillna("")
                    .astype(str)
                    .str.strip()
                    .str.zfill(6)
                )
                filtered = df_sse[codes_series == code]
                if not filtered.empty:
                    # 统一日期字段
                    filtered.rename(columns={"信用交易日期": "date"}, inplace=True)
                    filtered.sort_values("date", inplace=True)
                    result.append(
                        {"exchange": "SSE", "data": filtered.to_dict(orient="records")}
                    )
        except Exception as e:
            print(f"[query_margin_data] 读取 SSE 文件异常: {e}")

    # 查 SZSE
    if os.path.exists(MARGIN_FILE_SZSE):
        try:
            df_szse = pd.read_csv(MARGIN_FILE_SZSE, encoding="utf-8-sig")
            if "证券代码" in df_szse.columns:
                # 确保code是字符串且去空格
                code = str(code).strip().zfill(6)

                # 处理证券代码列：填充缺失，转字符串，去空格，补齐6位
                codes_series = (
                    df_szse["证券代码"].fillna("").astype(str).str.strip().str.zfill(6)
                )

                # 过滤匹配行
                filtered = df_szse[codes_series == code]
                if not filtered.empty:
                    filtered.rename(columns={"日期": "date"}, inplace=True)
                    filtered.sort_values("date", inplace=True)
                    result.append(
                        {"exchange": "SZSE", "data": filtered.to_dict(orient="records")}
                    )
        except Exception as e:
            print(f"[query_margin_data] 读取 SZSE 文件异常: {e}")

    if not result:
        return jsonify(
            {"code": 1, "message": f"股票代码 {code} 未查询到融资融券数据", "data": []}
        )

    return jsonify({"code": 0, "message": "查询成功", "data": result})


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
            # 简单进度日志
            print(
                f"[{idx - start_index + 1}/{end_index - start_index}] 正在处理 {code} ...",
                flush=True,
            )

            try:
                try:
                    df = pd.read_csv(path)
                    if df.empty:
                        continue  # 有表头但无数据
                except EmptyDataError:
                    continue  # 文件完全空
                df["日期"] = pd.to_datetime(df["日期"])
                df = df[df["日期"] >= cutoff_date]
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


if __name__ == "__main__":
    print(f"开始访问：{get_recent_trade_dates()}")

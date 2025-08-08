from flask import request, jsonify
from pandas.tseries.offsets import DateOffset
import pandas as pd
import os
from datetime import datetime, timedelta
import akshare as ak

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
HISTORY_CACHE_DIR = os.path.join(BASE_DIR, "history_cache")
LIST_CSV_PATH = os.path.join(BASE_DIR, "stocks_info", "list.csv")
stock_list_df = pd.read_csv(LIST_CSV_PATH, dtype=str)
code_name_map = dict(zip(stock_list_df["code"], stock_list_df["name"]))


def is_fund_inflow_continuous(code: str) -> dict:
    """
    判断是否资金持续流入（近3日）
    - 主力净流入额连续 3 天为正
    - 融资余额近 3 日持续增长（首尾比较）

    返回：
        {
            "股票代码": code,
            "主力连续净流入天数": 3,
            "主力累计净流入": 1234.56,
            "融资余额累计变化": 2.35,
            "是否资金持续流入": "是/否"
        }
    """
    try:
        # 主力资金净流入
        fund_df = ak.stock_individual_fund_flow(stock=code)
        fund_df["日期"] = pd.to_datetime(fund_df["日期"])
        recent_fund = fund_df.sort_values("日期").tail(3)

        net_inflows = recent_fund["主力净流入额"].astype(float).tolist()
        fund_positive_days = sum(1 for val in net_inflows if val > 0)
        fund_total_inflow = sum(net_inflows)

        # 融资余额
        margin_df = ak.stock_margin_detail(symbol=code)
        margin_df["日期"] = pd.to_datetime(margin_df["日期"])
        recent_margin = margin_df.sort_values("日期").tail(3)

        margin_balances = recent_margin["融资余额"].astype(float).tolist()
        if len(margin_balances) < 2 or margin_balances[0] == 0:
            margin_pct_change = 0.0
        else:
            margin_pct_change = (
                (margin_balances[-1] - margin_balances[0]) / margin_balances[0] * 100
            )

        is_continuous = (
            "是" if fund_positive_days == 3 and margin_pct_change > 0 else "否"
        )

        return {
            "股票代码": code,
            "主力连续净流入天数": fund_positive_days,
            "主力累计净流入": round(fund_total_inflow, 2),
            "融资余额累计变化": round(margin_pct_change, 2),
            "是否资金持续流入": is_continuous,
        }

    except Exception as e:
        return {"股票代码": code, "错误": str(e), "是否资金持续流入": "未知"}

def is_fund_inflow_continuous_api():
    try:
        data = request.get_json() or {}
        code = data.get("code") or request.args.get("code")

        if not code:
            return jsonify({"code": 1, "message": "缺少参数 code"}), 400

        result = is_fund_inflow_continuous(code)
        return jsonify({"code": 0, "message": "成功", "data": result})

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
        if os.path.exists(save_path):
            existing_df = pd.read_csv(save_path, dtype=str)
        else:
            existing_df = pd.DataFrame(
                columns=[
                    "股票代码",
                    "股票名称",
                    "当前价",
                    "阶段最低",
                    "阶段最高",
                    "涨跌幅（%）",
                ]
            )

        new_df = pd.DataFrame(results)

        # 合并去重，优先保留已有数据
        combined_df = pd.concat([existing_df, new_df])
        combined_df.drop_duplicates(subset=["股票代码"], keep="first", inplace=True)

        combined_df.to_csv(save_path, index=False, encoding="utf-8-sig")

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

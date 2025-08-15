import os
from flask import jsonify, request
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
import time
import time
import json
import threading

# 全局停止标志
stop_flag = False
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
LIST_CSV_PATH = os.path.join(BASE_DIR, "stocks_info", "list.csv")
HISTORY_CACHE_DIR = os.path.join(BASE_DIR, "history_cache")
TASK_STATUS_PATH = os.path.join(BASE_DIR, "stocks_info", "task_status.json")


def get_stock_list_cached():
    """
    尝试从缓存文件读取股票列表，如果不存在或失败则调用 AKShare 更新并写入缓存
    """
    if os.path.exists(LIST_CSV_PATH):
        try:
            df = pd.read_csv(LIST_CSV_PATH, dtype={"code": str})
            if not df.empty:
                return df
        except Exception as e:
            print("读取缓存失败：", e)

    # 获取数据并缓存
    df = ak.stock_info_a_code_name()
    df["code"] = df["code"].astype(str).str.zfill(6)
    os.makedirs(os.path.dirname(LIST_CSV_PATH), exist_ok=True)
    df.to_csv(LIST_CSV_PATH, index=False)
    return df


def stock_count_api():
    """返回股票总数"""
    try:
        df = get_stock_list_cached()
        return jsonify(
            {"stock_count": len(df), "message": "成功读取股票列表", "code": 200}
        )
    except Exception as e:
        return jsonify({"error": str(e), "message": "读取失败"}), 500


def stock_list_api():
    """返回股票列表"""
    try:
        df = get_stock_list_cached()
        stocks = df[["code", "name"]].to_dict(orient="records")
        return jsonify(
            {
                "data": stocks,
                "count": len(stocks),
                "message": "成功获取股票列表",
                "code": 200,
            }
        )
    except Exception as e:
        return jsonify({"error": str(e), "message": "读取股票列表失败"}), 500


def update_single_stock_api():
    """单只股票更新 API"""
    if request.method == "GET":
        code = request.args.get("code")
    else:
        code = request.json.get("code") if request.is_json else request.form.get("code")

    if not code:
        return jsonify({"error": "缺少参数: code"}), 400

    result = update_single_stock(code)
    return jsonify(result)


def update_single_stock(code):
    """单只股票更新"""
    global stop_flag
    if stop_flag:
        return {"code": -1, "message": f"[停止] {code} 更新中断", "updated_count": -1}

    today = datetime.today()
    end_date_str = today.strftime("%Y%m%d")

    try:
        cache_path = os.path.join(HISTORY_CACHE_DIR, f"{code}.csv")

        if os.path.exists(cache_path):
            df_old = pd.read_csv(cache_path, parse_dates=["日期"])
            last_date = df_old["日期"].max()
            start_date = last_date + timedelta(days=1)
            start_date_str = start_date.strftime("%Y%m%d")

            if start_date_str > end_date_str:
                return {
                    "code": 0,
                    "message": f"[无需更新] {code} 已是最新",
                    "updated_count": 0,
                }

            if stop_flag:
                return {
                    "code": -1,
                    "message": f"[停止] {code} 更新中断",
                    "updated_count": -1,
                }

            df_new = ak.stock_zh_a_hist(
                symbol=str(code),
                period="daily",
                start_date=start_date_str,
                end_date=end_date_str,
                adjust="",
            )
            if df_new.empty:
                return {"code": 0, "message": f"[无新数据] {code}", "updated_count": 0}

            df_new["日期"] = pd.to_datetime(df_new["日期"])
            df_combined = pd.concat([df_old, df_new], ignore_index=True)
            df_combined.drop_duplicates(subset=["日期"], inplace=True)
            df_combined.sort_values("日期", inplace=True)
            df_combined.to_csv(cache_path, index=False)

            return {
                "code": 0,
                "message": f"[更新成功] {code} 新增 {len(df_new)} 条记录",
                "updated_count": len(df_new),
            }

        else:
            if stop_flag:
                return {
                    "code": -1,
                    "message": f"[停止] {code} 更新中断",
                    "updated_count": -1,
                }
            df = ak.stock_zh_a_hist(
                symbol=str(code),
                period="daily",
                start_date="19800101",
                end_date=end_date_str,
                adjust="",
            )

            if df.empty:
                return {"code": 0, "message": f"[无数据] {code}", "updated_count": 0}

            df["日期"] = pd.to_datetime(df["日期"])
            df.sort_values("日期", inplace=True)
            df.to_csv(cache_path, index=False)

            return {
                "code": 0,
                "message": f"[首次保存] {code} 共 {len(df)} 条记录",
                "updated_count": len(df),
            }

    except Exception as e:
        return {"code": -1, "message": f"[更新失败] {code}: {e}", "updated_count": -1}


def update_stocks_batch(codes, max_workers=8, batch_size=50):
    global stop_flag
    total_updated = 0

    for i in range(0, len(codes), batch_size):
        if stop_flag:
            print("[批量] 检测到停止信号，结束任务")
            break

        batch_codes = codes[i : i + batch_size]
        print(
            f"[批次开始] 处理股票代码索引 {i + 1} - {i + len(batch_codes)} / {len(codes)}"
        )
        start_batch_time = time.time()

        try:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(update_single_stock, code): code
                    for code in batch_codes
                }

                for future in as_completed(futures):
                    if stop_flag:
                        executor.shutdown(cancel_futures=True)
                        print("[线程池] 停止信号，取消剩余任务")
                        return total_updated

                    code = futures[future]
                    try:
                        # 设定单任务最大等待时间，避免长时间卡住
                        result = future.result(timeout=60)
                        if result.get("updated_count", -1) > 0:
                            total_updated += result["updated_count"]
                        print(f"[更新完成] {code} -> {result.get('message')}")
                    except TimeoutError:
                        print(f"❌ {code} 更新超时，跳过该任务")
                    except Exception as e:
                        print(f"❌ {code} 异常: {e}")

            end_batch_time = time.time()
            print(
                f"[批次结束] 完成索引 {i + len(batch_codes)} / {len(codes)}，耗时 {end_batch_time - start_batch_time:.2f} 秒"
            )
        except Exception as e:
            print(f"❌ 批次异常: {e}")

        # 批次间休息1秒，避免请求过于密集
        time.sleep(1)

    print(f"[批量更新完成] 总共更新记录数: {total_updated}")
    return total_updated


def save_task_status(status):
    os.makedirs(os.path.dirname(TASK_STATUS_PATH), exist_ok=True)
    with open(TASK_STATUS_PATH, "w", encoding="utf-8") as f:
        json.dump(status, f, ensure_ascii=False, indent=2)


def load_task_status():
    if os.path.exists(TASK_STATUS_PATH):
        with open(TASK_STATUS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def async_all_stock_start_api():
    global stop_flag
    stop_flag = False

    status = load_task_status()
    if status.get("running", False):
        return jsonify({"code": 1, "message": "任务已在运行中"}), 400

    try:
        df = get_stock_list_cached()
        codes = df["code"].tolist()
    except Exception as e:
        return jsonify({"code": -1, "message": f"读取股票列表失败: {e}"}), 500

    def background_task():
        try:
            save_task_status(
                {"running": True, "progress": 0, "total": len(codes), "updated": 0}
            )
            updated_total = 0
            batch_size = 50
            max_workers = 8

            for i in range(0, len(codes), batch_size):
                if stop_flag:
                    save_task_status(
                        {
                            "running": False,
                            "progress": i,
                            "total": len(codes),
                            "updated": updated_total,
                            "message": "任务已手动停止",
                        }
                    )
                    print("[后台任务] 停止信号，任务终止")
                    return

                batch_codes = codes[i : i + batch_size]
                print(
                    f"[后台任务] 处理批次: {i + 1} - {i + len(batch_codes)} / {len(codes)}"
                )
                updated_count = update_stocks_batch(
                    batch_codes, max_workers=max_workers, batch_size=batch_size
                )
                updated_total += updated_count

                progress = min(i + batch_size, len(codes))
                save_task_status(
                    {
                        "running": True,
                        "progress": progress,
                        "total": len(codes),
                        "updated": updated_total,
                        "message": f"已处理 {progress} / {len(codes)}",
                    }
                )
                print(
                    f"[后台任务] 已处理 {progress} / {len(codes)}，累计更新 {updated_total} 条记录"
                )

            save_task_status(
                {
                    "running": False,
                    "progress": len(codes),
                    "total": len(codes),
                    "updated": updated_total,
                    "message": "任务完成",
                }
            )
            print("[后台任务] 全量同步任务完成")
        except Exception as e:
            print(f"❌ 后台任务异常: {e}")
            save_task_status(
                {
                    "running": False,
                    "progress": 0,
                    "total": len(codes),
                    "updated": 0,
                    "message": f"任务异常中断: {e}",
                }
            )

    threading.Thread(target=background_task, daemon=True).start()
    return jsonify({"code": 0, "message": "任务已启动"})


def check_async_all_status_api():
    """检查任务状态"""
    if not os.path.exists(TASK_STATUS_PATH):
        return jsonify({"code": 1, "message": "无同步任务状态", "running": False})

    status = load_task_status()
    return jsonify({"code": 0, "message": "成功获取任务状态", **status})


def all_stock_async_stop_api():
    """停止任务"""
    global stop_flag
    stop_flag = True

    status = load_task_status()
    if status.get("running", False):
        save_task_status({**status, "running": False, "message": "任务已手动停止"})
        return jsonify({"code": 0, "message": "停止请求已发送"})
    else:
        return jsonify({"code": 1, "message": "当前没有运行的任务"}), 400



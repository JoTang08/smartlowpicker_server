from flask import Blueprint, render_template

from app.routes.stocks_info import (
    stock_count_api,
    stock_list_api,
    update_single_stock_api,
    async_all_stock_start_api,
    all_stock_async_stop_api,
    check_async_all_status_api,
)

from app.routes.stocks_analyse import (
    analyze_batch_api,
    get_history_cache_count_api,
    get_low_price_stocks_api,
    list_low_price_stock_files_api,
    is_fund_inflow_continuous_api,
)

main = Blueprint("main", __name__)


@main.route("/")
def index():
    return render_template("index.html")


@main.route("/stocks/count", methods=["GET"])
def stocks_count_handler():
    return stock_count_api()


@main.route("/stocks/list", methods=["GET"])
def stocks_list_handler():
    return stock_list_api()


# @code: 代码
@main.route("/stocks/asyncCode", methods=["POST", "GET"])
def stocks_data_async():
    return update_single_stock_api()


# 开启同步任务
@main.route("/sync/all-start", methods=["POST", "GET"])
def async_all_stock_start():
    return async_all_stock_start_api()


# 查询同步任务进度接口
@main.route("/sync/all-status", methods=["POST", "GET"])
def check_async_all_status():
    return check_async_all_status_api()


# 停止更新
@main.route("/sync/all-stop", methods=["POST", "GET"])
def all_stock_async_stop():
    return all_stock_async_stop_api()


@main.route("/history_cache_count", methods=["POST", "GET"])
def get_history_cache_count():
    return get_history_cache_count_api()


@main.route("/low-price-stocks", methods=["POST", "GET"])
def get_low_price_stocks():
    return get_low_price_stocks_api()


@main.route("/analyze-batch", methods=["POST"])
def analyze_batch():
    return analyze_batch_api()


@main.route("/list_low_price_stock_files", methods=["GET"])
def list_low_price_stock_files():
    return list_low_price_stock_files_api()


@main.route("/is_fund_inflow_continuous", methods=["POST"])
def is_fund_inflow_continuous():
    return is_fund_inflow_continuous_api()

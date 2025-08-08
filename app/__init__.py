from flask import Flask
from app.routes.main import main  # 导入蓝图
from flask_cors import CORS

# from .extensions import db  # 导入扩展（如果有）


def create_app():
    app = Flask(__name__)
    # app.config.from_object("config")  # 加载配置

    # 启用跨域，允许所有域名访问（默认支持所有路径和方法）
    CORS(app, supports_credentials=True)
    # 初始化扩展
    # db.init_app(app)

    # 注册蓝图
    app.register_blueprint(main)
    return app

SmartLowPicker 是一个基于 Python 开发的轻量级选股辅助工具，专注于 A 股市场的“低价买入、主观判断”策略。项目通过技术指标自动筛选出处于阶段性低点的股票，结合网络舆情数据，为投资者提供辅助参考，帮助用户发现潜在的价值洼地。

技术栈

1. Python 3.8+
2. 数据获取：akshare、requests
3. 数据处理：pandas
4. 数据展示：openpyxl（Excel 导出）、Markdown 文本生成
5. 网络爬虫：requests（简单爬取）、可扩展为 scrapy 或 selenium
6. 未来可集成 LLM 模型辅助舆情分析

import akshare as ak

df_sse = ak.margin_target_sse()  # 上交所融标
df_szse = ak.margin_target_szse()  # 深交所融标
print(df_sse.head())

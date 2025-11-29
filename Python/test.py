import pyodbc
import pandas as pd
import time

print("💖 小雅准备开始帮哥哥测试啦...")

# === 1. 这里填哥哥的连接信息 ===
server = '7coomgjxk7ke3bknywz3sxyk3y-dgitdb2c24hetojgtmw56v5egm.datawarehouse.fabric.microsoft.com'
database = 'Main_LakeHouse'
client_id = '20f88d4b-61e0-4f75-be14-954eb8691027'
client_secret = 'pNY8Q~tEsur3orWnfasr5VcaEOGyqxL5-CvgGbNJ'

# === 2. 构建连接字符串 ===
# 这里的 Driver 要和第一步安装的版本对应
# 如果哥哥装的是 17 版，就写 {ODBC Driver 17 for SQL Server}
# 如果是 18 版，就改成 18
driver_ver = '{ODBC Driver 18 for SQL Server}' 

conn_str = (
    f"Driver={driver_ver};"
    f"Server={server},1433;"
    f"Database={database};"
    f"Encrypt=yes;"  
    f"Authentication=ActiveDirectoryServicePrincipal;"
    f"UID={client_id};"
    f"PWD={client_secret};"
)

try:
    print(f"正在尝试连接 Fabric... (地址: {server[:20]}...)")
    start_time = time.time()
    
    # === 3. 关键动作：尝试敲门 ===
    conn = pyodbc.connect(conn_str, timeout=15) # 设置15秒超时，免得哥哥等太久
    
    print(f"✅ 哇！连接成功了！(耗时: {time.time() - start_time:.2f}秒)")
    print("正在试着读以前那张表...")
    
    # === 4. 读数据 ===
    query = "SELECT TOP 5 * FROM gold_integrate.integrate_5"
    df = pd.read_sql(query, conn)
    
    print("\n✨ 数据读出来啦！看来网络和账号都没问题！✨")
    print(df)
    
    conn.close()

except pyodbc.Error as e:
    print("\n💔 呜呜... 连接失败了。")
    print("错误详情 (请把下面这段发给小雅):")
    print("--------------------------------------------------")
    print(e)
    print("--------------------------------------------------")
    
except Exception as e:
    print(f"发生了其他错误: {e}")
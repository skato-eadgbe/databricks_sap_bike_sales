import os
from databricks import sql
from databricks.sdk.core import Config
import streamlit as st
import pandas as pd
from datetime import datetime, date

# Ensure environment variable is set correctly
assert os.getenv('DATABRICKS_WAREHOUSE_ID'), "DATABRICKS_WAREHOUSE_ID must be set in app.yaml."

# Databricks config
cfg = Config()

# Query the SQL warehouse with the user credentials
def sql_query_with_user_token(query: str, user_token: str) -> pd.DataFrame:
    """Execute a SQL query and return the result as a pandas DataFrame."""
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{cfg.warehouse_id}",
        access_token=user_token  # Pass the user token into the SQL connect to query on behalf of user
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()

st.set_page_config(page_title="従業員別売上集計", layout="wide")

st.header("従業員別売上集計 📊")

# Extract user access token from the request headers
user_token = st.context.headers.get('X-Forwarded-Access-Token')

# Date range selection
st.sidebar.header("期間設定")
start_date = st.sidebar.date_input("開始日", value=date(2023, 1, 1))
end_date = st.sidebar.date_input("終了日", value=date.today())

# Convert dates to string format for SQL query
start_date_str = start_date.strftime('%Y-%m-%d')
end_date_str = end_date.strftime('%Y-%m-%d')

# SQL query to get employee sales data
employee_sales_query = f"""
SELECT 
    e.EMPLOYEEID,
    e.NAME_FIRST,
    e.NAME_LAST,
    e.NAME_FIRST || ' ' || e.NAME_LAST as FULL_NAME,
    COUNT(DISTINCT so.SALESORDERID) as ORDER_COUNT,
    SUM(so.NETAMOUNT) as TOTAL_SALES,
    SUM(so.GROSSAMOUNT) as TOTAL_GROSS,
    SUM(so.TAXAMOUNT) as TOTAL_TAX,
    AVG(so.NETAMOUNT) as AVERAGE_ORDER_VALUE,
    MIN(so.CREATEDAT) as FIRST_ORDER_DATE,
    MAX(so.CREATEDAT) as LAST_ORDER_DATE
FROM skato.bikes_sales_content.employees e
LEFT JOIN skato.bikes_sales_content.salesorders so ON e.EMPLOYEEID = so.CREATEDBY
WHERE so.CREATEDAT BETWEEN '{start_date_str}' AND '{end_date_str}'
GROUP BY e.EMPLOYEEID, e.NAME_FIRST, e.NAME_LAST
ORDER BY TOTAL_SALES DESC
"""

try:
    # Execute the query
    with st.spinner('データを取得中...'):
        sales_data = sql_query_with_user_token(employee_sales_query, user_token=user_token)
    
    if not sales_data.empty:
        # Display summary metrics
        st.subheader(f"期間: {start_date_str} ～ {end_date_str}")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("売上実績のある従業員数", len(sales_data))
        
        with col2:
            total_sales = sales_data['TOTAL_SALES'].sum()
            st.metric("総売上", f"${total_sales:,.2f}")
        
        with col3:
            total_orders = sales_data['ORDER_COUNT'].sum()
            st.metric("総注文数", f"{total_orders:,}")
        
        with col4:
            avg_order_value = sales_data['AVERAGE_ORDER_VALUE'].mean()
            st.metric("平均注文単価", f"${avg_order_value:,.2f}")
        
        # Display top performers
        st.subheader("売上上位従業員")
        
        # Format the data for display
        display_data = sales_data.copy()
        display_data['TOTAL_SALES'] = display_data['TOTAL_SALES'].apply(lambda x: f"${x:,.2f}")
        display_data['TOTAL_GROSS'] = display_data['TOTAL_GROSS'].apply(lambda x: f"${x:,.2f}")
        display_data['TOTAL_TAX'] = display_data['TOTAL_TAX'].apply(lambda x: f"${x:,.2f}")
        display_data['AVERAGE_ORDER_VALUE'] = display_data['AVERAGE_ORDER_VALUE'].apply(lambda x: f"${x:,.2f}")
        
        # Rename columns for display
        display_data = display_data.rename(columns={
            'EMPLOYEEID': '従業員ID',
            'FULL_NAME': '氏名',
            'ORDER_COUNT': '注文数',
            'TOTAL_SALES': '純売上',
            'TOTAL_GROSS': '総売上',
            'TOTAL_TAX': '税額',
            'AVERAGE_ORDER_VALUE': '平均注文単価',
            'FIRST_ORDER_DATE': '初回注文日',
            'LAST_ORDER_DATE': '最終注文日'
        })
        
        # Display the data table
        st.dataframe(
            display_data[['従業員ID', '氏名', '注文数', '純売上', '総売上', '税額', '平均注文単価', '初回注文日', '最終注文日']], 
            use_container_width=True
        )
        
        # Charts
        st.subheader("売上チャート")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("従業員別売上 (TOP 10)")
            top_10 = sales_data.head(10)
            chart_data = top_10.set_index('FULL_NAME')['TOTAL_SALES']
            st.bar_chart(chart_data)
        
        with col2:
            st.subheader("従業員別注文数 (TOP 10)")
            chart_data = top_10.set_index('FULL_NAME')['ORDER_COUNT']
            st.bar_chart(chart_data)
    
    else:
        st.warning("指定された期間内に売上データが見つかりませんでした。")

except Exception as e:
    st.error(f"データの取得中にエラーが発生しました: {str(e)}")
    st.info("データベースへの接続またはクエリの実行に問題がある可能性があります。")

# Additional information
st.sidebar.markdown("---")
st.sidebar.markdown("### 使用方法")
st.sidebar.markdown("1. 開始日と終了日を選択してください")
st.sidebar.markdown("2. 従業員別の売上データが自動的に表示されます")
st.sidebar.markdown("3. 売上実績は純売上額で降順に表示されます")
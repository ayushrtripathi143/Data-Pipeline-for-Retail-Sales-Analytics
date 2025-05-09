# Import python packages
import streamlit as st
import pandas as pd
import altair as alt


st.set_page_config(
    page_title="Dashboard",
    layout="wide",  
    initial_sidebar_state="expanded"  
)
st.logo(image="https://streamlit.io/images/brand/streamlit-logo-primary-colormark-darktext.png", size="large")

st.sidebar.markdown("""
## Sales Analytics App  
Analyze your sales data with interactive charts and KPIs.

- 📊 Filter by region, product, or date  
- 📈 View trends, top performers & insights  

Make smarter business decisions, faster.
""")
columns = [
    "ORDER_ID", "ORDER_DATE", "MONTH_ORDERED", "YYYY_ORDERED", "DAY_ORDER", "DAY_OF_WEEK",
    "CUSTOMER_NAME", "EMAIL", "PHONE", "ADDRESS", "CUST_AREA_CODE", "STATE_NAME",
    "LONGITUDE", "LATITUDE", "PRODUCT_NAME", "CATEGORY", "PRICE", "QUANTITY",
    "STORE_NAME", "STORE_LOCATION", "EMPLOYEE_NAME", "EMPLOYEE_TITLE", "ORDER_ITEM_TOTAL"
]

# Snowflake connection
conn = st.connection('snowflake')
cur = conn.cursor()
qry = '''SELECT * FROM retail_target.v_sales_analytics;'''
data = cur.execute(qry)

# DataFrame creation
df = pd.DataFrame(data, columns=columns)

# Title
st.title("Sales Analytics Dashboard")

# Main KPIs
with st.container(border=True):
    col1, col2, col3 = st.columns(3)
    total_sales = df['ORDER_ITEM_TOTAL'].sum()
    avg_order = df['ORDER_ITEM_TOTAL'].mean()
    total_orders = df['ORDER_ID'].nunique()

    col1.metric("Total Revenue", f"${total_sales:,.2f}")
    col2.metric("Avg Order Value", f"${avg_order:,.2f}")
    col3.metric("Unique Orders", total_orders)

# Trend & Category Charts
with st.container(border=True):
    st.subheader("Trends & Patterns")
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Monthly Sales**")
        monthly = df.groupby('MONTH_ORDERED')['ORDER_ITEM_TOTAL'].sum().reset_index()
        st.bar_chart(monthly, x='MONTH_ORDERED', y='ORDER_ITEM_TOTAL', y_label="Order Item Total", x_label="Month Ordered")

        st.markdown("**Orders by Day**")
        by_day = df.groupby('DAY_OF_WEEK')['ORDER_ID'].count().reset_index()
        st.bar_chart(by_day, x='DAY_OF_WEEK', y='ORDER_ID', x_label="Day of Week", y_label="Order Count")

    with col2:
        st.markdown("**Sales by State**")
        state_sales = df.groupby('STATE_NAME')['ORDER_ITEM_TOTAL'].sum().reset_index()
        st.bar_chart(state_sales, x='STATE_NAME', y='ORDER_ITEM_TOTAL', x_label="State Name", y_label="Order Count")

        st.markdown("**Top 10 Products**")
        top_products = df.groupby('PRODUCT_NAME')['ORDER_ITEM_TOTAL'].sum().nlargest(10).reset_index()
        st.bar_chart(top_products, x='PRODUCT_NAME', y='ORDER_ITEM_TOTAL',x_label="Product Name",y_label="Total Sales")

# Frequent Buyers Section
with st.container(border=True):
    st.subheader("Top 10 Loyal Customers")
    top_customers = df.groupby('CUSTOMER_NAME')['ORDER_ID'].count().nlargest(10).reset_index()
    st.bar_chart(top_customers, x='CUSTOMER_NAME', y='ORDER_ID',x_label="Customer Name",y_label="Order Counts")


















# Derived from: MONTH_ORDERED, PRICE, QUANTITY









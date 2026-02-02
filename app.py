import streamlit as st
import pandas as pd
import plotly.express as px
from databricks import sql
import datetime

# --- PAGE CONFIG ---
st.set_page_config(page_title="Databricks Data Explorer", layout="wide")
st.title("📊 Databricks Data Explorer")

# --- DATABASE CONNECTION ---
@st.cache_resource
def get_db_connection():
    # Credentials are pulled from st.secrets (defined later)
    return sql.connect(
        server_hostname=st.secrets["databricks"]["server_hostname"],
        http_path=st.secrets["databricks"]["http_path"],
        access_token=st.secrets["databricks"]["access_token"]
    )

# --- SIDEBAR INPUTS ---
st.sidebar.header("User Inputs")

# 1. File Upload
uploaded_file = st.sidebar.file_uploader("Upload your CSV", type="csv")

# 2. Dropdown Menus
category = st.sidebar.selectbox("Select Category", ["Sales", "Inventory", "Marketing"])
status = st.sidebar.selectbox("Status", ["Active", "Pending", "Closed"])

# 3. Date Picker
selected_date = st.sidebar.date_input("Select Date", datetime.date.today())

# --- MAIN LOGIC ---
if uploaded_file is not None:
    # Process Uploaded CSV
    user_df = pd.read_csv(uploaded_file)
    st.subheader("Uploaded Data Preview")
    st.dataframe(user_df.head())

    # Trigger Databricks Query
    if st.sidebar.button("Fetch Databricks Data"):
        try:
            conn = get_db_connection()
            with conn.cursor() as cursor:
                # Use parameterized query for safety
                query = f"SELECT * FROM your_catalog.your_schema.your_table LIMIT 100"
                cursor.execute(query)
                result = cursor.fetchall_arrow().to_pandas()
            
            st.success("Data fetched successfully!")

            # --- DISPLAY RESULTS ---
            col1, col2 = st.columns(2)

            with col1:
                st.subheader("Results Table")
                st.dataframe(result)

            with col2:
                st.subheader("Visualizations")
                # Example Chart 1: Bar Chart
                fig_bar = px.bar(result, x=result.columns[0], y=result.columns[1], title="Data Breakdown")
                st.plotly_chart(fig_bar, use_container_width=True)

                # Example Chart 2: Line Chart
                if len(result.columns) > 2:
                    fig_line = px.line(result, y=result.columns[2], title="Trend Over Time")
                    st.plotly_chart(fig_line, use_container_width=True)

        except Exception as e:
            st.error(f"Error connecting to Databricks: {e}")
else:
    st.info("Please upload a CSV file to get started.")

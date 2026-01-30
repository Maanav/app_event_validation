import streamlit as st
import pandas as pd
from databricks import sql
import plotly.express as px
import time

# ------------------------------------------------------------------
# 1. SECURITY CONFIGURATION
# ------------------------------------------------------------------
st.set_page_config(page_title="Internal Data Tool", page_icon="🔒", layout="wide")

# Hides Streamlit branding (Menu, Footer) for a cleaner look
st.markdown("""
    <style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    </style>
    """, unsafe_allow_html=True)

# --- PASSWORD PROTECTION FUNCTION ---
def check_password():
    """Returns `True` if the user had the correct password."""

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        # if st.session_state["password"] == st.secrets["APP_PASSWORD"]:
        if st.session_state["password"] == 1729:
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # Don't store password
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        # First run, show input for password.
        st.text_input(
            "Enter Company Access Key", type="password", on_change=password_entered, key="password"
        )
        return False
    elif not st.session_state["password_correct"]:
        # Password incorrect, show input again.
        st.text_input(
            "Incorrect password, recheck and enter again", type="password", on_change=password_entered, key="password"
        )
        st.error("😕 Password incorrect")
        return False
    else:
        # Password correct.
        return True

# --- STOP EXECUTION IF NOT LOGGED IN ---
if not check_password():
    st.stop()  # Do not run any code below this line if password fails

# ------------------------------------------------------------------
# 2. MAIN APPLICATION (Only runs after login)
# ------------------------------------------------------------------

# Helper: Connect to Databricks
@st.cache_resource
def get_connection():
    return sql.connect(
        server_hostname=st.secrets["DATABRICKS_HOST"],
        http_path=st.secrets["DATABRICKS_HTTP_PATH"],
        access_token=st.secrets["DATABRICKS_TOKEN"]
    )

# Sidebar
with st.sidebar:
    st.info("🔒 Secure Internal Environment")
    st.title("Validation Settings")
    uploaded_file = st.file_uploader("Upload Batch CSV", type="csv")
    table_name = st.text_input("Target Table", value="catalog.schema.table")

# Main Page
st.title("🏢 Corporate Data Validator")
st.markdown(f"**Status:** Connected to Databricks Warehouse")

if uploaded_file:
    try:
        input_df = pd.read_csv(uploaded_file)
        
        with st.expander("View Uploaded Raw Data"):
            st.dataframe(input_df.head())

        col1, col2 = st.columns([1,3])
        with col1:
            target_col = st.selectbox("Match on Column:", input_df.columns)

        if st.button("Run Compliance Check", type="primary"):
            # Progress bar simulation
            progress = st.progress(0, text="Initializing secure handshake...")
            for i in range(100):
                time.sleep(0.005)
                progress.progress(i + 1)
            
            # --- REAL QUERY LOGIC ---
            # ids = input_df[target_col].unique().tolist()
            # conn = get_connection()
            # formatted_ids = ",".join([f"'{str(x)}'" for x in ids])
            # query = f"SELECT * FROM {table_name} WHERE id_column IN ({formatted_ids})"
            # result_df = pd.read_sql(query, conn)
            
            # --- MOCK DATA (REMOVE THIS BLOCK WHEN LIVE) ---
            import numpy as np
            ids = input_df[target_col].unique().tolist()
            result_df = pd.DataFrame({
                'ID': ids[:10],
                'Status': np.random.choice(['Verified', 'Pending'], 10),
                'Value': np.random.randint(500, 5000, 10)
            })
            # -----------------------------------------------

            progress.empty()

            # KPI Cards
            st.divider()
            kpi1, kpi2, kpi3 = st.columns(3)
            kpi1.metric("Input Rows", len(input_df))
            kpi2.metric("Verified Matches", len(result_df))
            kpi3.metric("Verification Rate", f"{round(len(result_df)/len(input_df)*100)}%")

            # Interactive Tabs
            tab_data, tab_viz = st.tabs(["📄 Detailed Records", "📊 Analytics"])
            
            with tab_data:
                st.dataframe(result_df, use_container_width=True)
                st.download_button("Download Report", result_df.to_csv().encode('utf-8'), "report.csv")

            with tab_viz:
                if not result_df.empty:
                    fig = px.bar(result_df, x=result_df.columns[0], y=result_df.columns[2], title="Distribution")
                    st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"System Error: {e}")

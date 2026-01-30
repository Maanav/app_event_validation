import streamlit as st
import pandas as pd
from databricks import sql
import plotly.express as px
import time
from datetime import datetime, timedelta

# ------------------------------------------------------------------
# 1. SECURITY & CONFIG
# ------------------------------------------------------------------
st.set_page_config(
    page_title="Internal Analytics Tool", 
    page_icon="🔒", 
    layout="wide",
    initial_sidebar_state="expanded"  # Forces sidebar open on load
)

# Standard CSS to just hide the Streamlit menu/footer (Safe)
st.markdown("""
    <style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    </style>
    """, unsafe_allow_html=True)

# --- PASSWORD PROTECTION ---
def check_password():
    # if st.secrets.get("APP_PASSWORD") == "open": return True 
    
    def password_entered():
        # if st.session_state["password"] == st.secrets["APP_PASSWORD"]:
        if st.session_state["password"] ==1729:
            st.session_state["password_correct"] = True
            del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        st.text_input("Enter Company Access Key", type="password", on_change=password_entered, key="password")
        return False
    elif not st.session_state["password_correct"]:
        st.text_input("Enter Company Access Key", type="password", on_change=password_entered, key="password")
        st.error("😕 Password incorrect")
        return False
    else:
        return True

if not check_password():
    st.stop()

# ------------------------------------------------------------------
# 2. HELPER FUNCTIONS
# ------------------------------------------------------------------
@st.cache_resource
def get_connection():
    return sql.connect(
        server_hostname=st.secrets["DATABRICKS_HOST"],
        http_path=st.secrets["DATABRICKS_HTTP_PATH"],
        access_token=st.secrets["DATABRICKS_TOKEN"]
    )

def run_query(table_name, start_date, event_list):
    conn = get_connection()
    formatted_date = start_date.strftime('%Y-%m-%d')
    formatted_events = ",".join([f"'{str(x)}'" for x in event_list])
    
    query = f"""
    SELECT 
        event_date,
        event_name,
        props_feature,
        count(*) as total_count
    FROM {table_name}
    WHERE 
        event_date >= '{formatted_date}' 
        AND event_name IN ({formatted_events})
    GROUP BY 1, 2, 3
    ORDER BY event_date ASC
    """
    
    with conn.cursor() as cursor:
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        
    return pd.DataFrame(data, columns=columns)

# ------------------------------------------------------------------
# 3. SIDEBAR INPUTS (Native Streamlit)
# ------------------------------------------------------------------
with st.sidebar:
    st.header("Search Parameters")
    
    # 1. File Upload
    uploaded_file = st.file_uploader("1. Upload Definition CSV", type="csv", help="Must contain 'event' and 'props_feature'")
    
    # 2. Dropdown
    TABLE_OPTIONS = {
        "User Activity Logs": "catalog.schema.user_activity",
        "System Error Logs": "catalog.schema.system_errors",
        "Transaction Data": "catalog.schema.transactions",
        "Marketing Events": "catalog.schema.marketing_events"
    }
    selected_option = st.selectbox("2. Select Data Source", list(TABLE_OPTIONS.keys()))
    
    # 3. Date Picker
    start_date = st.date_input("3. Select Start Date", datetime.today() - timedelta(days=30))
    
    target_table = TABLE_OPTIONS[selected_option]
    
    st.divider()
    st.caption("v1.4 | Enterprise Edition")

# ------------------------------------------------------------------
# 4. MAIN DASHBOARD
# ------------------------------------------------------------------
st.title(f"📊 {selected_option}")
st.markdown("Use the sidebar (arrow at top-left) to update filters.")

if uploaded_file is not None:
    try:
        input_df = pd.read_csv(uploaded_file)
        
        # Validation
        required_cols = {'event', 'props_feature'}
        if not required_cols.issubset(input_df.columns):
            st.error(f"❌ CSV Error: Missing required columns. Found {list(

import streamlit as st
import pandas as pd
from databricks import sql
import plotly.express as px
import time
from datetime import datetime, timedelta

# ------------------------------------------------------------------
# 1. SECURITY & CONFIG
# ------------------------------------------------------------------
st.set_page_config(page_title="Internal Analytics Tool", page_icon="🔒", layout="wide")

st.markdown("""
    <style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    </style>
    """, unsafe_allow_html=True)

# --- PASSWORD PROTECTION (Kept from previous version) ---
def check_password():
    # if st.secrets["APP_PASSWORD"] == "open": return True # Dev bypass
    
    def password_entered():
        # if st.session_state["password"] == st.secrets["APP_PASSWORD"]:
        if st.session_state["password"] == '1729':
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
    
    # 1. Format inputs for SQL
    formatted_date = start_date.strftime('%Y-%m-%d')
    # Sanitize list for SQL IN clause ('event1', 'event2')
    formatted_events = ",".join([f"'{str(x)}'" for x in event_list])
    
    # 2. Construct Query
    # We assume the table has columns: 'event_date', 'event_name', 'props_feature'
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
# 3. SIDEBAR INPUTS
# ------------------------------------------------------------------
with st.sidebar:
    st.title("Search Parameters")
    
    # A. CSV Upload with Structure Check
    uploaded_file = st.file_uploader("1. Upload Definition CSV", type="csv", help="Must contain 'event' and 'props_feature'")
    
    # B. Dropdown Menu (Map user friendly names to actual table names)
    # Dictionary Key = Display Name, Value = Actual Databricks Table
    TABLE_OPTIONS = {
        "User Activity Logs": "catalog.schema.user_activity",
        "System Error Logs": "catalog.schema.system_errors",
        "Transaction Data": "catalog.schema.transactions",
        "Marketing Events": "catalog.schema.marketing_events",
        "Feature Usage": "catalog.schema.feature_usage"
    }
    selected_option = st.selectbox("2. Select Data Source", list(TABLE_OPTIONS.keys()))
    
    # C. Date Picker
    start_date = st.date_input("3. Select Start Date", datetime.today() - timedelta(days=30))

    # Store variables for access
    target_table = TABLE_OPTIONS[selected_option]

# ------------------------------------------------------------------
# 4. MAIN LOGIC
# ------------------------------------------------------------------
st.title(f"📊 {selected_option} Analysis")

if uploaded_file is not None:
    # --- STEP 1: READ AND VALIDATE CSV ---
    try:
        input_df = pd.read_csv(uploaded_file)
        
        # Check if columns exist
        required_cols = {'event', 'props_feature'}
        if not required_cols.issubset(input_df.columns):
            st.error(f"❌ CSV Error: Missing required columns. Found {list(input_df.columns)}, expected ['event', 'props_feature']")
            st.stop()
            
        with st.expander("✅ File Validated - Click to View Input"):
            st.dataframe(input_df.head())

        # Action Button
        if st.button("Fetch & Visualize Data", type="primary"):
            
            with st.spinner(f"Querying {target_table} from {start_date}..."):
                
                # Extract events for the SQL IN clause
                events_to_filter = input_df['event'].unique().tolist()
                
                # --- [REAL QUERY MODE] Uncomment below line to use real Databricks ---
                # result_df = run_query(target_table, start_date, events_to_filter)
                
                # --- [MOCK DATA MODE] Remove this block when live ---
                import numpy as np
                dates = pd.date_range(start=start_date, periods=14).tolist()
                mock_data = []
                for d in dates:
                    for e in events_to_filter[:5]: # Use actual events from CSV
                        mock_data.append({
                            'event_date': d, 
                            'event_name': e, 
                            'props_feature': np.random.choice(['Mobile', 'Desktop', 'Tablet']),
                            'total_count': np.random.randint(100, 5000)
                        })
                result_df = pd.DataFrame(mock_data)
                # ---------------------------------------------------

            if not result_df.empty:
                # Ensure date is datetime for plotting
                result_df['event_date'] = pd.to_datetime(result_df['event_date'])

                # --- VISUALIZATION SECTION ---
                
                # 1. DoD Line Chart (Aggregated by Date)
                st.subheader("📅 Day-over-Day (DoD) Trend")
                daily_agg = result_df.groupby('event_date')['total_count'].sum().reset_index()
                fig_line = px.line(daily_agg, x='event_date', y='total_count', markers=True, 
                                   title="Total Volume Trend", template="plotly_white")
                st.plotly_chart(fig_line, use_container_width=True)
                
                st.divider()
                
                # 2. Bar Graphs (Side by Side)
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("Distribution by Event")
                    # Group by Event Name
                    event_agg = result_df.groupby('event_name')['total_count'].sum().reset_index().sort_values('total_count', ascending=False)
                    fig_bar1 = px.bar(event_agg, x='event_name', y='total_count', 
                                      color='total_count', title="Top Events by Volume")
                    st.plotly_chart(fig_bar1, use_container_width=True)
                    
                with col2:
                    st.subheader("Distribution by Feature")
                    # Group by Props Feature
                    props_agg = result_df.groupby('props_feature')['total_count'].sum().reset_index()
                    fig_bar2 = px.bar(props_agg, x='props_feature', y='total_count', 
                                      title="Breakdown by Props/Feature")
                    st.plotly_chart(fig_bar2, use_container_width=True)

                # Data Download
                with st.expander("View Raw Data"):
                    st.dataframe(result_df)

            else:
                st.warning("Query returned 0 rows. Check if your CSV events match the database records.")

    except Exception as e:
        st.error(f"Processing Error: {e}")
else:
    st.info("👈 Please upload the definition CSV to begin.")

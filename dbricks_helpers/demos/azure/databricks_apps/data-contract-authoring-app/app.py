# --------------------------------------------------
# IMPORTS
# --------------------------------------------------
import os, time, json, yaml
import pyspark
import streamlit as st
from streamlit_ace import st_ace
from databricks.sdk import WorkspaceClient
from lakebase_helpers import *

# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------
database_instance_name = "data-contract-instance"
postgres_database_name = "data_contracts_database"
database_column_rowid = "id"
database_column_json = "value"

databricks_host = os.getenv("DATABRICKS_HOST")
databricks_client_id = os.getenv("DATABRICKS_CLIENT_ID")
databricks_client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")

# --------------------------------------------------
# CACHED RESOURCES
# --------------------------------------------------

@st.cache_resource
def get_workspace_client():
    """Initialize and cache the Databricks SDK Workspace client."""
    return WorkspaceClient()

@st.cache_resource
def get_oltp_conn(_ws_client_):
    """Initialize and cache the OLTP (Lakebase) PostgreSQL connection."""
    return get_oltp_db_conn(_ws_client_, database_instance_name, postgres_database_name, databricks_client_id)

ws_client = get_workspace_client()
oltp_conn = get_oltp_conn(ws_client)

# --------------------------------------------------
# UTILITY FUNCTIONS
# --------------------------------------------------

def load_value(_oltp_conn_):
    """Fetch the current JSON value from the selected Lakebase table."""
    try:
        cursor = _oltp_conn_.cursor()
        cursor.execute(f"SELECT id, value FROM {st.session_state.database_table_name} LIMIT 1;")
        row = cursor.fetchone()
        return row[0], row[1]
    except Exception as e:
        st.error(f"❌ Error loading JSON from OLTP table: {e}")
        return {}

@st.cache_resource
def json_to_yaml(json_obj):
    """Convert JSON object to YAML string."""
    try:
        return yaml.dump(json_obj, sort_keys=False).strip()
    except Exception as e:
        st.error(f"❌ YAML conversion failed: {e}")
        return ""

# @st.cache_resource
def yaml_to_json(yaml_str):
    """Convert YAML string back to JSON string."""
    try:
        parsed = yaml.safe_load(yaml_str)
        return json.dumps(parsed).strip()
    except Exception as e:
        st.error(f"❌ YAML parsing failed: {e}")
        return None

# --------------------------------------------------
# UI: TABLE SELECTION
# --------------------------------------------------

# Get list of authoring tables
table_names = list_public_tables(oltp_conn)

# Initialize selected table in session state
if "database_table_name" not in st.session_state:
    st.session_state.database_table_name = table_names[0]

# Dropdown to select a table
database_table_name = st.selectbox(
    "Select a Lakebase Data Contract Authoring Table to Edit:",
    table_names,
    index=table_names.index(st.session_state.database_table_name)
    if st.session_state.database_table_name in table_names else 0
)

# Reset session if table changes
if database_table_name != st.session_state.database_table_name:
    st.session_state.pop("yaml_str", None)
    st.session_state.database_table_name = database_table_name
    st.rerun()

# --------------------------------------------------
# UI: EDITOR INTERFACE
# --------------------------------------------------

st.title("Data Contract Authoring Interface")
st.subheader("Edit the YAML below for data contract authoring:")
st.subheader(database_table_name.replace('_', ' ').split('input')[0].strip().upper())

# Load and convert JSON to YAML only once
if "yaml_str" not in st.session_state:
    row_id, json_obj = load_value(oltp_conn)
    st.session_state.row_id = row_id
    st.session_state.yaml_str = json_to_yaml(json_obj)

# YAML Editor with syntax highlighting
edited_yaml = st_ace(
    value=st.session_state.yaml_str,
    language="yaml",
    theme="monokai",
    tab_size=2,
    min_lines=30,
    key="ace_yaml_editor",
    auto_update=True
)

# --------------------------------------------------
# ACTION: SAVE YAML TO LAKEBASE
# --------------------------------------------------

if st.button("💾 Save to Lakebase Table"):
    json_result = yaml_to_json(edited_yaml)
    if json_result:
        st.session_state.converted_json = json_result
        
        # Show feedback messages
        msg1 = st.empty()
        msg1.success("✅ Successfully converted YAML above back to JSON")

        SQL = f"""
            UPDATE {database_table_name}
            SET {database_column_json} = %s::jsonb
            WHERE {database_column_rowid} = %s;
        """

        info1 = st.empty()
        info1.info("📡 Sending update to Lakebase...")

        msg2 = st.empty()
        msg2.success(f"✅ Finished updating Lakebase table '{database_table_name}'")

        time.sleep(6)
        msg1.empty()
        info1.empty()
        msg2.empty()

        # Execute SQL update
        execute_oltp_db_sql(
            oltp_conn,
            sql_statement=SQL,
            insert_update_list=[st.session_state.converted_json, st.session_state.row_id]
        )
    else: 
        st.error("❌ Invalid YAML format. Please check your YAML input.")



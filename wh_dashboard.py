import streamlit as st
import pandas as pd
from config import get_connection

# Snowflake connection function
conn = get_connection()

# Run query
def run_query(query):
    df = pd.read_sql(query, conn)
    return df

def run_show_command_to_df(cur, command):
    cur.execute(command)
    results = cur.fetchall()
    columns = [col[0] for col in cur.description]
    return pd.DataFrame(results, columns=columns)


# UI Layout
st.title("Warehouse Monitoring Dashboard")

st.sidebar.title("🎲 Choose and option")
section = st.sidebar.radio("Choose a metric view:", [
    "Live Dashboard 📈",
    "Credit Usage Overview 💰",
    "Long-Running Queries 🏃🏻‍♀️‍➡️",
    "Bytes Scanned & Cache Hit % 🎯",
    "Local Spill Analysis 🫗",
    "Remote Spill Analysis 🍾",
    "Warehouse Load Summary 🏋🏻‍♂️",
    "Cluster Config (Min/Max) ✨",
    "Queued Time Analysis ⏳"

])

# Sections
if section == "Credit Usage Overview 💰":
    st.subheader("Credit Usage (Last 24H)")
    query = """
    SELECT
    WAREHOUSE_NAME,
    SUM(CREDITS_USED) AS TOTAL_CREDITS
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    GROUP BY WAREHOUSE_NAME
    ORDER BY TOTAL_CREDITS DESC;
    """
    st.dataframe(run_query(query))

elif section == "Long-Running Queries 🏃🏻‍♀️‍➡️":
    st.subheader("Long-Running Queries (>5 min, Last 24H)")
    query = """
    SELECT QUERY_ID, USER_NAME, WAREHOUSE_NAME, TOTAL_ELAPSED_TIME/60000 AS MINUTES
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE START_TIME >= DATEADD('day', -1, CURRENT_TIMESTAMP)
      AND TOTAL_ELAPSED_TIME >= 300000
    ORDER BY MINUTES DESC;
    """
    st.dataframe(run_query(query))

elif section == "Bytes Scanned & Cache Hit % 🎯":
    st.subheader("Bytes Scanned & Cache Usage (Last 24H)")
    query = """
    SELECT QUERY_ID,
       BYTES_SCANNED / 1024 / 1024 AS BYTES_SCANNED, PERCENTAGE_SCANNED_FROM_CACHE  / 1024 / 1024 AS CACHE_HIT,
       (PERCENTAGE_SCANNED_FROM_CACHE / NULLIF(BYTES_SCANNED, 0)) * 100 AS CACHE_HIT_PERCENT
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE START_TIME >= DATEADD('day', -1, CURRENT_TIMESTAMP)
    ORDER BY BYTES_SCANNED DESC
    LIMIT 10;
    """
    st.dataframe(run_query(query))

elif section == "Local Spill Analysis 🫗":
    st.subheader("Top 10 Queries with Local Spill (Last 24H)")
    query = """
    SELECT QUERY_ID, USER_NAME, WAREHOUSE_NAME,
           BYTES_SPILLED_TO_LOCAL_STORAGE / 1024 / 1024 AS MB_LOCAL_SPILL
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE START_TIME >= DATEADD('day', -1, CURRENT_TIMESTAMP)
      AND BYTES_SPILLED_TO_LOCAL_STORAGE > 0
    ORDER BY MB_LOCAL_SPILL DESC
    LIMIT 10;
    """
    st.dataframe(run_query(query))

elif section == "Remote Spill Analysis 🍾":
    st.subheader("Top 10 Queries with Remote Spill (Last 24H)")
    query = """
    SELECT QUERY_ID, USER_NAME, WAREHOUSE_NAME,
           BYTES_SPILLED_TO_REMOTE_STORAGE / 1024 / 1024 AS MB_REMOTE_SPILL
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE START_TIME >= DATEADD('day', -1, CURRENT_TIMESTAMP)
      AND BYTES_SPILLED_TO_REMOTE_STORAGE > 0
    ORDER BY MB_REMOTE_SPILL DESC
    LIMIT 10;
    """
    st.dataframe(run_query(query))
    st.caption("Remote storage spills are more costly and impact performance — investigate queries and warehouse sizing.")

elif section == "Warehouse Load Summary 🏋🏻‍♂️":
    st.subheader("Warehouse Load Summary (Last 24H)")
    query = """
    SELECT WAREHOUSE_NAME,
           AVG(AVG_RUNNING) AS AVG_RUNNING_QUERIES,
           AVG(AVG_QUEUED_LOAD) AS AVG_QUEUE_LOAD,
           AVG(AVG_QUEUED_PROVISIONING) AS AVG_PROVISIONING_TIME_SECONDS,
           AVG(AVG_RUNNING) AS AVG_RUNNING,
           AVG(AVG_BLOCKED) AS AVG_BLOCKED_QUERIES
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
    WHERE START_TIME >= DATEADD('day', -1, CURRENT_TIMESTAMP)
    GROUP BY WAREHOUSE_NAME
    ORDER BY AVG_QUEUE_LOAD  DESC;
    """
    st.dataframe(run_query(query))

elif section == "Queued Time Analysis ⏳":
    st.subheader("Warehouse Queued Time Metrics (Last 24H)")
    query = """
    SELECT WAREHOUSE_NAME,
           AVG(AVG_RUNNING) AS AVG_RUNNING,
           AVG(AVG_QUEUED_LOAD) AS AVG_QUEUE_LOAD,
           AVG(AVG_QUEUED_PROVISIONING) AS AVG_PROVISIONING_TIME_SECONDS
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
    WHERE START_TIME >= DATEADD('day', -1, CURRENT_TIMESTAMP)
    GROUP BY WAREHOUSE_NAME
    ORDER BY AVG_QUEUE_LOAD DESC;
    """
    st.dataframe(run_query(query))

elif section == "Cluster Config (Min/Max) ✨":
    st.subheader("Warehouse Cluster Min/Max Settings")

    cur = conn.cursor()
    try:
        df = run_show_command_to_df(cur, "SHOW WAREHOUSES")

        df_filtered = df[["name", "min_cluster_count", "max_cluster_count", "scaling_policy", "state"]]
        df_filtered = df_filtered.rename(columns={
            "name": "WAREHOUSE_NAME",
            "min_cluster_count": "MIN_CLUSTER_COUNT",
            "max_cluster_count": "MAX_CLUSTER_COUNT",
            "scaling_policy": "SCALING_POLICY",
            "state": "ENABLED"
        }).sort_values("MAX_CLUSTER_COUNT", ascending=False)

        st.dataframe(df_filtered)

    finally:
        cur.close()

elif section == "Live Dashboard 📈":
    st.subheader("Live Warehouse & Query Monitoring (Last 10 min)")

    # Active Queries in last 10 min
    st.write("### Active Queries (Last 10 min)")
    query = """
    SELECT QUERY_ID, USER_NAME, WAREHOUSE_NAME, EXECUTION_STATUS,
           TOTAL_ELAPSED_TIME/1000 AS SECONDS_ELAPSED
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE START_TIME >= DATEADD('minute', -10, CURRENT_TIMESTAMP)
      AND EXECUTION_STATUS IN ('RUNNING', 'QUEUED')
    ORDER BY START_TIME DESC;
    """
    st.dataframe(run_query(query))

    # Active Warehouse State
    st.write("### Warehouse State Snapshot")

    cur = conn.cursor()
    try:
        df_wh_state = run_show_command_to_df(cur, "SHOW WAREHOUSES")
    finally:
        cur.close()
    st.dataframe(df_wh_state[['name', 'state', 'size', 'running', 'queued', 'scaling_policy']])

    # Warehouse Load (Last 10 min)
    st.write("### Warehouse Load Metrics (Last 10 min)")
    query = """
    SELECT WAREHOUSE_NAME,
           AVG(AVG_RUNNING) AS AVG_RUNNING,
           AVG(AVG_QUEUED_LOAD) AS AVG_QUEUED_LOAD,
           AVG(AVG_QUEUED_PROVISIONING) AS AVG_PROVISIONING_TIME
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
    WHERE START_TIME >= DATEADD('minute', -10, CURRENT_TIMESTAMP)
    GROUP BY WAREHOUSE_NAME
    ORDER BY AVG_QUEUED_LOAD DESC;
    """
    st.dataframe(run_query(query))

    if st.button("🔄 Refresh the Dashboard", type="tertiary"):
        st.rerun()

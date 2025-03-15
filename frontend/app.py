import streamlit as st
import os
import time
import requests
import pandas as pd
import plotly_express as px
from datetime import datetime

FASTAPI_URL = os.getenv("FASTAPI_URL");  # Update with your FastAPI URL

QUERIES = {
    "company_filings": {
        "sql": """
            SELECT 
                s.name AS company_name, 
                s.sic AS industry_code, 
                COUNT(*) AS filing_count
            FROM FINDATA_RAW.STAGING_{year}_Q{quarter}.RAW_SUB s
            GROUP BY s.name, s.sic
            ORDER BY filing_count DESC
            LIMIT 15
        """,
        "chart_type": "bar",
        "title": "Top Companies by Filing Count"
    },
    "revenue_trends": {
        "sql": """
            SELECT 
                s.name AS company_name,
                n.ddate AS report_date,
                n.value AS revenue_value
            FROM FINDATA_RAW.STAGING_{year}_Q{quarter}.NUM n
            JOIN FINDATA_RAW.STAGING_{year}_Q{quarter}.SUB s ON n.adsh = s.adsh
            WHERE LOWER(n.num_tag) LIKE '%revenue%'
            AND n.abstract = FALSE
            AND n.value BETWEEN 0 AND 50000000  -- Filter for values between 0 and 50M
            ORDER BY s.name, n.ddate
            LIMIT 100
        """,
        "chart_type": "line",
        "title": "Company Revenue Trends (0-50M Range)"
    },
    "industry_analysis": {
        "sql": """
            SELECT 
                COALESCE(sc.INDUSTRY_NAME, 
                    CASE 
                        WHEN s.sic BETWEEN '0100' AND '0999' THEN 'Agriculture, Forestry, & Fishing'
                        WHEN s.sic BETWEEN '1000' AND '1499' THEN 'Mining'
                        WHEN s.sic BETWEEN '1500' AND '1799' THEN 'Construction'
                        WHEN s.sic BETWEEN '1800' AND '1999' THEN 'Not Used'
                        WHEN s.sic BETWEEN '2000' AND '3999' THEN 'Manufacturing'
                        WHEN s.sic BETWEEN '4000' AND '4999' THEN 'Transportation & Public Utilities'
                        WHEN s.sic BETWEEN '5000' AND '5199' THEN 'Wholesale Trade'
                        WHEN s.sic BETWEEN '5200' AND '5999' THEN 'Retail Trade'
                        WHEN s.sic BETWEEN '6000' AND '6799' THEN 'Finance, Insurance, & Real Estate'
                        WHEN s.sic BETWEEN '7000' AND '8999' THEN 'Services'
                        WHEN s.sic BETWEEN '9000' AND '9999' THEN 'Public Administration'
                        ELSE 'Other/Unknown Industry (' || s.sic || ')'
                    END
                ) AS industry_name,
                COUNT(DISTINCT s.cik) AS company_count,
                AVG(n.value) AS avg_value
            FROM FINDATA_RAW.STAGING_{year}_Q{quarter}.RAW_NUM n
            JOIN FINDATA_RAW.STAGING_{year}_Q{quarter}.RAW_SUB s ON n.adsh = s.adsh
            LEFT JOIN FINDATA_RAW.REFERENCE.SIC_CODES sc ON s.sic = sc.SIC_CODE
            WHERE s.sic IS NOT NULL
            GROUP BY industry_name, sic
            ORDER BY company_count DESC
            LIMIT 10
        """,
        "chart_type": "pie",
        "title": "Industry Distribution Analysis"
    }
}

def get_data(query_key, year, quarter):
    """Execute parameterized query through FastAPI"""
    try:
        formatted_sql = QUERIES[query_key]["sql"].format(year=year, quarter=quarter)
        print(formatted_sql)
        response = requests.post(
            f"{FASTAPI_URL}/snowflake/execute",
            json={"sql": formatted_sql}
        )
        print("Sent request to FastAPI")
        if response.status_code == 200:
            return pd.DataFrame(response.json(), columns=response.json()[0].keys())
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error fetching data: {str(e)}")
        return pd.DataFrame()

def plot_data(df, chart_type, title):
    """Generate visualization based on chart type"""
    if df.empty:
        return st.warning("No data available for selected period")
    
    try:
        if chart_type == "line":
            fig = px.line(df, x=df.columns[1], y=df.columns[2], title=title)
            # Set y-axis range for revenue trends to make the visualization more meaningful
            if df.columns[2] == 'revenue_value':
                fig.update_yaxes(range=[0, 50000000])
        elif chart_type == "bar":
            fig = px.bar(df, x=df.columns[0], y=df.columns[2], title=title)
        elif chart_type == "pie":
            fig = px.pie(df, names=df.columns[0], values=df.columns[1], title=title)
        else:
            return st.error("Unsupported chart type")
        
        fig.update_layout(height=600)
        st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Visualization error: {str(e)}")

def main():
    st.set_page_config(page_title="Financial Analytics Dashboard", layout="wide")
    st.title("📊 FinFusion: SEC Financial Data Explorer")
    
    # Project introduction
    st.markdown("""
    **Welcome to FinFusion's Analytics Platform**  
    This dashboard provides insights into SEC financial filings using:
    - 🗄️ **Snowflake** for data warehousing
    - 🔄 **Airflow** for pipeline orchestration
    - 📈 **SEC filings** as primary data source
    
    Explore quarterly financial trends, company filings analysis, and industry distributions through interactive visualizations.
    """)
    
    # Sidebar controls
    with st.sidebar:
        st.header("Filters")
        current_year = datetime.now().year
        year = st.slider("Select Year (Currently only try for 2023_Q4 which Team 3 was assigned to. Others will trigger the complete DAG)", 2010, current_year, current_year)
        quarter = st.selectbox("Select Quarter", [1, 2, 3, 4], format_func=lambda x: f"Q{x}")
        
        st.divider()
        selected_query = st.selectbox(
            "Choose Analysis Type",
            options=list(QUERIES.keys()),
            format_func=lambda x: QUERIES[x]["title"]
        )
        
    if st.sidebar.button("Generate Report"):
        with st.spinner("Analyzing financial data..."):
            progress_bar = st.progress(0)
            progress = 0
            status_text = st.empty()
            try:
                # Trigger Airflow DAG
                dag_response = requests.post(
                    f"{FASTAPI_URL}/airflow/rundag/sec_data_pipeline",
                    json={"year": year, "quarter": quarter}
                )
                if dag_response.status_code != 200:
                    st.error(f"Pipeline activation failed: {dag_response.text}")
                    st.stop()

                # Monitor DAG progress
                dag_run_id = dag_response.json().get('dag_run_id')
                progress = 0
                while progress < 100:
                    status_response = requests.get(
                        f"{FASTAPI_URL}/airflow/rundag/sec_data_pipeline/{dag_run_id}"
                    )
                    if status_response.status_code != 200:
                        st.error(f"Status check failed: {status_response.text}")
                        st.stop()

                    dag_status = status_response.json().get('state', 'running')
                    if dag_status == 'success':
                        progress = 100
                        progress_bar.progress(progress)
                        status_text.success("Processing complete!")
                        break
                    elif dag_status in ['failed', 'error']:
                        st.error(f"Pipeline failed: {dag_status}")
                        st.stop()
                    else:
                        progress = min(progress + 10, 90)
                        progress_bar.progress(progress)
                        status_text.text(f"Status: {dag_status}...")
                        time.sleep(2)

            except Exception as e:
                st.error(f"Error triggering pipeline: {str(e)}")
                st.stop()
            
            # Fetch and display data
            df = get_data(selected_query, year, quarter)

            if not df.empty:
                print("Plotting part")
                # Show metadata
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Selected Period", f"{year} Q{quarter}")
                with col2:
                    st.metric("Total Records", len(df))
                with col3:
                    st.metric("Data Freshness", datetime.now().strftime("%Y-%m-%d"))
                
                # Display visualization
                st.subheader(QUERIES[selected_query]["title"])
                plot_data(
                    df,
                    QUERIES[selected_query]["chart_type"],
                    QUERIES[selected_query]["title"]
                )
                
                # Show raw data
                with st.expander("View Detailed Data"):
                    st.dataframe(df.sort_index(ascending=False))
            else:
                st.warning("No data found for selected period")

if __name__ == "__main__":
    main()

from airflow import DAG

from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

# Define Snowflake query to check the existence of all four tables
CHECK_TABLES_SQL = """
SELECT COUNT(*)
FROM information_schema.tables
WHERE table_name IN ('NUM', 'PRE', 'TAG', 'RAW_SUB');
"""

CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS Balance_Sheet (
    adsh STRING(20) NOT NULL,
    cik INT NOT NULL,
    company_name STRING(150),
    ticker STRING(10),
    sic STRING(4),
    filing_date DATE NOT NULL,
    fiscal_year INT NOT NULL,
    fiscal_period STRING(10) NOT NULL,
    tag STRING(256) NOT NULL,
    description STRING(512),
    ddate DATE NOT NULL,
    value DECIMAL(28,4),
    uom STRING(20),
    segment STRING(1024),
    source STRING(10),
    PRIMARY KEY (adsh, tag, ddate)
);

CREATE TABLE IF NOT EXISTS Income_Statement (
    adsh STRING(20) NOT NULL,
    cik INT NOT NULL,
    company_name STRING(150),
    ticker STRING(10),
    sic STRING(4),
    filing_date DATE NOT NULL,
    fiscal_year INT NOT NULL,
    fiscal_period STRING(10) NOT NULL,
    tag STRING(256) NOT NULL,
    description STRING(512),
    ddate DATE NOT NULL,
    value DECIMAL(28,4),
    uom STRING(20),
    segment STRING(1024),
    source STRING(10),
    PRIMARY KEY (adsh, tag, ddate)
);

CREATE TABLE IF NOT EXISTS Cash_Flow (
    adsh STRING(20) NOT NULL,
    cik INT NOT NULL,
    company_name STRING(150),
    ticker STRING(10),
    sic STRING(4),
    filing_date DATE NOT NULL,
    fiscal_year INT NOT NULL,
    fiscal_period STRING(10) NOT NULL,
    tag STRING(256) NOT NULL,
    description STRING(512),
    ddate DATE NOT NULL,
    value DECIMAL(28,4),
    uom STRING(20),
    segment STRING(1024),
    source STRING(10),
    PRIMARY KEY (adsh, tag, ddate)
);

"""

INSERT_BALANCE_SHEET_SQL = """
TRUNCATE TABLE Balance_Sheet;
INSERT INTO Balance_Sheet
SELECT 
    num.adsh,
    sub.cik,
    sub.name AS company_name,
    NULL AS ticker, 
    sub.sic,
    sub.filed AS filing_date,
    sub.fy AS fiscal_year,
    sub.fp AS fiscal_period,
    num.tag,
    tag.tlabel AS description,
    num.ddate,
    num.value,
    num.uom,
    num.segments AS segment,
    sub.form AS source
FROM NUM num
JOIN raw_SUB sub ON num.adsh = sub.adsh
JOIN TAG tag ON num.tag = tag.tag
JOIN PRE pre ON num.adsh = pre.adsh AND num.tag = pre.tag
WHERE pre.stmt = 'BS';
"""

INSERT_INCOME_STATEMENT_SQL="""
TRUNCATE TABLE Income_Statement;
INSERT INTO Income_Statement
SELECT 
    num.adsh,
    sub.cik,
    sub.name AS company_name,
    NULL AS ticker, 
    sub.sic,
    sub.filed AS filing_date,
    sub.fy AS fiscal_year,
    sub.fp AS fiscal_period,
    num.tag,
    tag.tlabel AS description,
    num.ddate,
    num.value,
    num.uom,
    num.segments AS segment,
    sub.form AS source
FROM NUM num
JOIN raw_SUB sub ON num.adsh = sub.adsh
JOIN TAG tag ON num.tag = tag.tag
JOIN PRE pre ON num.adsh = pre.adsh AND num.tag = pre.tag
WHERE pre.stmt = 'IS';
"""
INSER_CASH_FLOW_SQL="""
TRUNCATE TABLE Cash_Flow;
INSERT INTO Cash_Flow
SELECT 
    num.adsh,
    sub.cik,
    sub.name AS company_name,
    NULL AS ticker, 
    sub.sic,
    sub.filed AS filing_date,
    sub.fy AS fiscal_year,
    sub.fp AS fiscal_period,
    num.tag,
    tag.tlabel AS description,
    num.ddate,
    num.value,
    num.uom,
    num.segments AS segment,
    sub.form AS source
FROM NUM num
JOIN raw_SUB sub ON num.adsh = sub.adsh
JOIN TAG tag ON num.tag = tag.tag
JOIN PRE pre ON num.adsh = pre.adsh AND num.tag = pre.tag
WHERE pre.stmt = 'CF';

"""
# Python function to check if all 4 tables exist
def check_all_tables(**kwargs):
    ti = kwargs['ti']
    records = ti.xcom_pull(task_ids="check_tables_existence")
    print('here is result')
    print (records)

    if records and records[0]['COUNT(*)'] == 4:  # If all four tables exist
        return "proceed_with_next_task"
    else:
        return "no_required_tables_found"
    
# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "catchup": False
}

# Define the DAG
with DAG(
    dag_id="check_all_snowflake_tables",
    default_args=default_args,
    schedule_interval="@daily",  # Change as needed
    tags=["snowflake", "check_tables"],
) as dag:

    # Task to check if all four tables exist in Snowflake
    check_tables_existence = SnowflakeOperator(
        task_id="check_tables_existence",
        sql=CHECK_TABLES_SQL,
        snowflake_conn_id="snowflake_default",
        autocommit=True
    )

    # BranchPythonOperator to decide the next step based on the table check result
    decide_next_step = BranchPythonOperator(
        task_id="decide_next_step",
        python_callable=check_all_tables,
        provide_context=True
    )

    # Task to execute if all tables exist
    proceed_with_next_task = PythonOperator(
        task_id="proceed_with_next_task",
        python_callable=lambda: print("All required tables exist. Proceeding with the next task..."),
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    # Task to execute if any table is missing
    no_required_tables_found = PythonOperator(
        task_id="no_required_tables_found",
        python_callable=lambda: print("One or more required tables do not exist. Exiting..."),
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    create_missing_tables = SnowflakeOperator(
        task_id="create_missing_tables",
        sql=CREATE_TABLES_SQL,
        snowflake_conn_id="snowflake_default",
        autocommit=True
    )


    ###############DBT
    
    # Task to insert data into Balance_Sheet table
    insert_balance_sheet = SnowflakeOperator(
    task_id="insert_balance_sheet",
    sql=INSERT_BALANCE_SHEET_SQL,
    snowflake_conn_id="snowflake_default",
    autocommit=True
    )

    insert_income_statement = SnowflakeOperator(
    task_id="insert_income_statement",
    sql=INSERT_INCOME_STATEMENT_SQL,
    snowflake_conn_id="snowflake_default",
    autocommit=True
    )

    insert_cash_flow = SnowflakeOperator(
    task_id="insert_cash_flow",
    sql=INSER_CASH_FLOW_SQL,
    snowflake_conn_id="snowflake_default",
    autocommit=True
    )

    # Define task dependencies
    check_tables_existence >> decide_next_step
    decide_next_step >> proceed_with_next_task >> create_missing_tables>>insert_balance_sheet>>insert_income_statement>>insert_cash_flow
    decide_next_step >> no_required_tables_found 

    


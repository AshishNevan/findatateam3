# Fin-Fusion Team 3 - SEC Financial Data Pipeline

## Project Summary

The FinData SEC Financial Data Pipeline is a comprehensive data engineering solution designed to extract, transform, and analyze financial data from the U.S. Securities and Exchange Commission (SEC). This project automates the collection of corporate financial reports in JSON format, processes them into structured data, and makes them available for analysis through both a data warehouse and interactive visualization tools. The system enables financial analysts, researchers, and investors to easily access standardized financial data for informed decision-making.

Detailed guide available at [**codelabs**](https://codelabs-preview.appspot.com/?file_id=1tOHmsV6Y-T9I_fWOGeXFZ7c8S-rG_hSdUmV2feef4P4#0)

## Project Scope

- Automated scraping of financial statement data from SEC website for any year and quarter
- Transformation of raw JSON financial reports into structured, queryable data
- Data quality validation and error handling
- Storage in Snowflake data warehouse for analytical processing
- Interactive data visualization through Streamlit
- Orchestrated workflow management with Apache Airflow
- RESTful API access to processed financial data

## Deployed Services

- **Streamlit Dashboard**: [https://findatateam3-ylcxtmn2ragdbygsq5ezgp.streamlit.app](https://findatateam3-ylcxtmn2ragdbygsq5ezgp.streamlit.app)
- **Airflow Orchestration**: [34.152.46.1:8080](http://34.152.46.1:8080)
- **FastAPI Backend**: [https://nevchris242-findata-backend.hf.space](https://nevchris242-findata-backend.hf.space)

## Tech Stack

- **Apache Airflow**: Workflow orchestration
- **Snowflake**: Data warehousing
- **DBT (Data Build Tool)**: Data transformation
- **Python**: Core programming language
- **FastAPI**: Backend API services
- **Streamlit**: Data visualization
- **Docker**: Containerization

## Prerequisites

- Docker and Docker Compose
- Python 3.8+
- Snowflake account
- Git

## Setup Instructions

### 1. Clone the Repository

```bash
git clone <repository-url>
cd findatateam3
```

### 2. Configure DBT Profile

Edit the DBT profile configuration file:

```bash
nano dbt/.dbt/profiles.yml
```

Add your Snowflake connection details:

```yaml
findatateam3:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <your-snowflake-account>
      user: <your-snowflake-username>
      password: <your-snowflake-password>
      role: <your-snowflake-role>
      database: <your-snowflake-database>
      warehouse: <your-snowflake-warehouse>
      schema: <your-snowflake-schema>
      threads: 4
```

### 3. Set Up Environment Variables

Create a `.env` file in the project root:

```bash
touch .env
```

Add the following environment variables:

```
AIRFLOW_UID=50000
SNOWFLAKE_ACCOUNT=<your-snowflake-account>
SNOWFLAKE_USER=<your-snowflake-username>
SNOWFLAKE_PASSWORD=<your-snowflake-password>
SNOWFLAKE_ROLE=<your-snowflake-role>
SNOWFLAKE_DATABASE=<your-snowflake-database>
SNOWFLAKE_WAREHOUSE=<your-snowflake-warehouse>
SNOWFLAKE_SCHEMA=<your-snowflake-schema>
```

### 4. Start the Services

Launch the Airflow services using Docker Compose:

```bash
docker compose up -d
```

### 5. Access Airflow UI

Open your browser and navigate to:

```
http://localhost:8080
```

Default credentials:

- Username: airflow
- Password: airflow

### 6. Configure Airflow Connections

In the Airflow UI:

1. Navigate to Admin > Connections
2. Add a new connection for Snowflake:

   - Conn Id: snowflake_default
   - Conn Type: Snowflake
   - Host: <your-snowflake-account>
   - Login: <your-snowflake-username>
   - Password: <your-snowflake-password>
   - Schema: <your-snowflake-schema>
   - Extra: {"account": "<your-snowflake-account>", "warehouse": "<your-snowflake-warehouse>", "database": "<your-snowflake-database>", "role": "<your-snowflake-role>"}

3. Add a new connection for the HTTP backend:
   - Conn Id: http_backend_default
   - Conn Type: HTTP
   - Host: https://nevchris242-findata-backend.hf.space

### 7. Run the Data Pipeline

Trigger the DAG from the Airflow UI

### 8. Access the Data

- **Snowflake**: Query the processed data directly in your Snowflake instance
- **Streamlit Dashboard**: Visit [https://findatateam3-ylcxtmn2ragdbygsq5ezgp.streamlit.app](https://findatateam3-ylcxtmn2ragdbygsq5ezgp.streamlit.app)
- **API**: Access data via [https://nevchris242-findata-backend.hf.space](https://nevchris242-findata-backend.hf.space)

## Project Workflow

1. **Programmatic access**: Use the deployed API service to invoke the DAG, data will be uploaded to snowflake.
2. **Web app**: Visualize financial statement data on the deployed streamlit web application.

## Stopping the Services

```bash
docker compose down
```

## Troubleshooting

- **Airflow Connection Issues**: Verify connection parameters in the Airflow UI
- **Data Processing Errors**: Check Airflow logs in the UI or in the `logs/` directory
- **DBT Transformation Failures**: Review logs in `dbt/logs/`
- **API Access Problems**: Ensure the backend service is running and accessible

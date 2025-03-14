# Use the official Airflow base image
FROM apache/airflow:2.10.5

# Install required system dependencies (Chrome & WebDriver)
USER root
#RUN pip install --no-cache-dir apache-airflow-providers-snowflake
RUN apt-get update && apt-get install -y \
    wget unzip curl \
    && wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo 'deb http://dl.google.com/linux/chrome/deb/ stable main' >> /etc/apt/sources.list.d/google.list \
    && apt-get update && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# Switch back to Airflow user
USER airflow
RUN pip install --no-cache-dir apache-airflow-providers-snowflake==5.5.0
RUN pip install airflow-dbt
RUN pip install dbt-snowflake
#RUN pip install dbt-core==1.9.2






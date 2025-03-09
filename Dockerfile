FROM apache/airflow:latest

# Copy requirements and install dependencies
USER airflow
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt
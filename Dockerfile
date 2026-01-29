FROM apache/airflow:2.9.3

# Switch to airflow user (required by the image)
USER airflow

# Copy requirements
COPY requirements.txt /requirements.txt

# Install Python dependencies as airflow user
RUN pip install --no-cache-dir -r /requirements.txt

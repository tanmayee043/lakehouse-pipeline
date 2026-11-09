# One image for both services: Dagster (pipeline + scheduler) and the Streamlit dashboard.
FROM python:3.11.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=utf-8 \
    DAGSTER_HOME=/app/dagster_home

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Install dbt packages and write the manifest Dagster reads, so containers start ready.
RUN cd dbt_project && dbt deps --profiles-dir . && dbt parse --profiles-dir .

# Weather ML Project

## Overview
This project is a machine learning pipeline for weather data processing and analysis. It incorporates Apache Airflow for workflow orchestration, Apache Kafka for real-time data streaming, and dbt for data transformation.

## Features
- **Data Pipeline Orchestration**: Using Apache Airflow
- **Real-time Data Streaming**: Kafka producers and consumers for weather data
- **Data Transformation**: dbt models for structured data transformation
- **Containerized Architecture**: Docker-based deployment for all components

## Project Structure
```
├── airflow/           # Airflow DAGs and configurations
├── dbt/              # Data transformation models
├── kafka/            # Kafka producers and consumers
├── configs/          # Configuration files
├── data/            # Data storage
└── scripts/         # Utility scripts
```


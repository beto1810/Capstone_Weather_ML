{% docs weather_data %}


# Weather Analytics Data Pipeline Documentation

## Overview
This dbt project implements a weather analytics data pipeline that processes and analyzes weather data for provinces in Vietnam. The pipeline includes data ingestion from weather APIs, historical data analysis, and weather forecasting capabilities.

## Project Structure
```
dbt_weather/
├── models/
│   ├── 01_staging/      # Raw data staging
│   ├── 02_int/          # Intermediate transformations
│   ├── 03_marts/        # Business-level aggregations
│   └── 04_predict/      # Forecasting models
├── macros/              # Reusable SQL snippets
├── tests/               # Custom data tests
└── seeds/               # Static reference data
```

## Data Flow

### Staging Layer (`01_staging/`)
- **stg_weather_data**: Raw weather measurements from API
- **stg_vietnam_provinces**: Reference data for provinces
- **stg_vietnam_districts**: Reference data for districts

### Intermediate Layer (`02_int/`)
- **int_weather_province**: Daily weather metrics by province

### Marts Layer (`03_marts/`)
- **fct_weather_province**: Historical weather facts by province
- **fct_weather_region**: Regional weather analytics
- **dim_vietnam_provinces**: Province dimension table

### Prediction Layer (`04_predict/`)
- **predict_weather_province**: Next-day weather forecasts
- Uses ML model predictions based on historical patterns

## Key Features
- **Incremental Processing**: Updates only new or changed records
- **Data Quality Tests**: Validates measurements and relationships
- **Weather Forecasting**: ML-based predictions for next-day weather
- **Geographic Aggregation**: Province and region-level analytics

## Model Details

### Staging Models
Each staging model implements basic type casting and cleaning:
```sql
-- Example: stg_weather_data
SELECT
    cast(province_name as varchar) as province_name,
    cast(temperature_celsius as float) as temperature_celsius,
    cast(humidity_percent as integer) as humidity_percent,
    -- ... other fields
FROM raw_weather_data
```

### Intermediate Models
Transform and prepare data for analytical use:
```sql
-- Example: int_weather_province
SELECT
    province_id,
    date(weather_timestamp) as weather_date,
    avg(temperature_celsius) as avg_temperature,
    -- ... other aggregations
FROM stg_weather_data
GROUP BY 1, 2
```

## Running the Pipeline

### Initial Setup
```bash
# Install dependencies
dbt deps

# Build all models
dbt run --full-refresh
```

### Daily Operations
```bash
# Incremental update
dbt run

# Run tests
dbt test

# Generate docs
dbt docs generate
dbt docs serve
```

## Testing
- **Generic Tests**: Not null, unique, accepted ranges
- **Custom Tests**: Weather measurement validations
- **Data Quality**: Relationship validation between models

## Maintenance Schedule
| Task | Frequency | Command |
|------|-----------|---------|
| Incremental Update | Daily | `dbt run` |
| Full Refresh | Weekly | `dbt run --full-refresh` |
| Model Retraining | Monthly | `dbt run --select tag:ml-models` |

## Contact
For questions or issues, please create a GitHub issue or contact the data team.

{% enddocs %}

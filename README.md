# E-COMMERCE ETL PROJECT

This project performs ETL (Extract, Transform, Load) by extracting data from the eBay API, transforming it using Apache Spark, and loading it into ClickHouse data warehouse.

## Architecture Overview

- **Extract**: Fetch data from eBay Browse API
- **Transform**: Process data using Apache Spark
- **Load**: Store transformed data in ClickHouse

## System Requirements

- Python 3.9.x
- Docker and Docker Compose
- Apache Spark 3.x
- Apache Airflow 2.x
- ClickHouse

## Project Structure

```
E-Commerce-ETL/
├── app-logs/                  # Application log files
├── dags/                      # Airflow DAG definitions
├── docker/                    # Docker configurations
│   └── airflow.Dockerfile     # Airflow container config
├── docker-data/               # Docker volume data
├── docker-requirements/       # Docker dependency files
├── logs/                      # Log files
├── plugins/                   # Plugin files
├── src/                      # Source code
│   ├── __pycache__/         # Python cache
│   ├── .pytest_cache/       # Test cache
│   ├── extract/             # Data extraction modules
│   ├── interface/           # Interface definitions
│   ├── job/                 # Job definitions
│   ├── load/                # Data loading modules
│   ├── sql/                 # SQL queries
│   ├── test/                # Test files
│   ├── transform/           # Data transformation logic
│   └── utils/              # Utility functions
├── venv/                    # Virtual environment
├── .env                     # Environment variables
├── .gitignore              # Git ignore rules
├── docker-compose.yml      # Docker compose config
├── README.md               # Project documentation
└── requirements.txt        # Python dependencies
```
## Installation

1. Clone the repository:
```bash
git clone https://github.com/pnquang260805/E-Commerce-ETL.git
cd ./E-Commerce-ETL
```

2. Install required libraries:
```bash
pip install -r requirements.txt
```

3. Configure environment variables:
Create a `.env` file with:
```
# eBay API Configuration
EBAY_TOKEN=your_ebay_api_token

# Airflow Configuration
AIRFLOW_UID=50000
AIRFLOW_GID=50000
_AIRFLOW_WWW_USER_USERNAME=your_username
_AIRFLOW_WWW_USER_PASSWORD=your_password
```

4. Start services:
```bash
docker-compose up -d
```

## Data Pipeline

1. **Extraction (eBay API)**
   - Fetches item data from eBay Browse API
   - Handles API authentication and rate limiting
   - Returns data as Spark DataFrame

2. **Transformation (Apache Spark)**
   - Cleans and standardizes data
   - Performs data type conversions
   - Creates dimensional models:
     - Seller dimension
     - Item dimension
     - Category dimension
     - Item Category bridge
     - Date dimension
     - Best seller fact

3. **Loading (ClickHouse)**
   - Loads transformed data into ClickHouse tables
   - Handles incremental updates
   - Maintains data consistency

## Usage

1. Access Airflow UI:
   - URL: `http://localhost:8080`
   - Login with credentials from `.env`

2. Run ETL Pipeline:
   - Run "create_dwh" trigger
   - Locate "ebay_dag" in the DAGs list
   - Enable the DAG
   - Trigger manually or wait for scheduled run

3. Monitor Progress:
   - Check task status in Airflow UI
   - View logs for detailed execution information
   - Monitor ClickHouse for loaded data


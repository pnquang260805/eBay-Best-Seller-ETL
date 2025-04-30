# E-COMMERCE ETL PROJECT

This project performs the ETL process by extracting data from the eBay API and a database, transforming it, and loading it into Data warehouse.

## Project Structure

```
E-Commerce-ETL/
├── dags/                   # Apache Airflow DAG definitions
│   └── etl_dag.py         # Main ETL workflow definition
├── data/
│   └── mysqlsampledatabase.sql  # Sample database schema
├── docker/                 # Docker configuration files
│   └──airflow.Dockerfile # Airflow custom image
├── docker-requirements/    # Additional Docker dependencies
├── logs/                  #airflow log
├── plugins/               # Custom Airflow plugins
│   ├── operators/        # Custom operators
│   └── hooks/            # Custom hooks
├── src/                   # Main source code
|   ├── interface/
|   ├── job/
│   ├── extract/       # Data extraction modules
│   ├── transform/     # Data transformation logic
│   ├── load/         # Data loading components
|   ├── sql/
|   ├── test/
|   └── utils
|
├── test/                 # Test suite
│   ├── unit/            # Unit tests
│   └── integration/     # Integration tests
├── .env                  # Environment configuration
├── docker-compose.yml    # Docker services configuration
└── requirements.txt      # Python dependencies
```
## System Requirements

- Python 3.9.x
- Docker and Docker Compose

## Installation

1. Clone the repository:
```bash
https://github.com/pnquang260805/E-Commerce-ETL.git
cd ./E-Commerce-ETL
```

2. Install required libraries:
```bash
pip install -r requirements.txt
```

3. Configure the environment: 
Create a .env file with the following environment variables:
+ EBAY_TOKEN: Your eBay API token
+ AIRFLOW_UID: Airflow user ID (default: 50000)
+ AIRFLOW_GID: Airflow group ID (default: 50000)
+ _AIRFLOW_WWW_USER_USERNAME: Airflow Web UI username
+ _AIRFLOW_WWW_USER_PASSWORD: Airflow Web UIpassword
+ MINIO_ACCESS_KEY: Your MinIO access key
+ MINIO_SECRET_KEY: Your MinIO secret key

4. Start services using Docker::
```bash
docker-compose up -d
```

## Usage

1. Access the Airflow UI:
- Open your browser and navigate to: `http://localhost:8080`
- Log in using the username and password from the .env file:: ```_AIRFLOW_WWW_USER_USERNAME``` and ```_AIRFLOW_WWW_USER_PASSWORD```

2. Activate a DAG:
- Find the DAG you want to run in the list
- Enable the DAG using the toggle button
- Run the DAG using the "Trigger DAG" button
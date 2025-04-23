# E-COMMERCE ETL PROJECT

This project performs the ETL process by extracting data from the eBay API and a database, transforming it, and loading it into MinIO (S3-compatible storage).

## Project Structure

```
E-Commerce-ETL/
├── dags/               # Contains Apache Airflow DAGs
├── data/               # Directory for storing data
├── docker/             # Docker configuration
├── docker-requirements/ # Docker requirements
├── logs/               # Directory for log files
├── plugins/            # Custom plugins
├── src/                # Main source code
├── test/               # Contains test files
├── .env                # Environment configuration file
├── docker-compose.yml  # Docker Compose configuration
└── requirements.txt     # Required Python libraries
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
# Learn Airflow

This project is for practicing with the Airflow platform, utilizing Python version 3.9 and Airflow version 2.8.0.

## Setup

To set up the environment, install dependencies, and start using Airflow, follow these steps:

1. Clone this repository to your local machine.
2. Navigate to the project directory.
3. Activate a virtual environment and verify the installation by running:
   ```bash
   source venv/bin/activate && airflow version
   ```
4. Start Airflow standalone by executing the following command:
   ```bash
   airflow standalone
   ```
5. Open your web browser and navigate to `localhost:8080` to access the Airflow UI.
6. The default home directory for Airflow is `~/airflow`. Within this path resides the `airflow.cfg` file. 
   To enable the execution of DAGs from this project, adjust the `dags_folder` parameter in the `airflow.cfg` file to this project location.
   
   For more details on configuration and installation, refer to the [Official Quick Start Guide](https://airflow.apache.org/docs/apache-airflow/2.8.0/start).

## Practice Content

This repository contains practice materials covering the following topics:

- Usage of BashOperator and PythonOperator.
- Usage of the Taskflow API.
- Usage of Dynamic Task Mapping.
- Integration of Papermill with Jupyter Notebooks.


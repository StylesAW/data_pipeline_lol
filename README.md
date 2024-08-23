# Data_pipeline_lol
This project automates the extraction, transformation, and loading (ETL) of League of Legends statistics using Python and SQL. The pipeline is orchestrated with Apache Airflow via Astronomer, and the data is stored in Snowflake for analysis. The process ensures up-to-date, clean data ready for reporting and visualization.

## Setup
### **Overview**
This project involves deploying a Python script along with a Directed Acyclic Graph (DAG) to Astronomer. The configuration includes setting up a virtual environment with specific dependencies to ensure the proper execution of the DAG.

### **Prerequisites**
Before setting up the project, ensure you have the following:

- **Docker**: Installed and running. You can download it from the [Docker website](https://www.docker.com/products/docker-desktop).
- **Astronomer CLI**: Installed and configured. You can download it from the [Astronomer website](https://www.astronomer.io/docs/cli).
- **Python 3.8+**: Ensure Python is installed on your system.
- **Airflow**: Make sure Airflow is correctly set up in your Astronomer environment.

## Setup Instructions

1. **Clone the Repository**

2. Configure your Astronomer environment. You can do this through the Astronomer by using the Astronomer CLI. Ensure you have set up your Astronomer workspace and project:
    - CLI: Use commands such as astro workspace create, astro project init, and astro dev init to configure your environment. For detailed instructions, refer to the [Astronomer CLI documentation](https://www.astronomer.io/docs/astro/cli/overview).

3. Deploy the DAG to Astronomer
Ensure the DAG file and script are correctly placed in the dags/ directory of your Astronomer project. Then, use the Astronomer CLI to deploy:

```bash
astro deploy
```
   
ℹ️ **Additional Information:** The script code is included within the same Python file as the DAG. This approach was necessary due to the requirement for a separate virtual environment with distinct dependencies to ensure compatibility with the libraries used. To achieve this, I utilized the VirtualenvOperator, which allows for the creation and management of isolated Python environments within Airflow. This setup ensures that all dependencies are properly managed and conflicts are avoided, facilitating a more robust and maintainable workflow.
## DAG code
```python
from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
#setting args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
#Setting dag
dag = DAG(
    'upload_to_snowflake',
    default_args=default_args,
    description='DAG to dump dataframes to snowflake',
    schedule='@hourly', 
)
#TASKS
upload_task_matches = PythonVirtualenvOperator(
    task_id='upload_matches_to_snowflake_task',
    python_callable=upload_matches_to_snowflake,
    requirements=[
        'requests',
        'sqlalchemy==2.0.31',
        'pandas==2.2.2',
        'snowflake-connector-python==3.11.0',
        'snowflake-sqlalchemy==1.6.1'
    ],
    system_site_packages=True,
    dag=dag,
)
upload_task_items = PythonVirtualenvOperator(
    task_id='upload_items_info_task',
    python_callable=fetch_item_info,
    requirements=[
        'requests',
        'sqlalchemy==2.0.31',
        'pandas==2.2.2',
        'snowflake-connector-python==3.11.0',
        'snowflake-sqlalchemy==1.6.1'
    ],
    system_site_packages=True,
    dag=dag,
)
upload_task_champs = PythonVirtualenvOperator(
    task_id='upload_champs_info_task',
    python_callable=fetch_champs_info,
    requirements=[
        'requests',
        'sqlalchemy==2.0.31',
        'pandas==2.2.2',
        'snowflake-connector-python==3.11.0',
        'snowflake-sqlalchemy==1.6.1'
    ],
    system_site_packages=True,
    dag=dag,
)
#Execution order
upload_task_matches >> upload_task_items >> upload_task_champs
```




This project is ready to be deployed to Astronomer and use Airflow.
Use: "astro deploy" command once you have created an account and connected to Astronomer



## A **DAG** running successfully on Astronomer 
![](images/successful_statistics_astronomer.png)

![](images/dag_working.png)

## Tasks executing seamlessly within the Airflow environment.

![](images/dags_success.png)

---


## Data has been successfully loaded into Snowflake and is now being displayed as intended. The data pipeline has effectively ingested the data and the results are visible in the Snowflake environment.
### Matches
![](images/loaded_data_snowflake_matches.png)
### Items
![](images/loaded_data_snowflake_items.png)
### Champs
![](images/loaded_data_snowflake_champs.png)

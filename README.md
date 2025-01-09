## Prerequisites

Before setting up the project, ensure you have the following:

- Docker and Docker Compose installed on your system.

## Setup

To run this project using Docker, follow these steps:

1. Clone this repository to your local machine.
2. Navigate to the directory containing the `docker-compose.yml` file.
3. Create folder `data` and 4 folders inside: `source`, `bronze`, `silver` and `gold`.
4. Download [US_Accidents_March23.csv](https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents) file and move it to `source` folder.
5. Build and run the containers using Docker Compose:

```bash
docker-compose up -d --build
```
This command will start the necessary services defined in your docker-compose.yml, such as Airflow webserver, scheduler, Spark master, and worker containers.

## Directory Structure for Jobs
Ensure your Spark job files are placed in the following directories and are accessible to the Airflow container:

* Python job: jobs/python/wordcountjob.py
* Scala job: jobs/scala/target/scala-2.12/word-count_2.12-0.1.jar
* Java job: jobs/java/spark-job/target/spark-job-1.0-SNAPSHOT.jar

These paths should be relative to the mounted Docker volume for Airflow DAGs.

## Usage
After the Docker environment is set up, the `sparking_flow` DAG will be available in the Airflow web UI [localhost:8080](localhost:8080), where it can be triggered manually or run on its yearly schedule.
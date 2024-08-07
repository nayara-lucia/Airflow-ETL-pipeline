<h1 align='center'> ETL Pipeline with Apache Airflow</h1>

### About
In this project, I used the Apache Airflow architecture to build an automated ETL pipeline. The need to integrate data from multiple sources such as APIs and databases became critical for the business, and Airflow was chosen due to its ability to schedule, monitor, and manage complex workflows. I used the Airflow environment with Docker Compose to configure and manage the Airflow components locally, providing an agile development and testing environment prior to deployment.

### Pipeline architecture

![Untitled](https://github.com/user-attachments/assets/b965b12c-8bf4-4332-aa6a-1bcc26245f75)

### Workflow
![unknown_2024 08 06-21 51_1_clip_1-ezgif com-video-to-gif-converter (1)](https://github.com/user-attachments/assets/bca00fae-494e-4bfc-9988-c80b3bc2705a)

```diff
@@ Start ➡️ Check api availability ➡️ Extracts data by making N requests ➡️ Makes the necessary transformations, cleaning, standardizations, etc. ➡️ In parallel, store in a table in big query and in a sheet in google sheets ➡️ End @@
```

### Data

Our data source will be The Movie Database API, where we will extract data on the popular movies of the day.<br>
<i>References:</i> https://developer.themoviedb.org/reference/intro/getting-started

### Prerequisites
* API Key from TMDB
* ![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)
* ![Google Cloud](https://img.shields.io/badge/GoogleCloud-%234285F4.svg?style=for-the-badge&logo=google-cloud&logoColor=white)

### Files

1. <b><a href=""> dags/tmdb_dag.py </a></b> this file contains the creation of the dag with the python functions and the tasks of the processes.
2. <b><a href=""> Dockerfile, .env, docker-compose.yaml </a></b> files for the dockerized environment setup with the installations of the necessary libraries, and necessary resources.

### Installation 
```diff
- If you want to run the Dag, please create your own variables for connection to the your TMDB API and connections to Google Cloud
```
1. Download/pull the repo.
2. run ```docker-compose up -d ```
3. After running all the containers you can check the healthy status with ```docker ps```
4. Go to ```localhost:8080``` (This may take some time to appear, just wait or refresh)
5. Trigger the dag

# Results
<b>Operators used:</b> 
* Dummy Operator
* HTTP Sensor
* Python Operator
  
<i>references:</i> https://airflow.apache.org/docs/

<b>Airflow UI:</b> 
- Variables ➡️ general key/value store that is global and can be queried from the tasks.
- Connections ➡️ used for storing credentials and other information necessary for connecting to external services.

![image](https://github.com/user-attachments/assets/e3e170d2-4e3b-494c-91c5-a7bf4839d6db)

<b>Dag running:</b>

![VdeosemttuloFeitocomoClipchamp8-ezgif com-video-to-gif-converter](https://github.com/user-attachments/assets/1f885324-afb2-4cf7-8342-344d93fe0e3e)


<b>Dashboard with PowerBI:</b>

![unknown_2024 07 21-14 45_clip_1-ezgif com-video-to-gif-converter](https://github.com/user-attachments/assets/aaa43c59-03df-4719-aebd-7a13aaceda91)












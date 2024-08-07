from airflow import DAG
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.python import PythonOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable

from datetime import datetime, timedelta
from gspread_dataframe import set_with_dataframe
import pandas as pd
import requests
import warnings
warnings.filterwarnings("ignore")
import gspread

api_key = Variable.get("k_tmdb")
url = Variable.get("url_tmdb")
endpoint = Variable.get("endpoint_movies_tmdb")

project_id = Variable.get("project_id_bq_movies")
dataset_id = Variable.get("dataset_id_bq_movies")

open_sheet = Variable.get("sheet_tmdb_movies")


def fetch_movies(total_pages, url, api_key,endpoint, **kwargs):
    all_results = [] 
    page = 1
    
    while page <= total_pages:
        try:
            url_complete = f"{url}{endpoint}{page}&api_key={api_key}"
            headers = {
                "accept": "application/json"
                }
            response = requests.get(url_complete, headers=headers)
            if response.status_code == 200:
                data = response.json()
                all_results.extend(data.get('results', []))
        except:
            print(f"Failed to fetch page {page}: {response.status_code}")
            pass
        
        page += 1
        print(all_results)
        ti = kwargs['ti']
        ti.xcom_push(key='extract', value=all_results)
    

def transform(**kwargs):
    ti = kwargs['ti']
    df_movies = ti.xcom_pull(task_ids=kwargs['task_id'], key='extract')
    
    df_movies = pd.DataFrame(df_movies)
    
    
    for column in df_movies.columns:
        count = df_movies[column].isnull().sum()
        if count > 200:
            df_movies.drop(column,axis=1,inplace=True)
            
    
    for i, value in enumerate(df_movies['poster_path'].values):
        if value is None:
            df_movies.iloc[i,4] = "not_informed"
            
        
    df_movies['poster_path'] = df_movies['poster_path'].apply(lambda x: 'https://image.tmdb.org/t/p/original/' + x if x != "not_informed" else x)
    
    
    for i, value in enumerate(df_movies['genre_ids'].values):
        try:
            df_movies['genre_ids'][i] = df_movies['genre_ids'][i][0]
        except:
            df_movies['genre_ids'][i] = "not_informed"
            
            

    df_movies['release_date'] = pd.to_datetime(df_movies['release_date'],format="%Y-%m-%d",errors='coerce')

    df_movies['release_date'] = df_movies['release_date'].fillna('not_informed')

    df_movies['release_year'] = df_movies['release_date'].apply(lambda x: x.strftime('%Y') if x != "not_informed" else x)

    
    df_movies['genre_name'] = df_movies['genre_ids'].map(replace_genre)
    
    
    df_movies['original_language'] = df_movies['original_language'].apply(lambda x: x.replace('en', 'english') if isinstance(x, str) else x)

    ti.xcom_push(key='transformed', value=df_movies)
    

def replace_genre(x):
    genre_map = {
        28: "Action", 12: "Adventure", 16: "Animation", 35: "Comedy",
        80: "Crime", 99: "Documentary", 18: "Drama", 10751: "Family",
        14: "Fantasy", 36: "History", 27: "Horror", 10402: "Music",
        9648: "Mystery", 10749: "Romance", 878: "Science Fiction", 
        10770: "TV Movie", 53: "Thriller", 10752: "War", 37: "Western"
    }
    return genre_map.get(x, "None")


def bq_update(table_name, project_id, dataset_id, **kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids=kwargs['task_id'], key='transformed')
    rows = df.to_dict(orient='records')
    
    
    fail_on_error = True

    hook = BigQueryHook(gcp_conn_id='bq-nayara', use_legacy_sql=False)
    hook.insert_all(project_id=project_id,
                    dataset_id=dataset_id,
                    table_id=f"{table_name}",
                    rows=rows,
                    fail_on_error=fail_on_error)
    
def sheet_update(open_sheet, **kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids=kwargs['task_id'], key='transformed')
    
    hook = GoogleBaseHook(gcp_conn_id="sheets")
    credentials = hook.get_credentials()
    google_credentials = gspread.Client(auth=credentials)
    sheet = google_credentials.open(open_sheet)
    worksheet = sheet.worksheet("sheet1")
    set_with_dataframe(worksheet=worksheet, 
                       dataframe=df, 
                       include_index=False, 
                       include_column_header=True, 
                       resize=True)

# Argumentos da dag
args = {

    'owner': 'Nayara',
    'depends_on_past': False,
    'start_date': datetime(2024, 4,24),
    'email': ['n8572762@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}


with DAG('tmdb_etl', default_args=args, description='Extracting Daily Movie Data',
    schedule_interval='0 0 * * *', default_view='graph',tags=['ETL'],catchup=False) as dag:
    
    dummy_task1 = DummyOperator(
            task_id=f'START')
    
    task_http_sensor_check = HttpSensor(
    task_id="http_sensor_check",
    http_conn_id="tmdb_conn",
    endpoint=f'/3/movie/popular?language=en-US&page=1&api_key={api_key}',
    response_check=lambda response: "results" in response.json(),
    poke_interval=5,
    )
    
    task_requests = PythonOperator(
            task_id='extract',
            python_callable=fetch_movies,
            op_kwargs={'total_pages': 500, 'url': url, 'api_key':api_key,'endpoint':endpoint}
        )
    
    task_transform = PythonOperator(
        task_id='transform',
        python_callable=transform,
        op_kwargs={'task_id': 'extract'}
    )
    
    
    task_bq_update = PythonOperator(
            task_id=f'bq_update',
            python_callable=bq_update,
            op_kwargs={'table_name': 'movies', 'task_id': 'transform','project_id':project_id,'dataset_id':dataset_id,'table_name': 'movies'}
    )
    
    task_sheet_update = PythonOperator(
            task_id=f'sheet_update',
            python_callable=sheet_update,
            op_kwargs={'open_sheet': open_sheet,'task_id': 'transform'}
        )
    
    dummy_task2 = DummyOperator(
            task_id=f'END')
    
    dummy_task1 >> task_http_sensor_check >> task_requests >> task_transform >> [task_bq_update,task_sheet_update] >> dummy_task2 
    
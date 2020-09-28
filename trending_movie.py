from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.hooks.dbapi_hook import DbApiHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.models import Variable

from datetime import datetime
from airflow.models import Variable
import requests
from pprint import pprint

args = {
    'owner': 'ombapit',
    'start_date': datetime(2020, 9, 28)
}

dag = DAG(
    dag_id='trending_movie_tmdb',
    default_args=args,
    schedule_interval='0 0 * * *',
    tags=['tmdb']
)

# python function untuk step 1
def insert_db(**context):
    # get variable
    obj = Variable.get('TMDB_API', deserialize_json=True)
    # request get trending movie
    response = requests.get(obj["host"] + 'trending/all/week?api_key=' + obj["key"])
    
    page = 1
    resp = response.json()
    # for i in range(data.total_pages):
    for i in range(2):
        response = requests.get(obj["host"] + 'trending/all/week?api_key=' + obj["key"] + '&page=' + str(page))
        data = response.json()

        # looping data ke variable
        rows = []
        for key in data['results']:
            # value = data['results']
            if 'title' in key:
                title = key['title'].encode("utf-8")
                ori_title = key['original_title'].encode("utf-8")
                release_date = key['release_date']
            else:
                title = key['name'].encode("utf-8")
                ori_title = key['original_name'].encode("utf-8")
                release_date = key['first_air_date']
            # convert genre ke separated comma
            genres = ','.join(map(str, key['genre_ids']))

            row = (
                key['id'],
                title,
                release_date,
                ori_title,
                genres,
                key['media_type'],
                key['vote_average']
            )
            rows.append(row)

        # simpan data perpage
        api: MySqlHook = MySqlHook(default_conn_name='mysql_default')
        api.insert_rows(
            table='movie',
            rows=tuple(rows)
        )
        
        page +=1

    return 'success'
    
insert_api = PythonOperator(
    task_id='insert_data_tmdb',
    provide_context=True,
    python_callable=insert_db,
    dag=dag,
)

# python function untuk step 2
def filter_db():
    api = MySqlHook()
    data = api.get_records(
        sql='select * from movie where vote_average > 7'
    )
    
    # truncate table filter
    api.run(
        sql='truncate table movie_filter'
    )

    # insert ke table filter
    api.insert_rows(
        table='movie_filter',
        rows=data
    )

filter_data = PythonOperator(
    task_id='filter_data_movie',
    python_callable=filter_db,
    dag=dag,
)

# python function untuk step 3
def insert_genre_db(**context):
    # get variable
    obj = Variable.get('TMDB_API', deserialize_json=True)
    # request get trending movie
    response = requests.get(obj["host"] + 'genre/movie/list?api_key=' + obj["key"])
    data = response.json()

    # looping data ke variable
    rows = []
    for key in data['genres']:
        row = (
            key['id'],
            key['name']
        )
        rows.append(row)

    # simpan data
    api: MySqlHook = MySqlHook(default_conn_name='mysql_default')
    api.insert_rows(
        table='genre',
        rows=tuple(rows)
    )
    return 'success'
insert_genre_api = PythonOperator(
    task_id='insert_genre_tmdb',
    provide_context=True,
    python_callable=insert_genre_db,
    dag=dag,
)

# python function untuk step 4
def join_movie_genre():
    api: MySqlHook = MySqlHook(default_conn_name='mysql_default')

    # ambil data genre
    genre = api.get_records(
        sql='select id,name from genre'
    )
    # looping genre
    genres = dict()
    for x in genre:
        genres[str(x[0])] = x[1]

    # ambil data movie_filter
    data = api.get_records(
        sql='select id,genres from movie_filter'
    )

    # looping movie filter
    for x in data:
        list_genre = ''
        for g in x[1].split(','):
            if g in genres:
                list_genre += genres[g] + ','
        list_genre = list_genre.rstrip(',')

        # update data
        api.run(
            sql = 'update movie_filter set genres=\''+ list_genre +'\' where id=' + str(x[0])
        )

    return 'success'

join_movie = PythonOperator(
    task_id='join_movie_genre',
    python_callable=join_movie_genre,
    dag=dag,
)
# task pertama
insert_api >> filter_data >> join_movie

# task kedua
insert_genre_api >> join_movie
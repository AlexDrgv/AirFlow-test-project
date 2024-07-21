#!/usr/bin/env python
# coding: utf-8

# In[35]:


import pandas as pd
from datetime import timedelta
from datetime import datetime
import numpy as np
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


# In[36]:


#путь к датасету
TOP_1M_DOMAINS = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
TOP_1M_DOMAINS_FILE = 'vgsales.csv'


# In[37]:


#Задаем словарь дефолтными параметрами для дага
default_args = {
    'owner': 'a-drugov',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 9, 29),
    'schedule_interval': '0 12 * * *'
}


# Инициализируем и описываем сам DAG

# In[39]:


@dag(default_args=default_args, catchup=False)
def adrugov_airflow3():
    @task(retries=3)    #таск чтения данных
    def get_data():
            year = 1994 + hash(f'{"a-drugov"}') % 23
            games = pd.read_csv(TOP_1M_DOMAINS).query("Year == @year")
            return games
#1.Какая игра была самой продаваемой в этом году во всем мире?
    @task(retries=4)
    def top_game(games):
            top_game = games.groupby("Name",as_index=False).agg({"Global_Sales":"sum"})            .sort_values("Global_Sales",ascending=False).head(1).Name
            return top_game
#2.Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task(retries=4)
    def top_genres(games):
        top_genres = games.groupby("Genre",as_index=False).agg({"EU_Sales":"sum"})    .sort_values("EU_Sales",ascending=False).head(1).Genre
        return top_genres
 #3.На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? Перечислить все, если их несколько
    @task(retries=4)
    def top_platform_northam(games):
        top_platform_northam = games.query("NA_Sales > 1").groupby("Platform",as_index = False)                .agg({"Name":pd.Series.nunique})                .sort_values("Name",ascending=False).head(1).Platform
        return top_platform_northam
#4.У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
    @task(retries=4)
    def top_japan(games):
        top_japan = games.groupby("Publisher",as_index = False).agg({"JP_Sales":"mean"})            .sort_values("JP_Sales",ascending=False).head(1).Publisher
        return top_japan
#5.Сколько игр продались лучше в Европе, чем в Японии?
    @task(retries=4)
    def europe_biggerthanjapan(games):
            europe_biggerthanjapan = games.groupby("Name",as_index = False)                .agg({"EU_Sales":"sum","JP_Sales":"sum"})                .query("EU_Sales > JP_Sales")                .shape[0]
            return europe_biggerthanjapan
# Выводим данные
    @task(retries=4, retry_delay=timedelta(4))
    def print_data(top_game_df, top_genres_df, top_platform_northam_df, top_japan_df, europe_biggerthanjapan_df):

            year = 1994 + hash('a-drugov') % 23

            print(f"The most popular game in {year} is {top_game}")
            print(f"The most popular genres in Europe in {year} are {top_genres}")
            print(f"The most popular platform with the biggest amount of games being sold more than 1 millions copies in {year} in North America is {top_platform_northam}")
            print(f"The best publisher of mean sales in Japan in {year} is {top_japan}")
            print(f"{europe_biggerthanjapan} were sold in bigger quantity in Europe than in Japan in {year}")

    data = get_data()
    world_top_game = top_game(data)
    top_genres_eur = top_genres(data)
    top_platform_na = top_platform_northam(data)
    top_japan = top_japan(data)
    europe_biggerthanjapan = europe_biggerthanjapan(data)
    print_data(world_top_game, top_genres_eur, top_platform_na, top_japan, europe_biggerthanjapan)
    
adrugov_airflow3 = adrugov_airflow3()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:



     


# In[ ]:





# In[ ]:





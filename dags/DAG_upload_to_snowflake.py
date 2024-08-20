from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonVirtualenvOperator


#Functions
def upload_matches_to_snowflake():
    import re
    import pandas as pd
    import requests
    import snowflake.connector
    from sqlalchemy import create_engine, text
    import credentials
    
    api_key = credentials.api_key
    tag_line = 'RFRMA'
    game_name = 'Styles'
    server = 'americas.api.riotgames.com'
    region='la1'
    
    user = credentials.username
    password = credentials.password
    account = credentials.account
    warehouse= credentials.warehouse
    database= credentials.database
    schema= credentials.schema

    # We create a function to add an underscore before each capital
    def add_underscore_before_capital(column_name):
        import re
        """
        Adds an underscore before each capital letter in the column name

        Args:
        column_name (str): The original column name.

        Returns:
        str: The modified column name with underscores before each capital letter
        """
        # We use a regular expresion to find capitals and add underscore
        return re.sub(r'([A-Z])', r'_\1', column_name)
    
    endpoint = f'https://{server}/riot/account/v1/accounts/by-riot-id/{game_name}/{tag_line}?api_key={api_key}'
    res = requests.get(endpoint).json()
    df = pd.DataFrame([res])
    puuid = res['puuid']
    #return df, res['puuid']

    endpoint = f'https://{region}.api.riotgames.com/lol/summoner/v4/summoners/by-puuid/{puuid}?api_key={api_key}'
    res = requests.get(endpoint).json()
    df_summoner = pd.DataFrame([res])
    df_summoner = pd.concat([df, df_summoner] ,axis=1)
    df_summoner = df_summoner.loc[:,~df_summoner.columns.duplicated()]
    summoner_id = df_summoner["id"][0]
    #return df_summoner

    endpoint = f'https://la1.api.riotgames.com/lol/league/v4/entries/by-summoner/{summoner_id}?api_key={api_key}'
    summoner_rank = requests.get(endpoint).json()
    df_rank = pd.DataFrame([summoner_rank[0]])
    #return df_rank

    # concating
    df_summoner = pd.concat([df_summoner, df_rank] ,axis=1)
    # dropping
    df_summoner = df_summoner.drop('summonerId', axis=1)
    # transforming
    df_summoner['revisionDate'] = pd.to_datetime(df_summoner['revisionDate'],unit='ms')
    #return df_summoner

    summ_puuid = df_summoner.loc[0,'puuid']
    endpoint = f'https://{region}.api.riotgames.com/lol/champion-mastery/v4/champion-masteries/by-puuid/{summ_puuid}/top?api_key={api_key}'
    res = requests.get(endpoint).json()
    df_champs_mastery = pd.DataFrame(res)
    df_champs_mastery['lastPlayTime'] = pd.to_datetime(df_champs_mastery['lastPlayTime'],unit='ms')
    #return df_champs_mastery

    summ_puuid = df_summoner['puuid'][0]
    match_server='https://americas.api.riotgames.com'
    endpoint = f'{match_server}/lol/match/v5/matches/by-puuid/{summ_puuid}/ids?api_key={api_key}'
    res = requests.get(endpoint).json()
    last_20_matches = res
    #return last_20_matches

    basics =['puuid','riotIdGameName','win','lane','role','teamPosition','kills','deaths','assists','totalMinionsKilled','eligibleForProgression' ,'timePlayed']
    kills = ['killingSprees','firstBloodKill','firstTowerKill','doubleKills','tripleKills','quadraKills', 'pentaKills',]
    wards = ['wardsKilled','wardsPlaced','visionWardsBoughtInGame','detectorWardsPlaced',]
    objectives = ['objectivesStolen','turretKills','dragonKills',]
    pings = ['enemyMissingPings']
    misc = ['item0', 'item1', 'item2','item3', 'item4', 'item5','item6','goldEarned','largestKillingSpree','largestMultiKill', 'magicDamageDealtToChampions', 'magicDamageTaken','neutralMinionsKilled', 'participantId','physicalDamageDealtToChampions', 'physicalDamageTaken','teamId','totalDamageDealtToChampions','totalDamageTaken','totalHeal','totalHealsOnTeammates','trueDamageDealtToChampions','trueDamageTaken','visionScore',]
    stats = basics + kills + wards + objectives + pings + misc

    conn=snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema,
        role='ACCOUNTADMIN'
    )
    cursor=conn.cursor()
    # It's time to test this connection
    cursor.execute("SELECT CURRENT_TIMESTAMP;")
    result = cursor.fetchone()
    print("Successful connection, timestamp snowflake:", result[0])
    #Connect to Snowflake using sqlalchemy
    connect_string = (
        f'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}'
    )
    engine = create_engine(connect_string)
    for match in last_20_matches:
        match_id = match # match 
        table_name = "matches"
        query = f"SELECT COUNT(*) FROM {table_name} WHERE MATCH_ID='{match_id}'"
        cursor.execute(query)
        count = cursor.fetchone()
        if count[0] == 0:
            endpoint = f'https://americas.api.riotgames.com/lol/match/v5/matches/{match_id}?api_key={api_key}'
            res = requests.get(endpoint).json()
            df = pd.DataFrame(res['info']['participants'])
            df_match = df[stats].copy() # the purpose is do not create a view in order to use an independent copy
            df_match.loc[:, 'matchId'] = res['metadata']['matchId']
            df_match.loc[:, 'gameMode'] = res['info']['gameMode']
            df_match.loc[:, 'gameDuration'] = res['info']['gameDuration']
            df_match.columns = [add_underscore_before_capital(col) for col in df_match.columns]
            df_match.columns = df_match.columns.str.lower()
            try:
                with engine.connect() as connection:
                    df_match.to_sql(table_name, connection, index=False, if_exists='append', method='multi')
                print("success")
            except Exception as e:
                print(f'something has gone wrong {e}')
        else:
            print(f'it already exists {count[0]}')
    conn.close()
    cursor.close()
    engine.dispose()




def fetch_item_info():
    import pandas as pd
    import requests
    import snowflake.connector
    from sqlalchemy import create_engine, text
    import credentials
    
    user = credentials.username
    password = credentials.password
    account = credentials.account
    warehouse= credentials.warehouse
    database= credentials.database
    schema= credentials.schema
    
    version_lst = requests.get('http://ddragon.leagueoflegends.com/api/versions.json').json()
    endpoint = f'http://ddragon.leagueoflegends.com/cdn/{version_lst[0]}/data/en_US/item.json'
    res = requests.get(endpoint).json()
    item_lst = []
    for iid, data in res['data'].items():
        item = {
            'id' : iid,
            'name': data['name'],
            'plaintext': data['plaintext'],
            'stats': data['stats'],
            'gold_base': data['gold']['base'],
            'gold_total': data['gold']['total'],
            'gold_sell': data['gold']['sell'],
        }
        item_lst.append(item)
    df_items = pd.DataFrame(item_lst)
    df_items

    #Connect to Snowflake using pandas
    conn=snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema,
        role='ACCOUNTADMIN'
    )
    cursor=conn.cursor()
    cursor.execute("SELECT CURRENT_TIMESTAMP;")
    result = cursor.fetchone()
    print("Successful connection, timestamp snowflake:", result[0])
    connect_string = (
        f'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}'
    )
    engine = create_engine(connect_string) 
    table_name_main = "items"
    table_name_second = "items_stats"
    df_items_stats = pd.json_normalize(df_items['stats'])
    df_items = df_items.drop(columns=['stats'])
    df_items.columns = df_items.columns.str.lower()
    df_items_stats.columns = df_items_stats.columns.str.lower()
    try:
        query = f"SELECT COUNT(*) FROM {table_name_main}"
        cursor.execute(query)
        count = cursor.fetchone()
        if count[0] == 0:
            with engine.connect() as connection:
                df_items.to_sql(table_name_main, connection, index=False, if_exists='append')
                #df_items_stats.to_sql(table_name_second, connection, index=False, if_exists='append')
            print("success")
        else:
            print("It exists")
    except Exception as e:
        print(f'something has gone wrong {e}')
    conn.close()
    cursor.close()
    engine.dispose()

def fetch_champs_info():
    import pandas as pd
    import requests
    import snowflake.connector
    from sqlalchemy import create_engine, text
    import credentials
    
    user = credentials.username
    password = credentials.password
    account = credentials.account
    warehouse= credentials.warehouse
    database= credentials.database
    schema= credentials.schema
    
    # Retrieve the Current API Version
    version_lst = requests.get('http://ddragon.leagueoflegends.com/api/versions.json').json()
    endpoint = f'http://ddragon.leagueoflegends.com/cdn/{version_lst[0]}/data/en_US/champion.json'
    res = requests.get(endpoint).json()
    champ_lst = []
    for iid, data in res['data'].items():
        champ = {
            'id' : iid,
            'name': data['name'],
            'title': data['title'],
            'blurb': data['blurb'],
            'tags': data['tags'],
            'partype': data['partype'],
        }
        champ_lst.append(champ)
    df_champs = pd.DataFrame(champ_lst)

    conn=snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema,
        role='ACCOUNTADMIN'
    )
    cursor=conn.cursor()
    cursor.execute("SELECT CURRENT_TIMESTAMP;")
    result = cursor.fetchone()
    print("Successful connection, timestamp snowflake:", result[0])
    connect_string = (
        f'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}'
    )
    engine = create_engine(connect_string)
    table_name = "champions"
    df_champs.columns = df_champs.columns.str.lower()
    df_champs['tags'] = df_champs['tags'].apply(lambda x: ', '.join(x))
    try:
        query = f"SELECT COUNT(*) FROM {table_name}"
        cursor.execute(query)
        count = cursor.fetchone()

        if count[0] == 0:
            with engine.connect() as connection:
                df_champs.to_sql(table_name, connection, index=False, if_exists='append')
            print("success")
        else:
            print("It exists")
    except Exception as e:
        print(f'something has gone wrong {e}')
    conn.close()
    cursor.close()
    engine.dispose()


DAG
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
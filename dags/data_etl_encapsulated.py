import requests
import pandas as pd
import snowflake.connector
from sqlalchemy import create_engine, text
from datetime import datetime
import credentials
import re

def fetch_summoner_puuid(api_key, game_name, tag_line, server):
    endpoint = f'https://{server}/riot/account/v1/accounts/by-riot-id/{game_name}/{tag_line}?api_key={api_key}'
    res = requests.get(endpoint).json()
    df = pd.DataFrame([res])
    return df, res['puuid']

def fetch_summoner_data(puuid, api_key, region, df):
    endpoint = f'https://{region}.api.riotgames.com/lol/summoner/v4/summoners/by-puuid/{puuid}?api_key={api_key}'
    res = requests.get(endpoint).json()
    df_summoner = pd.DataFrame([res])
    df_summoner = pd.concat([df, df_summoner] ,axis=1)
    df_summoner = df_summoner.loc[:,~df_summoner.columns.duplicated()]
    #summoner_id = df_summoner["id"][0]
    return df_summoner

def fetch_rank_mastery(summoner_id, api_key, region):
    endpoint = f'https://la1.api.riotgames.com/lol/league/v4/entries/by-summoner/{summoner_id}?api_key={api_key}'
    summoner_rank = requests.get(endpoint).json()
    df_rank = pd.DataFrame([summoner_rank[0]])
    return df_rank

def concat_transform_dataframes(df_rank,df_summoner):
    # concating
    df_summoner = pd.concat([df_summoner, df_rank] ,axis=1)
    # dropping
    df_summoner = df_summoner.drop('summonerId', axis=1)
    # transforming
    df_summoner['revisionDate'] = pd.to_datetime(df_summoner['revisionDate'],unit='ms')
    return df_summoner

def fetch_champion_mastery(api_key, df_summoner, region):
    summ_puuid = df_summoner.loc[0,'puuid']
    endpoint = f'https://{region}.api.riotgames.com/lol/champion-mastery/v4/champion-masteries/by-puuid/{summ_puuid}/top?api_key={api_key}'
    res = requests.get(endpoint).json()
    df_champs_mastery = pd.DataFrame(res)
    df_champs_mastery['lastPlayTime'] = pd.to_datetime(df_champs_mastery['lastPlayTime'],unit='ms')
    return df_champs_mastery

def retrieve_match_ids(api_key, df_summoner):
    summ_puuid = df_summoner['puuid'][0]
    match_server='https://americas.api.riotgames.com'
    endpoint = f'{match_server}/lol/match/v5/matches/by-puuid/{summ_puuid}/ids?api_key={api_key}'
    res = requests.get(endpoint).json()
    last_20_matches = res
    return last_20_matches

def stats_setting():
    basics =['puuid','riotIdGameName','win','lane','role','teamPosition','kills','deaths','assists','totalMinionsKilled','eligibleForProgression' ,'timePlayed']
    kills = ['killingSprees','firstBloodKill','firstTowerKill','doubleKills','tripleKills','quadraKills', 'pentaKills',]
    wards = ['wardsKilled','wardsPlaced','visionWardsBoughtInGame','detectorWardsPlaced',]
    objectives = ['objectivesStolen','turretKills','dragonKills',]
    pings = ['enemyMissingPings']
    misc = ['item0', 'item1', 'item2','item3', 'item4', 'item5','item6','goldEarned','largestKillingSpree','largestMultiKill', 'magicDamageDealtToChampions', 'magicDamageTaken','neutralMinionsKilled', 'participantId','physicalDamageDealtToChampions', 'physicalDamageTaken','teamId','totalDamageDealtToChampions','totalDamageTaken','totalHeal','totalHealsOnTeammates','trueDamageDealtToChampions','trueDamageTaken','visionScore',]
    stats = basics + kills + wards + objectives + pings + misc
    return stats

def process_and_upload_matches_to_snowflake(api_key, user,password,account,warehouse,database,schema, last_20_matches, stats):
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


# We create a function to add an underscore before each capital
def add_underscore_before_capital(column_name):
            """
            Adds an underscore before each capital letter in the column name

            Args:
            column_name (str): The original column name.

            Returns:
            str: The modified column name with underscores before each capital letter
            """
            # We use a regular expresion to find capitals and add underscore
            return re.sub(r'([A-Z])', r'_\1', column_name)


def fetch_item_info(): 
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
    return df_items


def upload_items_to_snowflake(df_items,user,password,account,warehouse,database,schema):
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
    return df_champs

def upload_champs_to_snowflake(df_champs,user,password,account,warehouse,database,schema):
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

def get_credentials():
    user = credentials.username
    password = credentials.password
    account = credentials.account
    warehouse= credentials.warehouse
    database= credentials.database
    schema= credentials.schema
    return user, password, account, warehouse, database, schema

def upload_matches_to_snowflake():
    api_key = credentials.api_key
    tag_line = 'RFRMA'
    game_name = 'Styles'
    server = 'americas.api.riotgames.com'
    region='la1'

    user, password, account, warehouse, database, schema = get_credentials()

    df, puuid = fetch_summoner_puuid(api_key, game_name, tag_line, server)
    df_summoner = fetch_summoner_data(puuid, api_key, region, df)
    df_rank = fetch_rank_mastery(df_summoner["id"][0], api_key, region)
    df_summoner = concat_transform_dataframes(df_rank,df_summoner)
    df_champs_mastery = fetch_champion_mastery(api_key, df_summoner, region)
    last_20_matches = retrieve_match_ids(api_key, df_summoner)
    stats = stats_setting()
    
    process_and_upload_matches_to_snowflake(api_key, user,password,account,warehouse,database,schema, last_20_matches, stats)

def upload_items_to_snowflake():
    user, password, account, warehouse, database, schema = get_credentials()
    
    df_items = fetch_item_info()
    upload_items_to_snowflake(df_items,user,password,account,warehouse,database,schema)

def upload_champs_to_snowflake():
    user, password, account, warehouse, database, schema = get_credentials()
    
    df_champs = fetch_champs_info()
    upload_champs_to_snowflake(df_champs,user,password,account,warehouse,database,schema)


#if __name__ == "__main__":
    #upload_matches_to_snowflake()
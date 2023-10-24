from client_info import user_connection, token, usr_con_stronger
import pandas as pd
import spotipy
import requests
import math
import numpy as np
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


def get_playlists(connection:spotipy.Spotify):
    playlists = connection.current_user_playlists(offset=0)
    playlist_items = dict()
    for k in playlists['items']:
        playlist_items.update({k['name']:k['id']})
    return playlist_items

def browse_json_items(total:int):
    if total <= 0:
        return []
    else:
        result = []
        whole = math.ceil((int(total))/100)
        decimal = (total/100) - (int(total))//100
        for i in range(whole):
            result.append((i*100))
        if decimal:
            result.append(math.ceil(decimal*100))
        result[-1] = result[-2]+result[-1]
        return result
    
def get_playlist_tracks(playlist_id:str,connection:spotipy.Spotify):
    table = list()
    counter = 0
    try:
        playlist_tracks = connection.playlist_tracks(playlist_id,limit=100)
        rounds = browse_json_items(int(playlist_tracks['total']))
        for i in rounds[:-1]:
            playlist_tracks = connection.playlist_tracks(playlist_id,offset=i)
            top = len(range(i,rounds[counter+1]))-1
            for k in range(0,top):
                track = playlist_tracks['items'][k]['track'].items()
                track_dict = {item[0]: item[1] for item in track}
                table.append(track_dict)
            counter += 1
    except Exception as e:
        print(str(e))
    return table


def write_to_db(conn:snowflake.connector,table:str,df:pd.DataFrame,if_exists='replace'):
    new_df = list(zip(df.columns,df.dtypes))
    new_df = [(i[0],'string') if i[1] =='object' else (i[0],i[1]) for i in new_df]
    new_df = [(i[0],'int') if 'int' in str(i[1]) else (i[0],i[1]) for i in new_df]
    new_df = [(i[0],'float') if 'float' in str(i[1]) else (i[0],i[1]) for i in new_df]
    columns = [str(i[0])+' '+str(i[1])+'\n' for i in new_df]
    script = f"""
    CREATE TABLE {table}
    (
    {','.join(columns)}
    );
    """
    cursor = conn.cursor()
    if if_exists == 'replace':
        cursor.execute(f"DROP TABLE {table}")
        cursor.execute(script)
    else:
        for i in df.columns:
            if df[i].apply(lambda x: isinstance(x, list)).any():
                df[i] = df[i].apply(lambda x: '-'.join(x))
                
        for _, row in df.iterrows():
            query = f"INSERT INTO {table} ({', '.join(df.columns)}) VALUES ({', '.join(['%s' for _ in df.columns])})"
            values = tuple(row[col] for col in df.columns)
            cursor.execute(query,values)
            run = True
    try:
        if run:
            pass
        else:
            write_pandas(conn,df,table_name=table,quote_identifiers=False)
            run = True
    except Exception as e:
            if run:
                pass
            else:
                write_pandas(conn,df,table_name=table,quote_identifiers=True)
                run = True 
    finally:
        if run:
            print("Write Completed")
        else:

            print(str(e)+"Check inputs and code")


def get_track_info(track_id,spotify_connection:spotipy.Spotify):

    user_track = spotify_connection.track(track_id)
    get_genre = spotify_connection.artist(user_track['artists'][0]['id'])
    fields = {
        'track_id'     :        user_track['id']
        ,'track_name'   :     user_track['name']
        ,'track_genre'  :    get_genre['genres']
        ,'duration'     :        user_track['duration_ms']
        ,'popularity'   :      user_track['popularity']
        ,'track_number' :    user_track['track_number']
        ,'artist_id'    :       user_track['artists'][0]['id']
        ,'artist_name'  :     user_track['artists'][0]['name']
    }
    return fields

def get_user_recent_tracks(connection:spotipy.Spotify,search_date:str):
    from datetime import date
    from datetime import datetime
    import time
    param_date = date.fromisoformat(search_date)
    date_in_unix = int(time.mktime(param_date.timetuple()) * 1000)
    recent = connection.current_user_recently_played(limit=50,after=date_in_unix)
    all_fields = []
    for i in range(len(recent)):
        fields = {
            'track_id': recent['items'][i]['track']['id']
            ,'track_name': recent['items'][i]['track']['name']
            ,'artist_name': ' - '.join([recent['items'][i]['ltrack']['artists'][j]['name'] for j in range(len(recent['items'][i]['track']['artists']))])
            ,'from':datetime.utcfromtimestamp(int(recent['cursors']['before'])/1000).strftime('%Y-%m-%d %H:%M:%S')
            ,'to': datetime.utcfromtimestamp(int(recent['cursors']['after'])/1000).strftime('%Y-%m-%d %H:%M:%S')

        }
        all_fields.append(fields)
    return all_fields

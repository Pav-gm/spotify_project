import pandas as pd
import spotipy
from spotipy import SpotifyOAuth
import math
import json

def set_spotify_connection(connection_path:str):
    """
    Connects to Spotify Web API

    Parameters:
    - connection_path(string): File path of the json object that contains the Client ID and Client Secret.

    Returns:
    - object: An authenticated spotipy client instance.

    """
    with open(connection_path,'+r') as con_file:
        data = json.load(con_file) 
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id=data['Client ID'],
    client_secret=data['Client Secret'],
    redirect_uri="http://localhost:8080",
    scope="user-library-read user-read-playback-state user-read-recently-played playlist-read-private")
    )
    return sp

def get_playlists(connection:spotipy.Spotify) -> list:
    """
    Generates a dictionary of playlists for the specified user connection.
    
    Parameters:
    - connection (spotipy.Spotify): An authenticated spotipy client instance.

    Returns:
    - dictionary: returns a dictionary with playlist name(s), id(s), owner(s), public(s) and type(s).
    """
    limit = connection.current_user_playlists()['total']
    runs = browse_json_items(int(limit))
    _playlists = []
    for run in runs:
        playlists = connection.current_user_playlists(offset=run)
        for playlist in playlists['items']:
            fields = {
                "name": playlist['name'],
                "id": playlist['id'],
                "owner": playlist['owner']['display_name'],
                "public": playlist['public'],
                "type": playlist['typte']
            }
            _playlists.append(fields)
    return _playlists

def browse_json_items(total_items:int) -> list:
    """
    Generate a list of offsets to paginate through large sets of results.
    
    Parameters:
    - total (int): Total number of items.

    Returns:
    - list: List of offsets to be used for pagination, 
            including the exact number of items in the last batch.
    """
    OFFSET_LIMIT = 100
    if total_items < 1:
        return []
    else:
        all_pages = total_items // OFFSET_LIMIT
        offsets = [i*100 for i in range(all_pages)]
        if total_items % OFFSET_LIMIT:
            last_offset = all_pages * OFFSET_LIMIT
            offsets.extend([last_offset,total_items])
        return offsets

def get_track_info(track_id,spotify_connection:spotipy.Spotify) -> dict:
    """
    Retrieves detailed information of a specific track from Spotify.
    
    This function fetches a track's details including its name, genre, duration, 
    popularity, track number, artist's ID, and artist's name based on the track's ID.

    Parameters:
    - track_id (str): The ID of the Spotify track.
    - spotify_connection (spotipy.Spotify): An authenticated spotipy client instance.

    Returns:
    - dict: A dictionary containing the track's details:
        * track_id: The track's Spotify ID.
        * track_name: Name of the track.
        * track_genre: Genres associated with the track.
        * duration: Duration of the track in milliseconds.
        * popularity: Popularity of the track on Spotify (0-100).
        * track_number: The track's position in its album.
        * artist_id: The Spotify ID of the track's primary artist.
        * album_id: The Spotify ID of the track's album.

    Notes:
    - The function will concatenate artists with '-' if there's more than one present in the result.
    """
    user_track = spotify_connection.track(track_id)
    get_genre = spotify_connection.artist(user_track['artists'][0]['id'])
    fields = {
        'track_id'      :  user_track['id']
        ,'track_name'   :  user_track['name']
        ,'track_genre'  :  get_genre['genres']
        ,'duration'     :  user_track['duration_ms']
        ,'popularity'   :  user_track['popularity']
        ,'track_number' :  user_track['track_number']
        ,'artist_id'    :  user_track['artists'][0]['id']
        ,'album_id'     : user_track['album']['id']
    }
    return fields
    
def get_playlist_tracks(playlist_id:str, connection:spotipy.Spotify) -> list:
    """
    Fetches tracks from a Spotify playlist based on its ID.
    
    Given a playlist's ID, this function will retrieve the tracks contained in that playlist. 
    It handles pagination by using the `browse_json_items` function to determine the 
    appropriate offsets, fetching batches of up to 100 tracks at a time.

    Parameters:
    - playlist_id (str): The ID of the Spotify playlist.
    - connection (spotipy.Spotify): An authenticated spotipy client instance.

    Returns:
    - list: A list of dictionaries where each dictionary contains track details.

    Exceptions:
    - Prints exceptions raised during the process.

    Notes:
    - This function uses the Spotify API's limits and offset for pagination. 
      It fetches a maximum of 100 tracks at a time.
    """
    table = list()
    counter = 0
    try:
        playlist_tracks = connection.playlist_tracks(playlist_id, limit=1)
        rounds = browse_json_items(int(playlist_tracks['total']))
        for i in rounds[:-1]:
            playlist_tracks = connection.playlist_tracks(playlist_id, offset=i)
            top = len(range(i, rounds[counter+1])) - 1
            for k in range(0, top):
                track = playlist_tracks['items'][k]['track']['id']
                track_dict = get_track_info(track,connection)
                track_dict['playlist_id'] = playlist_id
                table.append(track_dict)
            counter += 1
    except Exception as e:
        print(str(e))
    return table

def get_user_recent_tracks(connection:spotipy.Spotify,search_date:str) -> list:
    """
    Retrieves a user's recently played tracks on Spotify after a specified date.

    This function fetches up to 50 of the user's most recently played tracks after 
    the given date. It returns details for each track, including its ID, name, 
    artist name, and the time range in which it was played.

    Parameters:
    - connection (spotipy.Spotify): An authenticated spotipy client instance.
    - search_date (str): A date string in the format 'YYYY-MM-DD'. The function 
      will fetch tracks played after this date.

    Returns:
    - list: A list of dictionaries, with each dictionary containing details of 
      a recently played track:
        * track_id: The Spotify ID of the track.
        * track_name: Name of the track.
        * artist_name: Name(s) of the track's artist(s).
        * from: The start timestamp of the play in 'YYYY-MM-DD HH:MM:SS' format.
        * to: The end timestamp of the play in 'YYYY-MM-DD HH:MM:SS' format.

    Notes:
    - The function will concatenate artists with '-' if there's more than one present in the result.
    """
    from datetime import date
    from datetime import datetime
    import time
    def get_fields(item:dict,position:int):
        fields = {
            'track_id': item['items'][position]['track']['id']
            ,'track_name': item['items'][position]['track']['name']
            ,'artist_name': ' - '.join([item['items'][position]['track']['artists'][j]['name'] for j in range(len(item['items'][position]['track']['artists']))])
            ,'from':datetime.utcfromtimestamp(int(item['cursors']['before'])/1000).strftime('%Y-%m-%d %H:%M:%S')
            ,'to': datetime.utcfromtimestamp(int(item['cursors']['after'])/1000).strftime('%Y-%m-%d %H:%M:%S')
        }
        return fields
    param_date = date.fromisoformat(search_date)
    date_in_unix = int(time.mktime(param_date.timetuple()) * 1000)
    recent = connection.current_user_recently_played(limit=50,after=date_in_unix)
    return [get_fields(recent,i) for i in range(len(recent))]

def get_artists_from_playlists(connection:spotipy.Spotify ,playlist_ids:list) -> list: 
    """
    Retrieves artist information from a list of Spotify playlist IDs.

    Given a Spotify client connection and a list of playlist IDs, this function iterates through each playlist
    and fetches the artist details for all tracks within those playlists. It handles pagination automatically
    by determining the number of total tracks and fetching them in batches, if necessary.

    Parameters:
    - connection (spotipy.Spotify): An authenticated Spotipy client instance.
    - playlist_ids (list): A list of playlist IDs from which to retrieve artist information.

    Returns:
    - list: A list of dictionaries, with each dictionary containing 'artist_id' and 'artist_name' for each artist
            found within the tracks of the given playlists.
    """
    output=[]
    for playlist in playlist_ids:
        limit = connection.playlist_tracks(playlist)['total']
        runs = browse_json_items(int(limit))
        for run in runs:
            artists = connection.playlist_tracks(playlist,offset=run)
            for item in artists['items']:
                [fields := {"artist_id": artist['id'], "artist_name": artist['name']} for artist in item['track']['artists']]
                output.append(fields)
    return output

def get_playlists_albums(playlist_id:str,connection:spotipy.Spotify):
    """
    Retrieves album details for all tracks in a given Spotify playlist.
    Args:
        playlist_id (str): The Spotify playlist ID.
        connection (spotipy.Spotify): A Spotipy Spotify client instance.
    Returns:
        list: A list of dictionaries, each containing details of an album 
              (including album ID, name, release date, artist ID, total tracks, and album type).
    """
    table = list()
    counter = 0
    try:
        playlist_tracks = connection.playlist_tracks(playlist_id, limit=1)
        rounds = browse_json_items(int(playlist_tracks['total']))
        for i in rounds[:-1]:
            playlist_tracks = connection.playlist_tracks(playlist_id, offset=i)
            top = len(range(i, rounds[counter+1])) - 1
            for k in range(0, top):
                fields = {
                'album_id' : playlist_tracks['items'][k]['track']['album']['id']
                , 'album_name' : playlist_tracks['items'][k]['track']['album']['name']
                , 'release_date' : playlist_tracks['items'][k]['track']['album']['release_date']
                , 'artist_id' : playlist_tracks['items'][k]['track']['album']['artists'][0]['id']
                , 'total_tracks' : playlist_tracks['items'][k]['track']['album']['total_tracks']
                , 'album_type' : playlist_tracks['items'][k]['track']['album']['type']
                }
                if fields not in table:
                    table.append(fields)
                else:
                    pass
            counter += 1
    except Exception as e:
        print(str(e))
    return table


def get_playlist_tracks_scd(playlist_id:str,connection:spotipy.Spotify):
    """
    Retrieves detailed track information from a specified Spotify playlist.
    Args:
        playlist_id (str): The Spotify playlist ID.
        connection (spotipy.Spotify): A Spotipy Spotify client instance.
    Returns:
        list: A list of dictionaries, each containing specific details of a track 
              (including the date it was added to the playlist, who added it, the track ID, and the playlist ID it was added to).
    """
    table = list()
    counter = 0
    try:
        playlist_tracks = connection.playlist_tracks(playlist_id, limit=1)
        rounds = browse_json_items(int(playlist_tracks['total']))
        for i in rounds[:-1]:
            playlist_tracks = connection.playlist_tracks(playlist_id, offset=i)
            top = len(range(i, rounds[counter+1])) - 1
            for k in range(0, top):
                fields = {
                'added_at'   : playlist_tracks['items'][k]['added_at']
                ,'added_by'   : playlist_tracks['items'][k]['added_by']
                ,'track_id'   : playlist_tracks['items'][k]['track']['id']
                ,'playlist_id': playlist_id
                }
                if fields not in table:
                    table.append(fields)
                else:
                    pass
            counter += 1
    except Exception as e:
        print(str(e))
    return table




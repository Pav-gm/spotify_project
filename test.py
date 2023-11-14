from spotify_operations import get_playlist_tracks,set_spotify_connection
from pandas import DataFrame

if __name__ == '__main__':
    con = set_spotify_connection(r'C:\Users\pavel\Desktop\Spotify_Project\connection_spotify.json')
    test = get_playlist_tracks('1JvsSfXPZh4NY5phCjd3G1',con)
    df = DataFrame(test)
    print(df.head())
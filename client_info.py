import spotipy
from spotipy.oauth2 import SpotifyOAuth
from spotipy.oauth2 import SpotifyClientCredentials

def user_connection():
    cli_id="cd31d3046995404b9acacb8a9996e721"
    rdt_url="http://localhost:8080"
    scp="user-library-read user-read-playback-state user-read-recently-played playlist-read-private"
    return spotipy.Spotify(auth_manager=SpotifyOAuth(
        client_id=cli_id,
        client_secret="b50dfe5f0cb04d28848ed14e88e2c4f7",
        redirect_uri=rdt_url,
        scope=scp)
    )

def token():
    CLIENT_ID = 'cd31d3046995404b9acacb8a9996e721'
    REDIRECT_URI = 'http://localhost:8080'
    SCOPE = "user-library-read user-read-playback-state user-read-recently-played playlist-read-private"
    auth_url = f"https://accounts.spotify.com/authorize?client_id={CLIENT_ID}&response_type=code&redirect_uri={REDIRECT_URI}&scope={SCOPE}"
    return auth_url

def usr_con_stronger():
    auth_manager = SpotifyClientCredentials(client_id="cd31d3046995404b9acacb8a9996e721",client_secret="b50dfe5f0cb04d28848ed14e88e2c4f7")
    sp = spotipy.Spotify(auth_manager=auth_manager)
    return sp

if __name__ == '__main__':
    print(token())
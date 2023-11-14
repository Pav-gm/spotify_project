<b><h1>Spotify & Snowflake Data Integration</h1></b>

This repository provides a set of Python functions designed to integrate Spotify's API data with Snowflake's database service. 
These functions allow users to fetch tracks, playlists, and other relevant data from Spotify and subsequently store it in a structured Snowflake table.

<b>Features</b>

<ol>
        <li><strong>Fetch Spotify Playlist Tracks</strong>: The function <code>get_playlist_tracks</code> retrieves the tracks in a given Spotify playlist.</li>
        <li><strong>Retrieve Track Information</strong>: With <code>get_track_info</code>, you can fetch detailed information about a specific track, such as its genre, duration, and artist.</li>
        <li><strong>Get User's Recent Tracks</strong>: Using <code>get_user_recent_tracks</code>, it's possible to retrieve a list of tracks recently played by a user.</li>
        <li><strong>Snowflake Connection</strong>: <code>snowflake_connection</code> allows you to establish a connection to a Snowflake instance using specified connection details.</li>
        <li><strong>Data Transfer to Snowflake</strong>: With <code>write_to_db</code>, you can directly write a pandas DataFrame into a Snowflake table, with options to customize the table's schema based on the DataFrame's structure.</li>
</ol>
  
<b>Prerequisites</b>

Python 3.x
spotipy library
snowflake-connector-python library
pandas library

<b>Config</b>

Before using any of these functions, .json files should be created for both Snowflake and Spotify credentials as follows:

<strong>Snowflake:</strong>

<li>User</li>
<li>Password</li>
<li>Account</li>
<li>Warehouse</li>
<li>Database</li>
<li>Schema</li>
<br>

<strong>Spotify</strong>

<li>Client ID</li>
<li>Client Secret</li>

from dotenv import load_dotenv
import os
import requests
import json
import pandas as pd
import gzip

load_dotenv()
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")


def get_app_access_token(client_id, client_secret):
    """
    Returns an access token to be used in the headers of GET API calls to
    the Get Category's Playlists and Get Playlist endpoints of Spotify.
    Docs to create an app found here: https://developer.spotify.com/documentation/web-api/concepts/apps
    Args:
        client_id: your Spotify's app client_id
        client_secret: your Spotif's client_secret
    Returns: an access token to be used in downstream API calls as Authorization headers
    """
    import base64
    auth_credentials_string = f"{client_id}:{client_secret}"
    auth_bytes = auth_credentials_string.encode('ascii')
    auth_base64 = base64.b64encode(auth_bytes)
    auth_str = auth_base64.decode('ascii')
    auth_url = 'https://accounts.spotify.com/api/token'
    auth_headers = {
        'Authorization': f"Basic {auth_str}",
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    auth_params = {
        'grant_type': 'client_credentials'
    }
    auth_response = requests.post(
        url=auth_url,
        data=auth_params,
        headers=auth_headers
    )
    access_token = auth_response.json()['access_token']
    return access_token    
    

access_token = get_app_access_token(client_id, client_secret)
bearer_token_headers = {
    'Authorization': f"Bearer {access_token}"
}

# 1. creating category_playlists_records ****************
category_id = "latin"
endpoint_url = f"https://api.spotify.com/v1/browse/categories/{category_id}/playlists"
 
response = requests.get(url=endpoint_url, headers=bearer_token_headers)

response_dict = json.loads(response.text)
data = []

for item in response_dict["playlists"]["items"]:
    tracks = item.get("tracks", dict())
    playlist_data = {
        "description": item.get("description", None),
        "name": item.get("name", None),
        "id": item.get("id", None),
        "tracks_url": tracks.get("href", None),
        "total_tracks": tracks.get("total", None),
        "snapshot_id": item.get("snapshot_id", None),
    }
    data.append(playlist_data)

df_playlists = pd.DataFrame(data=data)
# print(df_playlists.head())
df_playlists.to_csv("output/category_playlists_records.csv")

# 2. creating playlist_records **************************
playlist_ids = [playlist.get("id", None) for playlist in data]

data_playlists = []
for playlist_id in playlist_ids:
    if playlist_id:
        endpoint_url = f"https://api.spotify.com/v1/playlists/{playlist_id}" 
        response = requests.get(url=endpoint_url, headers=bearer_token_headers)
        resp_dict = json.loads(response.text)
        data_playlists.append(resp_dict)

data_p2 = []

for playlist in data_playlists:
    followers = playlist.get("followers", dict())
    playlist_info = {
        "id": playlist.get("id", None),
        "followers": followers.get("total", None)
    }
    data_p2.append(playlist_info)

df_pl_records = pd.DataFrame(data=data_p2)
df_pl_records = df_pl_records.drop_duplicates()
df_pl_records.to_csv("output/playlist_records.csv")
# print(df_pl_records.head())

# creating tracks_records and rest *******************************
data_tracks = []
data_playlist_track = []
track_artist_id_records = []
artist_records = []

for playlist in data_playlists:
    tracks = playlist.get("tracks", None)
    playlist_id = playlist.get("id", None)
    if tracks:
        for item in tracks["items"]:            
            track = item.get("track", dict())
            item_added_at = item.get("added_at", None)
            track_id = track.get("id", None)
            track_name = track.get("name", None)
            track_popularity = track.get("popularity", None)
            track_uri = track.get("uri", None)
            album = track.get("album", dict())
            artists = album.get("artists", list())
            album_type = album.get("album_type", None)
            track_data = {
                "album_type": album_type,
                "track_id": track_id,
                "name": track_name,
                "popularity": track_popularity,
                "uri": track_uri,
            }
            data_tracks.append(track_data)
            track_added_data = {
                "playlist_id": playlist_id,
                "playlist_added_at": item_added_at,
                "track_id": track_id,
            }
            data_playlist_track.append(track_added_data)
            for artist in artists:
                artist_id = artist.get("id", None)
                artist_name = artist.get("name", None)
                tr_art_id_data = {
                    "track_id": track_id,
                    "artist_id": artist_id,
                }
                track_artist_id_records.append(tr_art_id_data)
                artist_records.append((artist_id, artist_name))

df_tracks = pd.DataFrame(data_tracks)
df_tracks = df_tracks.drop_duplicates()
df_tracks.to_csv("output/tracks_records.csv")

# creating playlist_track_id_records ********************
df_playlist_track_id_records = pd.DataFrame(data_playlist_track)
df_playlist_track_id_records.to_csv("output/playlist_track_id_records.csv")

# creating track_artist_id_records **********************
df_track_artists = pd.DataFrame(track_artist_id_records)
df_track_artists.to_csv("output/track_artist_id_records.csv")

# creating artists_records ******************************
df_artists = pd.DataFrame(artist_records, columns=["artist_id", "artist_name"])
df_artists = df_artists.drop_duplicates()
df_artists.to_csv("output/artists_records.csv")

# compress the csv files using gzip *********************

def compress_file(file_name, output_dir):
    with open(file_name, 'rb') as f_in:
        with gzip.open(os.path.join(output_dir, f'{os.path.basename(file_name)}.gz'), 'wb') as f_out:
            f_out.writelines(f_in)
    os.remove(file_name)


def find_compress_files(input_dir, output_dir):
    for filename in os.listdir(input_dir):
        if filename.endswith(".csv"):
            file_path = os.path.join(input_dir, filename)
            compress_file(file_path, output_dir)


# directories are dependent on the environment
input_dir = 'output'
output_dir = 'output/compressed'
find_compress_files(input_dir, output_dir)

print("The files have been successfully written and compressed in output/compressed.")

from dotenv import load_dotenv
import os
import requests
import json
import pandas as pd

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

# creating category_playlists_records ****************
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
# TODO write to csv

# creating playlist_records **************************
playlist_ids = [playlist.get("id", None) for playlist in data]

data_playlists = []
for playlist_id in playlist_ids:
    if playlist_id:
        endpoint_url = f"https://api.spotify.com/v1/playlists/{playlist_id}" 
        response = requests.get(url=endpoint_url, headers=bearer_token_headers)
        resp_dict = json.loads(response.text)
        data_playlists.append(resp_dict)
        # followers = resp_dict.get("followers", dict())
        # playlist_info = {
        #     "id": playlist_id,
        #     "followers": followers.get("total", None)
        # }
        # data_playlists.append(playlist_info)

for playlist in data_playlists:
    followers = playlist.get("followers", dict())
    playlist_info = {
        "id": playlist.get("id", None),
        "followers": followers.get("total", None)
    }

df_pl_records = pd.DataFrame(data=data_playlists)
df_pl_records.to_csv("output/df_pl_records.csv")
# print(df_pl_records.head())

# creating tracks_records ****************************


# creating playlist_track_id_records *****************
# creating track_artist_id_records *******************
# creating artists_records ***************************


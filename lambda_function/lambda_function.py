import os
import base64
from requests import post, get
import json
from kafka import KafkaProducer
from json import dumps

client_id = "092aadc50b294167ad92303ff8244d2a"
client_secret = "541646b1b6af4d6390798c4a9bb3789e"

producer = KafkaProducer(bootstrap_servers=['52.91.46.51'],  # change ip here
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))


def get_token():
    auth_string = client_id + ":" + client_secret
    auth_bytes = auth_string.encode("utf-8")
    auth_base64 = str(base64.b64encode(auth_bytes), "utf-8")

    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Authorization": "Basic " + auth_base64,
        "Content-Type": "application/x-www-form-urlencoded"
    }

    data = {"grant_type": "client_credentials"}
    result = post(url, headers=headers, data=data)
    json_result = json.loads(result.content)
    token = json_result["access_token"]
    return token


def get_auth_header(token):
    return {"Authorization": "Bearer " + token}


def get_new_releases(token, limit=30):
    offset = int(os.environ.get('OFFSET', '30'))
    url = "https://api.spotify.com/v1/browse/new-releases"
    headers = get_auth_header(token)
    query = f"market=us&offset={offset}&limit={limit}"
    query_url = url + "?" + query
    result = get(query_url, headers=headers)
    json_result = json.loads(result.content)["albums"]["items"]

    offset += limit
    limit = 3

    for album in json_result:
        album_id = album['id']
        tracks = get_tracks(token, album_id)

    os.environ['OFFSET'] = str(offset + limit)


def get_tracks(token, album_id):
    url = f"https://api.spotify.com/v1/albums/{album_id}/tracks"
    headers = get_auth_header(token)
    result = get(url, headers=headers)
    json_result = json.loads(result.content)
    track_info = []

    for track in json_result["items"]:
        track_id = track["id"]
        info = get_track_info(token, track_id)
        producer.send('spotify_topic', value=info)
        producer.flush()
        track_info.append(info)
    return track_info


def get_track_info(token, track_id):
    url = f"https://api.spotify.com/v1/tracks/{track_id}"
    headers = get_auth_header(token)
    result = get(url, headers=headers)
    json_result = json.loads(result.content)
    track_info = {
        "track_id": track_id,
        "name": json_result["name"],
        "artists": [artist["name"] for artist in json_result["artists"]],
        "album_name": json_result["album"]["name"],
        "popularity": json_result["popularity"],
        "release_date": json_result["album"]["release_date"],
        "track_duration": json_result["duration_ms"]
    }
    return track_info


def lambda_handler(event, context):
    token = get_token()
    tracks = get_new_releases(token)

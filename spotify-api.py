#!/usr/bin/env python
# coding: utf-8

# get_ipython().run_line_magic('matplotlib', 'inline')

import os
# import my_spotify_credentials as credentials
import numpy as np
import pandas as pd
# import ujson
import spotipy
import spotipy.util
# import seaborn as sns

os.environ["SPOTIPY_CLIENT_ID"] = '853e88b971b343b694a527af7fb5b182'
os.environ["SPOTIPY_CLIENT_SECRET"] = '63b40ec5180f42ce9f8838c89919bb65'
os.environ["SPOTIPY_REDIRECT_URI"] = 'http://localhost:8888/callback'
scope = 'user-follow-read, user-library-read'
username = '369m7qwebxgur7mgugkns9qzf'

token = spotipy.util.prompt_for_user_token(username, scope)

if token:
    spotipy_obj = spotipy.Spotify(auth=token)
    # saved_users_resp = spotipy_obj.current_user_following_users(ids='369m7qwebxgur7mgugkns9qzf')
    saved_tracks_resp = spotipy_obj.current_user_saved_tracks(limit=50)
else:
    print('Couldn\'t get token for that username')
print(saved_tracks_resp)
number_of_tracks = saved_tracks_resp['total']
print('%d tracks' % number_of_tracks)


def save_only_some_fields(track_response):
    return {
        'id': str(track_response['track']['id']),
        'name': str(track_response['track']['name']),
        'artists': [artist['name'] for artist in track_response['track']['artists']],
        'duration_ms': track_response['track']['duration_ms'],
        'popularity': track_response['track']['popularity'],
        'added_at': track_response['added_at']
    }


tracks = [save_only_some_fields(track) for track in saved_tracks_resp['items']]

while saved_tracks_resp['next']:
    saved_tracks_resp = spotipy_obj.next(saved_tracks_resp)
    tracks.extend([save_only_some_fields(track) for track in saved_tracks_resp['items']])

tracks_df = pd.DataFrame(tracks)
pd.set_option('display.max_rows', len(tracks))

# pd.reset_option('display.max_rows')

tracks_df['artists'] = tracks_df['artists'].apply(lambda artists: artists[0])
tracks_df['duration_ms'] = tracks_df['duration_ms'].apply(lambda duration: duration / 1000)
tracks_df = tracks_df.rename(columns={'duration_ms': 'duration_s'})

audio_features = {}

for idd in tracks_df['id'].tolist():
    audio_features[idd] = spotipy_obj.audio_features(idd)[0]

tracks_df['acousticness'] = tracks_df['id'].apply(lambda idd: audio_features[idd]['acousticness'])
tracks_df['speechiness'] = tracks_df['id'].apply(lambda idd: audio_features[idd]['speechiness'])
tracks_df['key'] = tracks_df['id'].apply(lambda idd: str(audio_features[idd]['key']))
tracks_df['liveness'] = tracks_df['id'].apply(lambda idd: audio_features[idd]['liveness'])
tracks_df['instrumentalness'] = tracks_df['id'].apply(lambda idd: audio_features[idd]['instrumentalness'])
tracks_df['energy'] = tracks_df['id'].apply(lambda idd: audio_features[idd]['energy'])
tracks_df['tempo'] = tracks_df['id'].apply(lambda idd: audio_features[idd]['tempo'])
tracks_df['time_signature'] = tracks_df['id'].apply(lambda idd: audio_features[idd]['time_signature'])
tracks_df['loudness'] = tracks_df['id'].apply(lambda idd: audio_features[idd]['loudness'])
tracks_df['danceability'] = tracks_df['id'].apply(lambda idd: audio_features[idd]['danceability'])
tracks_df['valence'] = tracks_df['id'].apply(lambda idd: audio_features[idd]['valence'])


class getSong():
    def __init__(self):
        super(getSong, self).__init__()

    def passs(self):
        return tracks_df

# 'name', 'artists', 'duration_ms', 'explicit', 'danceability', 'energy',
#        'key', 'loudness', 'mode', 'speechiness', 'acousticness',
#        'instrumentalness', 'liveness', 'valence', 'tempo'

if __name__ == '__main__':
    A = getSong()
    A.passs()
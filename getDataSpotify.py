import os
import glob
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from dotenv import load_dotenv

def process_files(dir_path):
    files = glob.glob(os.path.join(dir_path, 'part*'))  # Match all files with the given prefix
    print(len(files))
    for file_path in files:
        print(f"Processing file: {file_path}")
        with open(file_path) as file:
            list_tracks = [line.rstrip() for line in file]
            track_data = sp.tracks(tracks=list_tracks)
            print(*track_data['tracks'], sep='\n')

load_dotenv()
cid = os.getenv('SPOTIFY_CLIENT_ID')
secret = os.getenv('SPOTIFY_CLIENT_SECRET')
client_credentials_manager = SpotifyClientCredentials(client_id=cid, client_secret=secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
prefix_path = 'track_uris/'
#track_data = sp.tracks(tracks=list_tracks)
process_files(prefix_path)
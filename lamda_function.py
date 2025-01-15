import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):
    # Fetch Spotify API credentials from environment variables
    client_id = os.environ.get('SPOTIPY_CLIENT_ID')
    client_secret = os.environ.get('SPOTIPY_CLIENT_SECRET')

    if not client_id or not client_secret:
        return {
            'statusCode': 500,
            'body': json.dumps("Spotify API credentials are not set in environment variables.")
        }

    # Initialize Spotify authorization
    auth_manager = SpotifyClientCredentials(
        client_id=client_id,
        client_secret=client_secret
    )

    sp = spotipy.Spotify(auth_manager=auth_manager)

    # Default playlist URL (can be overridden via event)
    playlist_url = event.get('playlist_url', 'https://open.spotify.com/playlist/37i9dQZEVXbMWDif5SCBJq')
    playlist_URI = playlist_url.split("/")[-1]
    if "?" in playlist_URI:
        playlist_URI = playlist_URI.split("?")[0]  # Remove query parameters if present

    try:
        # Fetch playlist data
        data = sp.playlist_tracks(playlist_URI)
    except Exception as e:
        print(f"Error fetching playlist tracks: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error fetching playlist tracks: {str(e)}")
        }

    # S3 bucket details
    bucket_name = os.environ.get("S3_BUCKET_NAME")
    if not bucket_name:
        return {
            'statusCode': 500,
            'body': json.dumps("S3 bucket name is not set in environment variables.")
        }

    s3_client = boto3.client('s3')
    filename = "spotify_raw_" + datetime.now().strftime("%Y%m%d_%H%M%S") + ".json"

    try:
        # Upload data to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=f"raw_data/to_processed/{filename}",
            Body=json.dumps(data)
        )
        print(f"Data successfully uploaded to S3: {bucket_name}/raw_data/to_processed/{filename}")
    except Exception as e:
        print(f"Error saving to S3: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error saving to S3: {str(e)}")
        }

    return {
        'statusCode': 200,
        'body': json.dumps(f"Data successfully uploaded to S3: {bucket_name}/raw_data/to_processed/{filename}")
    }

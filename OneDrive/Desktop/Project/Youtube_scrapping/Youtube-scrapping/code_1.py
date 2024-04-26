import streamlit as st
import pandas as pd
from googleapiclient.discovery import build
import pymysql

api_key = "AIzaSyDkW3NmT3ccQqMTaP0k1hrqUl1CYF6eiRs"
channel_id = input()
youtube = build('youtube','v3',developerKey = api_key)

# Function to fetch channel details
def get_channel_details(youtube, channel_id):
    request = youtube.channels().list(
        part='snippet,contentDetails,statistics,status',
        id=channel_id
    )
    response = request.execute()
    for item in response['items']:
        return {
            'channel_id': item['id'],
            'channel_name': item['snippet']['title'],
            'channel_views': item['statistics']['viewCount'],
            'total_videos': item['statistics']['videoCount'],
            'channel_description': item['snippet']['description'],
            'channel_status': item['status']['privacyStatus'],
            'playlist_id': item['contentDetails']['relatedPlaylists']['uploads']
        }

# Function to fetch playlist info
def extract_playlist_info(youtube, playlist_id):
    request = youtube.playlists().list(
        part='snippet,status,contentDetails,id,localizations,player',
        id=playlist_id
    )
    response = request.execute()
    playlist_info = []
    for item in response['items']:
        snippet = item.get('snippet', {})
        content_details = item.get('contentDetails', {})
        status = item.get('status', {})
        playlist_info.append({
            'playlist_id': item.get('id'),
            'channel_id': snippet.get('channelId'),
            'playlist_title': snippet.get('title'),
            'playlist_description': snippet.get('description'),
            'published_at': snippet.get('publishedAt'),
            'privacy_status': status.get('privacyStatus'),
            'video_count': content_details.get('itemCount')
        })
    return playlist_info

# Function to fetch video ids
def get_video_ids(youtube, playlist_id):
    request = youtube.playlistItems().list(
        part='contentDetails',
        playlistId=playlist_id,
        maxResults=50
    )
    response = request.execute()
    video_ids = [item['contentDetails']['videoId'] for item in response['items']]
    while 'nextPageToken' in response:
        request = youtube.playlistItems().list(
            part='contentDetails',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=response['nextPageToken']
        )
        response = request.execute()
        video_ids.extend([item['contentDetails']['videoId'] for item in response['items']])
    return video_ids

# Function to fetch video details
def get_video_details(youtube, video_ids):
    video_details = []
    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=','.join(video_ids[i:i+50])
        )
        response = request.execute()
        for item in response['items']:
            video_info = {
                'video_id': item['id'],
                'channel_title': item['snippet']['channelTitle'],
                'title': item['snippet']['title'],
                'description': item['snippet']['description'],
                'tags': item['snippet'].get('tags'),
                'published_at': item['snippet']['publishedAt'],
                'view_count': item['statistics']['viewCount'],
                'like_count': item['statistics']['likeCount'],
                'favorite_count': item['statistics']['favoriteCount'],
                'comment_count': item['statistics']['commentCount'],
                'duration': item['contentDetails']['duration'],
                'definition': item['contentDetails']['definition'],
                'caption': item['contentDetails']['caption']
            }
            video_details.append(video_info)
    return video_details

# Function to create table in database
def create_table(connection, table_name, columns):
    cursor = connection.cursor()
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({','.join(columns)})")

# Function to insert data into table
def insert_data(connection, table_name, data):
    cursor = connection.cursor()
    placeholders = ','.join(['%s'] * len(data[0]))
    columns = ','.join(data[0].keys())
    query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    cursor.executemany(query, [tuple(d.values()) for d in data])
    connection.commit()

# Streamlit app
def main():
    st.title("YouTube Data Dashboard")
    channel_id = st.text_input("Enter YouTube Channel ID")
    if st.button("Fetch Data"):
        # Build YouTube API service
        youtube = build('youtube', 'v3', developerKey="YOUR_API_KEY_HERE")
        
        # Fetch channel details
        channel_details = get_channel_details(youtube, channel_id)
        
        # Fetch playlist info
        playlist_info = extract_playlist_info(youtube, channel_details['playlist_id'])
        
        # Fetch video ids
        video_ids = get_video_ids(youtube, channel_details['playlist_id'])
        
        # Fetch video details
        video_details = get_video_details(youtube, video_ids)
        
        # Connect to MySQL database
        connection = pymysql.connect(host='127.0.0.1',user = 'root',passwd="Shashi@007",database='youtube_data')
        
        # Create tables and insert data
        create_table(connection, 'channel', channel_details.keys())
        insert_data(connection, 'channel', [channel_details])
        
        create_table(connection, 'playlist', playlist_info[0].keys())
        insert_data(connection, 'playlist', playlist_info)
        
        create_table(connection, 'video', video_details[0].keys())
        insert_data(connection, 'video', video_details)
        
        connection.close()

if __name__ == "__main__":
    main()

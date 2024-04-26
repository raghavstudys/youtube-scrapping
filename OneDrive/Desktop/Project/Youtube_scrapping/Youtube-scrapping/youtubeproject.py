from googleapiclient.discovery import build
import pandas as pd
import pymysql
import streamlit as st



st.title("Hey this is a youtube data extractor")

a = st.text_input("Enter your channel ID:")

#Basic Details
api_key = "AIzaSyDkW3NmT3ccQqMTaP0k1hrqUl1CYF6eiRs"
channel_id = a
youtube = build('youtube','v3',developerKey = api_key)
my_connection = pymysql.connect(host='127.0.0.1',user = 'root',passwd="Shashi@007",database='youtube_data')

#creating func to extract channel details
def get_channel_details(youtube,channel_id):
    all_data = []
    request = youtube.channels().list(
        part = 'snippet,contentDetails,statistics,status',
        id = channel_id)
    response = request.execute()
    data = dict(channel_id = response['items'][0]['id'],
                channel_name =response['items'][0]['snippet']['title'],
                channel_views =response['items'][0]['statistics']['viewCount'],
                Total_videos = response['items'][0]['statistics']['videoCount'],
                channel_description = response['items'][0]['snippet']['description'],
                channel_status = response['items'][0]['status']['privacyStatus'],
                playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
                )
    all_data.append(data)
    return all_data

    #creating func to extract channel details
def get_channel_details(youtube,channel_id):
    all_data = []
    request = youtube.channels().list(
        part = 'snippet,contentDetails,statistics,status',
        id = channel_id)
    response = request.execute()
    data = dict(channel_id = response['items'][0]['id'],
                channel_name =response['items'][0]['snippet']['title'],
                channel_views =response['items'][0]['statistics']['viewCount'],
                Total_videos = response['items'][0]['statistics']['videoCount'],
                channel_description = response['items'][0]['snippet']['description'],
                channel_status = response['items'][0]['status']['privacyStatus'],
                playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
                )
    all_data.append(data)
    return all_data

channel_temp = get_channel_details(youtube,channel_id)
channel = pd.DataFrame(channel_temp)

st.dataframe(channel)
st.spinner(text="In progress...")

def insert_data_to_mysql(table_name, host='127.0.0.1', user='root', passwd='Shashi@007', database='youtube_data'):
    columns = ",".join(f"{i} Text" for i in channel.columns)
    my_connection = pymysql.connect(host='127.0.0.1',user = 'root',passwd="Shashi@007",database='youtube_data')
    temp = ','.join(['%s']*len(channel.columns))
    query = f"Insert into youtube_data.channel values ({temp}) "
    for i in range(len(channel)):
        my_connection.cursor().execute(query,tuple(channel.iloc[i]))
        my_connection.commit()
    
st.button('Feed to databse',on_click=insert_data_to_mysql(channel, host='127.0.0.1', user='root', passwd='Shashi@007', database='youtube_data'))
st.spinner(text="In progress...")


playlist_id = channel['playlist_id'][0]

def playlist_ext(youtube,playlist_id):
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
            playlist_id = item.get('id')
            channel_id = snippet.get('channelId')
            playlist_title = snippet.get('title')
            playlist_description = snippet.get('description')
            published_at = snippet.get('publishedAt')
            privacy_status = status.get('privacyStatus')
            video_count = content_details.get('itemCount')
            playlist_info.append({
                'playlist_id': playlist_id,
                'channel_id': channel_id,
                'playlist_title': playlist_title,
                'playlist_description': playlist_description,
                'published_at': published_at,
                'privacy_status': privacy_status,
                'video_count': video_count
                })
        return pd.DataFrame(playlist_info)

playlist = playlist_ext(youtube,playlist_id)

st.subheader("Playlist Table")
st.dataframe(playlist)
st.spinner(text="In progress...")

def insert_playlist_to_mysql(table_name, host='127.0.0.1', user='root', passwd='Shashi@007', database='youtube_data'):
    columns = ",".join(f"{i} Text" for i in playlist.columns)
    my_connection = pymysql.connect(host='127.0.0.1',user = 'root',passwd="Shashi@007",database='youtube_data')
    temp = ','.join(['%s']*len(playlist.columns))
    query = f"Insert into youtube_data.playlist values ({temp}) "
    for i in range(len(playlist)):
        my_connection.cursor().execute(query,tuple(playlist.iloc[i]))
        my_connection.commit()
st.button('Feed playlist to database',on_click=insert_playlist_to_mysql(playlist, host='127.0.0.1', user='root', passwd='Shashi@007', database='youtube_data'))
    
#video ids func
def get_video_id_func(youtube,playlist_id):
    
    request = youtube.playlistItems().list(
                part='contentDetails,snippet,status,id',
                playlistId = playlist_id,
                maxResults = 50)
    response = request.execute()
    
    video_ids = []
    
    for i in range(len(response['items'])):
        video_ids.append(response['items'][i]['contentDetails']['videoId'])
    
    next_page_token = response.get('nextPageToken')
    more_pages = True
    
    while more_pages:
        if next_page_token is None:
            more_pages = False
        else:
            request = youtube.playlistItems().list(
                        part='contentDetails',
                        playlistId = playlist_id,
                        maxResults = 50,
                        pageToken = next_page_token)
            response = request.execute()
    
            for i in range(len(response['items'])):
                video_ids.append(response['items'][i]['contentDetails']['videoId'])
        
            next_page_token = response.get('nextPageToken')
            
    return video_ids

video_ids = get_video_id_func(youtube,playlist_id)

def video_details(youtube,video_ids):
    all_video_info = []
    
    for  i in range(0,len(video_ids),50):
        request = youtube.videos().list(
            part = 'snippet,contentDetails,statistics',
            id = ",".join(video_ids[i:i+50])
        )
        response  = request.execute()
    
        for video in response['items']:
            stats_to_keep = {'snippet':['channelId','channelTitle','title','description','publishedAt'],
                             'statistics':['viewCount','likeCount','favouriteCount','commentCount'],
                             'contentDetails':['duration','definition','caption']
                            }
            video_info = {}
            video_info['video']=video['id']

            for k in stats_to_keep.keys():
                for v in stats_to_keep[k]:
                    try:
                        video_info[v] = video[k][v]
                    except:
                        video_info[v] = None

            all_video_info.append(video_info)

    return pd.DataFrame(all_video_info)


video = video_details(youtube,video_ids)

st.subheader("Video details Table")
st.dataframe(video)
st.spinner(text="In progress...")

def insert_video_to_mysql(table_name, host='127.0.0.1', user='root', passwd='Shashi@007', database='youtube_data'):
    my_connection = pymysql.connect(host='127.0.0.1',user = 'root',passwd="Shashi@007",database='youtube_data')
    columns = ",".join(f"{i} Text" for i in video.columns)
    temp = ','.join(['%s']*len(video.columns))
    query = f"Insert into youtube_data.video values ({temp}) "
    for i in range(len(video)):
        my_connection.cursor().execute(query,tuple(video.iloc[i]))
        my_connection.commit()

st.button("Feed Video Details",on_click= insert_video_to_mysql(video, host='127.0.0.1', user='root', passwd='Shashi@007', database='youtube_data'))
st.spinner(text="In progress...")

video_to_play = f"https://www.youtube.com/watch?v={video_ids[0]}"
st.subheader("Recent Video")
st.video(video_to_play, format="video/mp4", start_time=10)
st.snow()
st.success('successfully loaded!', icon="✅")


###################################################################################################################################################


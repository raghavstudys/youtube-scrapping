from googleapiclient.discovery import build
import pandas as pd
import pymysql
import streamlit as st


st.title("QUERIES")

a = st.selectbox(label="FAQ",options=['What are the names of all the videos and their corresponding channels?',
                                      'Which channels have the most number of videos, and how many videos do they have?',
                                      'What are the top 10 most viewed videos and their respective channels?',
                                      'How many comments were made on each video, and what are their corresponding video names?',
                                      'Which videos have the highest number of likes, and what are their corresponding channel names?',
                                      'What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                                      'What is the total number of views for each channel, and what are their corresponding channel names?',
                                      'What are the names of all the channels that have published videos in the year 2022?',
                                      'What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                                      'Which videos have the highest number of comments, and what are their corresponding channel names?'])

#Basic Details
api_key = "AIzaSyDkW3NmT3ccQqMTaP0k1hrqUl1CYF6eiRs"
channel_id = a
youtube = build('youtube','v3',developerKey = api_key)
my_connection = pymysql.connect(host='127.0.0.1',user = 'root',passwd="Shashi@007",database='youtube_data')


if a == 'What are the names of all the videos and their corresponding channels?':
    #sql queries 
    #What are the names of all the videos and their corresponding channels?
    channel_name = '''select video.title,channel.channel_name from youtube_data.channel inner join youtube_data.video on video.channelId = channel.channel_id'''
    names_of_channel = pd.read_sql_query(channel_name,my_connection)
    df_name_of_channels = pd.DataFrame(names_of_channel)
    st.dataframe(df_name_of_channels)

elif a == 'Which channels have the most number of videos, and how many videos do they have?':
    #Which channels have the most number of videos, and how many videos do they have?
    max_count = '''SELECT
        a.channel_name,
        MAX(a.video_count) AS max_video_count
    FROM
        (SELECT
            channel.channel_name,
            COUNT(video.title) AS video_count
        FROM
            youtube_data.channel
        INNER JOIN
            youtube_data.video ON video.channelId = channel.channel_id
        GROUP BY
            1) AS a
    GROUP BY
        1
    ORDER BY
        2 DESC
    LIMIT 1;'''

    most_number_of_video = pd.read_sql_query(max_count,my_connection)
    df_most_num_video = pd.DataFrame(most_number_of_video)
    st.dataframe(df_most_num_video)

elif a == "What are the top 10 most viewed videos and their respective channels?":

    top10videos = '''select title,channelTitle,max(viewCount) as Count from youtube_data.video
    group by 1,2 order by 3 desc LIMIT 10 ;'''

    top = pd.read_sql_query(top10videos,my_connection)
    st.dataframe(top)

elif a == 'Which videos have the highest number of likes, and what are their corresponding channel names?':
    like_count = '''select title,channelTitle,max(likeCount) as Count from youtube_data.video group by 1,2 order by 3 desc LIMIT 1;'''
    like = pd.read_sql_query(like_count,my_connection)
    df_like = pd.DataFrame(like)
    st.dataframe(df_like)

elif a == 'What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
    total_like = '''select title,channelTitle,sum(likeCount) as Count from youtube_data.video group by 1,2 order by 3 desc ;'''
    likes = pd.read_sql_query(total_like,my_connection)
    df_likes = pd.DataFrame(likes)
    st.dataframe(df_likes)

elif a == 'What is the total number of views for each channel, and what are their corresponding channel names?':
    total_view = '''select channelTitle,concat(sum(viewCount)/1000," ","K") as Count from youtube_data.video
    group by 1 order by 2 desc ;'''
    total_views = pd.read_sql_query(total_view,my_connection)
    df_total_views = pd.DataFrame(total_views)
    st.dataframe(df_total_views)

elif a == 'What are the names of all the channels that have published videos in the year 2022?':

    publishedYear = '''select channelTitle from (select channelTitle,count(year_) from(select channelTitle, year(publishedAt) as year_ from youtube_data.video having year_ =2022) as a group by 1 having count(year_) >1) as a;'''
    published = pd.read_sql_query(publishedYear,my_connection)
    df_published = pd.DataFrame(published)
    st.dataframe(df_published)

elif a == 'Which videos have the highest number of comments, and what are their corresponding channel names?':

    comment = '''select channelTitle,max(commentCount) as max_comment from youtube_data.video group by 1 order by 2 desc limit 1;'''
    comment = pd.read_sql_query(comment,my_connection)
    df_comment = pd.DataFrame(comment)
    st.dataframe(df_comment)

elif a == 'How many comments were made on each video, and what are their corresponding video names?':
    comments = '''select title,sum(commentCount) as comment_count from youtube_data.video group by 1 ;'''
    a = pd.read_sql_query(comments,my_connection)
    df_comments_ = pd.DataFrame(a)
    st.dataframe(df_comments_)
else:
    print("Sorry we're working on it")
    
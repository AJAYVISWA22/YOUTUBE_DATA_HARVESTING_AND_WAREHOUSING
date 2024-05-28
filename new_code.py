from googleapiclient.discovery import build
import psycopg2
import pandas as pd
import streamlit as st
import time

#AIzaSyC1Jd4I9OeaDOfQyKFYtKNcViKhp6VpXEs

# API key connection
def api_connect():
    api_key = "YOUR_API_KEY"
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey=api_key)
    return youtube

youtube = api_connect()

# Get channel information
def get_channel_info(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()
    for i in response['items']:
        data = dict(
            Channel_Name=i["snippet"]["title"],
            Channel_Id=i["id"],
            Subscribers=i['statistics']['subscriberCount'],
            Views=i["statistics"]["viewCount"],
            Total_Videos=i["statistics"]["videoCount"],
            Channel_Description=i["snippet"]["description"],
            Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"]
        )
    return data

# Get video IDs
def get_videos_ids(channel_id):
    video_ids = []
    response = youtube.channels().list(id=channel_id, part='contentDetails').execute()
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None
    while True:
        response1 = youtube.playlistItems().list(
            part='snippet',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response1.get('nextPageToken')
        if next_page_token is None:
            break
    return video_ids

# Get video information
def get_video_info(video_ids):
    video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()
        for item in response["items"]:
            data = dict(
                Channel_Name=item['snippet']['channelTitle'],
                Channel_Id=item['snippet']['channelId'],
                Video_Id=item['id'],
                Title=item['snippet']['title'],
                Tags=item['snippet'].get('tags'),
                Thumbnail=item['snippet']['thumbnails']['default']['url'],
                Description=item['snippet'].get('description'),
                Published_Date=item['snippet']['publishedAt'],
                Duration=item['contentDetails']['duration'],
                Views=item['statistics'].get('viewCount'),
                Likes=item['statistics'].get('likeCount'),
                Comments=item['statistics'].get('commentCount'),
                Favorite_Count=item['statistics']['favoriteCount'],
                Definition=item['contentDetails']['definition'],
                Caption_Status=item['contentDetails']['caption']
            )
            video_data.append(data)
    return video_data

# Get comment information
def get_comment_info(video_ids):
    comment_data = []
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response = request.execute()
            for item in response['items']:
                data = dict(
                    Comment_Id=item['snippet']['topLevelComment']['id'],
                    Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                    Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt']
                )
                comment_data.append(data)
    except Exception as e:
        st.error(f"Error fetching comments: {e}")
    return comment_data

# Get playlist details
def get_playlist_details(channel_id):
    next_page_token = None
    all_data = []
    while True:
        request = youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()
        for item in response['items']:
            data = dict(
                Playlist_Id=item['id'],
                Title=item['snippet']['title'],
                Channel_Id=item['snippet']['channelId'],
                Channel_Name=item['snippet']['channelTitle'],
                PublishedAt=item['snippet']['publishedAt'],
                Video_Count=item['contentDetails']['itemCount']
            )
            all_data.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break
    return all_data

# Function to create a connection to PostgreSQL
def create_connection():
    return psycopg2.connect(
        host="localhost",
        user="postgres",
        password="Ajay",
        database="youtube_data",
        port="5432"
    )

# Function to create and populate the channels table
def channels_table(channel_data):
    mydb = create_connection()
    cursor = mydb.cursor()
    try:
        create_query = '''CREATE TABLE IF NOT EXISTS channels (
            Channel_Name VARCHAR(100),
            Channel_Id VARCHAR(80) PRIMARY KEY,
            Subscribers BIGINT,
            Views BIGINT,
            Total_Videos INT,
            Channel_Description TEXT,
            Playlist_Id VARCHAR(80)
        )'''
        cursor.execute(create_query)
        mydb.commit()
    except Exception as e:
        st.error(f"Error creating channels table: {e}")

    cursor.execute("SELECT Channel_Id FROM channels WHERE Channel_Id = %s", (channel_data["Channel_Id"],))
    if cursor.fetchone():
        return f"Channel {channel_data['Channel_Name']} already exists in the database."
    else:
        insert_query = '''INSERT INTO channels (
            Channel_Name, Channel_Id, Subscribers, Views, Total_Videos, Channel_Description, Playlist_Id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)'''
        values = (
            channel_data['Channel_Name'],
            channel_data['Channel_Id'],
            channel_data['Subscribers'],
            channel_data['Views'],
            channel_data['Total_Videos'],
            channel_data['Channel_Description'],
            channel_data['Playlist_Id']
        )
        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except Exception as e:
            st.error(f"Error inserting into channels: {e}")

# Function to create and populate the playlists table
def playlist_table(playlists_data):
    mydb = create_connection()
    cursor = mydb.cursor()
    try:
        create_query = '''CREATE TABLE IF NOT EXISTS playlists (
            Playlist_Id VARCHAR(100) PRIMARY KEY,
            Title VARCHAR(100),
            Channel_Id VARCHAR(100),
            Channel_Name VARCHAR(100),
            PublishedAt TIMESTAMP,
            Video_Count INT
        )'''
        cursor.execute(create_query)
        mydb.commit()
    except Exception as e:
        st.error(f"Error creating playlists table: {e}")

    for data in playlists_data:
        cursor.execute("SELECT Playlist_Id FROM playlists WHERE Playlist_Id = %s", (data["Playlist_Id"],))
        if cursor.fetchone():
            continue
        insert_query = '''INSERT INTO playlists (
            Playlist_Id, Title, Channel_Id, Channel_Name, PublishedAt, Video_Count
        ) VALUES (%s, %s, %s, %s, %s, %s)'''
        values = (
            data['Playlist_Id'],
            data['Title'],
            data['Channel_Id'],
            data['Channel_Name'],
            data['PublishedAt'],
            data['Video_Count']
        )
        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except Exception as e:
            st.error(f"Error inserting into playlists: {e}")

# Function to create and populate the videos table
def videos_table(videos_data):
    mydb = create_connection()
    cursor = mydb.cursor()
    try:
        create_query = '''CREATE TABLE IF NOT EXISTS videos (
            Channel_Name VARCHAR(100),
            Channel_Id VARCHAR(100),
            Video_Id VARCHAR(30) PRIMARY KEY,
            Title VARCHAR(150),
            Tags TEXT,
            Thumbnail VARCHAR(200),
            Description TEXT,
            Published_Date TIMESTAMP,
            Duration INTERVAL,
            Views BIGINT,
            Likes BIGINT,
            Comments INT,
            Favorite_Count INT,
            Definition VARCHAR(10),
            Caption_Status VARCHAR(50)
        )'''
        cursor.execute(create_query)
        mydb.commit()
    except Exception as e:
        st.error(f"Error creating videos table: {e}")

    for data in videos_data:
        cursor.execute("SELECT Video_Id FROM videos WHERE Video_Id = %s", (data["Video_Id"],))
        if cursor.fetchone():
            continue
        insert_query = '''INSERT INTO videos (
            Channel_Name, Channel_Id, Video_Id, Title, Tags, Thumbnail, Description, Published_Date, Duration, Views,
            Likes, Comments, Favorite_Count, Definition, Caption_Status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
        values = (
            data['Channel_Name'],
            data['Channel_Id'],
            data['Video_Id'],
            data['Title'],
            data.get('Tags'),
            data['Thumbnail'],
            data.get('Description'),
            data['Published_Date'],
            data['Duration'],
            data.get('Views'),
            data.get('Likes'),
            data.get('Comments'),
            data['Favorite_Count'],
            data['Definition'],
            data['Caption_Status']
        )
        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except Exception as e:
            st.error(f"Error inserting into videos: {e}")

# Function to create and populate the comments table
def comments_table(comments_data):
    mydb = create_connection()
    cursor = mydb.cursor()
    try:
        create_query = '''CREATE TABLE IF NOT EXISTS comments (
            Comment_Id VARCHAR(100) PRIMARY KEY,
            Video_Id VARCHAR(100),
            Comment_Text TEXT,
            Comment_Author VARCHAR(100),
            Comment_Published TIMESTAMP
        )'''
        cursor.execute(create_query)
        mydb.commit()
    except Exception as e:
        st.error(f"Error creating comments table: {e}")

    for data in comments_data:
        cursor.execute("SELECT Comment_Id FROM comments WHERE Comment_Id = %s", (data["Comment_Id"],))
        if cursor.fetchone():
            continue
        insert_query = '''INSERT INTO comments (
            Comment_Id, Video_Id, Comment_Text, Comment_Author, Comment_Published
        ) VALUES (%s, %s, %s, %s, %s)'''
        values = (
            data['Comment_Id'],
            data['Video_Id'],
            data['Comment_Text'],
            data['Comment_Author'],
            data['Comment_Published']
        )
        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except Exception as e:
            st.error(f"Error inserting into comments: {e}")

# Function to delete channel details from the database
def delete_channel_details(channel_id):
    mydb = create_connection()
    cursor = mydb.cursor()
    try:
        cursor.execute("DELETE FROM comments WHERE Video_Id IN (SELECT Video_Id FROM videos WHERE Channel_Id = %s)", (channel_id,))
        cursor.execute("DELETE FROM videos WHERE Channel_Id = %s", (channel_id,))
        cursor.execute("DELETE FROM playlists WHERE Channel_Id = %s", (channel_id,))
        cursor.execute("DELETE FROM channels WHERE Channel_Id = %s", (channel_id,))
        mydb.commit()
        return f"Channel data with ID {channel_id} has been deleted from the database."
    except Exception as e:
        st.error(f"Error deleting channel data: {e}")

# Function to create and populate all tables
def tables(channel_id):
    channel_data = get_channel_info(channel_id)
    video_ids = get_videos_ids(channel_id)
    video_data = get_video_info(video_ids)
    comment_data = get_comment_info(video_ids)
    playlist_data = get_playlist_details(channel_id)

    channels_table(channel_data)
    playlist_table(playlist_data)
    videos_table(video_data)
    comments_table(comment_data)

    return "Tables Created Successfully"

# Functions to display tables
def show_channels_table():
    mydb = create_connection()
    cursor = mydb.cursor()
    cursor.execute("SELECT * FROM channels")
    channels = cursor.fetchall()
    df = pd.DataFrame(channels, columns=["Channel_Name", "Channel_Id", "Subscribers", "Views", "Total_Videos", "Channel_Description", "Playlist_Id"])
    st.dataframe(df)

def show_playlists_table():
    mydb = create_connection()
    cursor = mydb.cursor()
    cursor.execute("SELECT * FROM playlists")
    playlists = cursor.fetchall()
    df = pd.DataFrame(playlists, columns=["Playlist_Id", "Title", "Channel_Id", "Channel_Name", "PublishedAt", "Video_Count"])
    st.dataframe(df)

def show_videos_table():
    mydb = create_connection()
    cursor = mydb.cursor()
    cursor.execute("SELECT * FROM videos")
    videos = cursor.fetchall()
    df = pd.DataFrame(videos, columns=["Channel_Name", "Channel_Id", "Video_Id", "Title", "Tags", "Thumbnail", "Description", "Published_Date", "Duration", "Views", "Likes", "Comments", "Favorite_Count", "Definition", "Caption_Status"])
    st.dataframe(df)

def show_comments_table():
    mydb = create_connection()
    cursor = mydb.cursor()
    cursor.execute("SELECT * FROM comments")
    comments = cursor.fetchall()
    df = pd.DataFrame(comments, columns=["Comment_Id", "Video_Id", "Comment_Text", "Comment_Author", "Comment_Published"])
    st.dataframe(df)

# Streamlit Interface
with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skill Take Away")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("PostgreSQL")
    st.caption("API Integration")
    st.caption("Data Management using SQL")

channel_id = st.text_input("Enter the channel ID")

if st.button("Collect and Store Data"):
    mydb = create_connection()
    cursor = mydb.cursor()
    cursor.execute("SELECT Channel_Id FROM channels")
    existing_channels = [row[0] for row in cursor.fetchall()]

    if channel_id in existing_channels:
        st.success("Channel Details of the given channel id already exist")
    else:
        tables(channel_id)
        st.success("Data collected and stored successfully")

# Select Channel
mydb = create_connection()
cursor = mydb.cursor()
cursor.execute("SELECT Channel_Name FROM channels")
all_channels = [row[0] for row in cursor.fetchall()]

unique_channel = st.selectbox("Select the Channel", all_channels)

if st.button("Migrate to SQL"):
    tables(unique_channel)
    st.success("Tables Created Successfully")

# Show Tables
show_table = st.radio("SELECT THE TABLE FOR VIEW", ("CHANNELS", "PLAYLISTS", "VIDEOS", "COMMENTS"))

if show_table == "CHANNELS":
    show_channels_table()
elif show_table == "PLAYLISTS":
    show_playlists_table()
elif show_table == "VIDEOS":
    show_videos_table()
elif show_table == "COMMENTS":
    show_comments_table()

# SQL Queries
question = st.selectbox("Select your question", (
    "1. All the videos and the channel name",
    "2. Channels with most number of videos",
    "3. 10 most viewed videos",
    "4. Comments in each video",
    "5. Videos with highest likes",
    "6. Likes of all videos",
    "7. Views of each channel",
    "8. Videos published in the year of 2022",
    "9. Average duration of all videos in each channel",
    "10. Videos with highest number of comments"
))

if question == "1. All the videos and the channel name":
    query1 = '''SELECT Title as videos, Channel_Name as channelname FROM videos'''
    cursor.execute(query1)
    mydb.commit()
    t1 = cursor.fetchall()
    df = pd.DataFrame(t1, columns=["video title", "channel name"])
    st.write(df)

elif question == "2. Channels with most number of videos":
    query2 = '''SELECT Channel_Name as channelname, Total_Videos as no_videos FROM channels ORDER BY Total_Videos DESC'''
    cursor.execute(query2)
    mydb.commit()
    t2 = cursor.fetchall()
    df2 = pd.DataFrame(t2, columns=["channel name", "No of videos"])
    st.write(df2)

elif question == "3. 10 most viewed videos":
    query3 = '''SELECT Views as views, Channel_Name as channelname, Title as videotitle FROM videos WHERE Views IS NOT NULL ORDER BY Views DESC LIMIT 10'''
    cursor.execute(query3)
    mydb.commit()
    t3 = cursor.fetchall()
    df3 = pd.DataFrame(t3, columns=["views", "channel name", "videotitle"])
    st.write(df3)

elif question == "4. Comments in each video":
    query4 = '''SELECT Comments as no_comments, Title as videotitle FROM videos WHERE Comments IS NOT NULL'''
    cursor.execute(query4)
    mydb.commit()
    t4 = cursor.fetchall()
    df4 = pd.DataFrame(t4, columns=["no of comments", "videotitle"])
    st.write(df4)

elif question == "5. Videos with highest likes":
    query5 = '''SELECT Title as videotitle, Channel_Name as channelname, Likes as likecount FROM videos WHERE Likes IS NOT NULL ORDER BY Likes DESC'''
    cursor.execute(query5)
    mydb.commit()
    t5 = cursor.fetchall()
    df5 = pd.DataFrame(t5, columns=["videotitle", "channelname", "likecount"])
    st.write(df5)

elif question == "6. Likes of all videos":
    query6 = '''SELECT Likes as likecount, Title as videotitle FROM videos'''
    cursor.execute(query6)
    mydb.commit()
    t6 = cursor.fetchall()
    df6 = pd.DataFrame(t6, columns=["likecount", "videotitle"])
    st.write(df6)

elif question == "7. Views of each channel":
    query7 = '''SELECT Channel_Name as channelname, Views as totalviews FROM channels'''
    cursor.execute(query7)
    mydb.commit()
    t7 = cursor.fetchall()
    df7 = pd.DataFrame(t7, columns=["channel name", "totalviews"])
    st.write(df7)

elif question == "8. Videos published in the year of 2022":
    query8 = '''SELECT Title as video_title, Published_Date as videorelease, Channel_Name as channelname FROM videos WHERE EXTRACT(YEAR FROM Published_Date)=2022'''
    cursor.execute(query8)
    mydb.commit()
    t8 = cursor.fetchall()
    df8 = pd.DataFrame(t8, columns=["videotitle", "published_date", "channelname"])
    st.write(df8)

elif question == "9. Average duration of all videos in each channel":
    query9 = '''SELECT Channel_Name as channelname, AVG(Duration) as averageduration FROM videos GROUP BY Channel_Name'''
    cursor.execute(query9)
    mydb.commit()
    t9 = cursor.fetchall()
    df9 = pd.DataFrame(t9, columns=["channelname", "averageduration"])

    T9 = []
    for index, row in df9.iterrows():
        channel_title = row["channelname"]
        average_duration = row["averageduration"]
        average_duration_str = str(average_duration)
        T9.append(dict(channeltitle=channel_title, avgduration=average_duration_str))
    df1 = pd.DataFrame(T9)
    st.write(df1)

elif question == "10. Videos with highest number of comments":
    query10 = '''SELECT Title as videotitle, Channel_Name as channelname, Comments as comments FROM videos WHERE Comments IS NOT NULL ORDER BY Comments DESC'''
    cursor.execute(query10)
    mydb.commit()
    t10 = cursor.fetchall()
    df10 = pd.DataFrame(t10, columns=["video title", "channel name", "comments"])
    st.write(df10)

# Function to delete channel details from the database (added in SQL-only version)
def delete_channel_details(channel_id):
    mydb = create_connection()
    cursor = mydb.cursor()
    try:
        cursor.execute("DELETE FROM comments WHERE Video_Id IN (SELECT Video_Id FROM videos WHERE Channel_Id = %s)", (channel_id,))
        cursor.execute("DELETE FROM videos WHERE Channel_Id = %s", (channel_id,))
        cursor.execute("DELETE FROM playlists WHERE Channel_Id = %s", (channel_id,))
        cursor.execute("DELETE FROM channels WHERE Channel_Id = %s", (channel_id,))
        mydb.commit()
        return f"Channel data with ID {channel_id} has been deleted from the database."
    except Exception as e:
        st.error(f"Error deleting channel data: {e}")

if st.button("Delete Channel Data"):
    delete_result = delete_channel_details(channel_id)
    st.success(delete_result)

    

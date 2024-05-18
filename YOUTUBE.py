from googleapiclient.discovery import build
import psycopg2
import pandas as pd
import streamlit as st
import time


# API key connection
def Api_connect():
    Api_Id = "AIzaSyC1Jd4I9OeaDOfQyKFYtKNcViKhp6VpXEs"
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey=Api_Id)
    return youtube

youtube = Api_connect()

# Database connection
def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        user="postgres",
        password="Ajay",
        database="DATAS",  # Ensure this matches your actual database name
        port="5432"
    )


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
            Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"],
            Thumbnail=i["snippet"]["thumbnails"]["medium"]["url"]
        )
    return data

# Get video ids
def get_videos_ids(channel_id):
    video_ids = []
    response = youtube.channels().list(id=channel_id, part='contentDetails').execute()
    Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    while True:
        response1 = youtube.playlistItems().list(
            part='snippet',
            playlistId=Playlist_Id,
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
    Comment_data = []
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
                Comment_data.append(data)
    except:
        pass
    return Comment_data

# Get playlist details
def get_playlist_details(channel_id):
    next_page_token = None
    All_data = []
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
            All_data.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break
    return All_data


# Function to display the channels table
def show_channels_table(channel_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = "SELECT * FROM channels WHERE Channel_Name = %s"
    cursor.execute(query, (channel_name,))
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=["Channel_Name","Channel_Id", "Total_Videos", "Subscribers", "Views","Channel_Description","Playlist_Id","Thumbnail"])
    st.write(df)

# Function to display the playlists table
def show_playlists_table(channel_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = "SELECT * FROM playlists WHERE Channel_Id IN (SELECT Channel_Id FROM channels WHERE Channel_Name = %s)"
    cursor.execute(query, (channel_name,))
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=["Playlist_Id","title", "Channel_Id","Channel_Name","Publish_date", "Total_Videos"])
    st.write(df)

# Function to display the videos table
def show_videos_table(channel_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = "SELECT * FROM videos WHERE Channel_Id IN (SELECT Channel_Id FROM channels WHERE Channel_Name = %s)"
    cursor.execute(query, (channel_name,))
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=["Channel_name","Channel_Id","Video_Id","Title","Tags","Thumbnails","Description","Published_Date","Duration", "Views", "Likes", "Comments","Favorites","Definition","Caption_status"])
    st.write(df)

# Function to display the comments table
def show_comments_table(channel_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = "SELECT * FROM comments WHERE Video_Id IN (SELECT Video_Id FROM videos WHERE Channel_Id IN (SELECT Channel_Id FROM channels WHERE Channel_Name = %s))"
    cursor.execute(query, (channel_name,))
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=["Comment_Id", "Video_Id", "Comment_Text", "Author", "Published_Date"])
    st.write(df)



# Insert data into PostgreSQL
def channel_details(channel_id):
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_details(channel_id)
    vi_ids = get_videos_ids(channel_id)
    vi_details = get_video_info(vi_ids)
    com_details = get_comment_info(vi_ids)
    
    #thumbnail=get_channel_thumbnail(channel_name)

    conn = get_db_connection()
    cursor = conn.cursor()

    # Insert channel details
    insert_channel_query = '''INSERT INTO channels (Channel_Name, Channel_Id, Subscribers, Views, Total_Videos, Channel_Description, Playlist_Id,Thumbnail)
                              VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (Channel_Id) DO NOTHING'''
    cursor.execute(insert_channel_query, (
        ch_details['Channel_Name'], ch_details['Channel_Id'], ch_details['Subscribers'],
        ch_details['Views'], ch_details['Total_Videos'], ch_details['Channel_Description'], ch_details['Playlist_Id'] ,ch_details['Thumbnail'] 
    ))
    
    # Insert playlist details
    insert_playlist_query = '''INSERT INTO playlists (Playlist_Id, Title, Channel_Id, Channel_Name, PublishedAt, Video_Count)
                               VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (Playlist_Id) DO NOTHING'''
    for pl in pl_details:
        cursor.execute(insert_playlist_query, (
            pl['Playlist_Id'], pl['Title'], pl['Channel_Id'], pl['Channel_Name'], pl['PublishedAt'], pl['Video_Count']
        ))

    # Insert video details
    insert_video_query = '''INSERT INTO videos (Channel_Name, Channel_Id, Video_Id, Title, Tags, Thumbnail, Description, Published_Date, Duration, Views, Likes, Comments, Favorite_Count, Definition, Caption_Status)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (Video_Id) DO NOTHING'''
    for vi in vi_details:
        cursor.execute(insert_video_query, (
            vi['Channel_Name'], vi['Channel_Id'], vi['Video_Id'], vi['Title'], vi['Tags'], vi['Thumbnail'], vi['Description'],
            vi['Published_Date'], vi['Duration'], vi['Views'], vi['Likes'], vi['Comments'], vi['Favorite_Count'],
            vi['Definition'], vi['Caption_Status']
        ))

    # Insert comment details
    insert_comment_query = '''INSERT INTO comments (Comment_Id, Video_Id, Comment_Text, Comment_Author, Comment_Published)
                              VALUES (%s, %s, %s, %s, %s) ON CONFLICT (Comment_Id) DO NOTHING'''
    for com in com_details:
        cursor.execute(insert_comment_query, (
            com['Comment_Id'], com['Video_Id'], com['Comment_Text'], com['Comment_Author'], com['Comment_Published']
        ))

    conn.commit()
    cursor.close()
    conn.close()

    return "Upload completed successfully"

# Table creation for channels, playlists, videos, comments
def channels_table():
    conn = get_db_connection()
    cursor = conn.cursor()
    create_query = '''CREATE TABLE IF NOT EXISTS channels (
                        Channel_Name VARCHAR(100),
                        Channel_Id VARCHAR(80) PRIMARY KEY,
                        Subscribers BIGINT,
                        Views BIGINT,
                        Total_Videos INT,
                        Channel_Description TEXT,
                        Playlist_Id VARCHAR(80),
                        Thumbnail TEXT
                      )'''
    cursor.execute(create_query)
    conn.commit()
    cursor.close()
    conn.close()

def playlist_table():
    conn = get_db_connection()
    cursor = conn.cursor()
    create_query = '''CREATE TABLE IF NOT EXISTS playlists (
                        Playlist_Id VARCHAR(100) PRIMARY KEY,
                        Title VARCHAR(100),
                        Channel_Id VARCHAR(100),
                        Channel_Name VARCHAR(100),
                        PublishedAt TIMESTAMP,
                        Video_Count INT
                      )'''
    cursor.execute(create_query)
    conn.commit()
    cursor.close()
    conn.close()

def videos_table():
    conn = get_db_connection()
    cursor = conn.cursor()
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
                        Definition VARCHAR(50),
                        Caption_Status VARCHAR(50)
                      )'''
    cursor.execute(create_query)
    conn.commit()
    cursor.close()
    conn.close()

def comments_table():
    conn = get_db_connection()
    cursor = conn.cursor()
    create_query = '''CREATE TABLE IF NOT EXISTS comments (
                        Comment_Id VARCHAR(50) PRIMARY KEY,
                        Video_Id VARCHAR(30),
                        Comment_Text TEXT,
                        Comment_Author VARCHAR(100),
                        Comment_Published TIMESTAMP
                      )'''
    cursor.execute(create_query)
    conn.commit()
    cursor.close()
    conn.close()

def create_all_tables():
    channels_table()
    playlist_table()
    videos_table()
    comments_table()

 # Function to delete channel details from the database (added in SQL-only version)
def delete_channel_details(channel_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Fetch the channel_id based on channel_name
        cursor.execute("SELECT Channel_Id FROM channels WHERE Channel_Name = %s", (channel_name,))
        channel_id = cursor.fetchone()
        if not channel_id:
            return f"Channel with name {channel_name} does not exist in the database."
        channel_id = channel_id[0]

        cursor.execute("DELETE FROM comments WHERE Video_Id IN (SELECT Video_Id FROM videos WHERE Channel_Id = %s)", (channel_id,))
        cursor.execute("DELETE FROM videos WHERE Channel_Id = %s", (channel_id,))
        cursor.execute("DELETE FROM playlists WHERE Channel_Id = %s", (channel_id,))
        cursor.execute("DELETE FROM channels WHERE Channel_Id = %s", (channel_id,))
        conn.commit()
        return f"Channel data with name {channel_name} has been deleted from the database."
    except Exception as e:
        st.error(f"Error deleting channel data: {e}")
    
    cursor.close()
    conn.close()

    

def get_channel_thumbnail(channel_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = "SELECT Thumbnail FROM channels WHERE Channel_Name = %s"
    cursor.execute(query, (channel_name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    return None


# Streamlit
st.set_page_config(page_title="YouTube Data Harvesting", page_icon="📺", layout="wide")
st.title("YouTube Data Harvesting")
menu = ["Home","Add/Remove", "Search", "Analysis"]
choice = st.sidebar.selectbox("Menu", menu)
create_all_tables()


def for_choice_home():
    st.write("Welcome to the YouTube Data Harvesting app. Navigate to the 'menu' to begin.")

def for_choice_Add_Remove():
    st.subheader("Add or Remove")
    channel_id = st.text_input("Enter Channel ID")
    conn = get_db_connection()
    cursor = conn.cursor()
    if st.button("Get Data"):
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT Channel_Id FROM channels WHERE Channel_Id = %s", (channel_id,))
        existing_channel = cursor.fetchone()
        if existing_channel:
            st.success("Channel details of the given channel ID already exist.")
        else:
            result = channel_details(channel_id)
            st.success(result)   
    
    cursor.execute("SELECT Channel_Name FROM channels")
    all_channels = [row[0] for row in cursor.fetchall()]
    unique_channel = st.selectbox("Uploaded Channels", all_channels)


    channel_name = st.text_input("Enter Channel Name")
    if st.button("Delete Channel Data"):
        delete_result = delete_channel_details(channel_name)
        st.success(delete_result)

        cursor.close()
        conn.close()

def for_choice_Search():
    st.subheader("Data of YouTube Channel")
    st.write("Select the channel to get their data")

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT Channel_Name FROM channels")
    all_channels = [row[0] for row in cursor.fetchall()]
    unique_channel = st.selectbox("Select the Channel", all_channels)

    col1, col2 = st.columns(2)

    with col1:
        show_table = st.radio("SELECT THE TABLE FOR VIEW", ("CHANNELS", "PLAYLISTS", "VIDEOS", "COMMENTS"))

    with col2:
        if unique_channel:
            channel_thumbnail = get_channel_thumbnail(unique_channel)
            if channel_thumbnail:
                st.image(channel_thumbnail, caption=unique_channel, width=240)


    if show_table == "CHANNELS":
        show_channels_table(unique_channel)
    elif show_table == "PLAYLISTS":
        show_playlists_table(unique_channel)
    elif show_table == "VIDEOS":
        show_videos_table(unique_channel)
    elif show_table == "COMMENTS":
        show_comments_table(unique_channel)

    cursor.close()
    conn.close()

def for_choice_Analysis():
    st.subheader("Analysis of YouTube Data")
    st.write("Perform analysis on the harvested data here.")
    
    
    conn = get_db_connection()
    cursor = conn.cursor()

    question = st.selectbox("Select your question",(
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
        query1 = '''SELECT title AS videos, channel_name AS channelname FROM videos'''
        cursor.execute(query1)
        t1 = cursor.fetchall()
        df = pd.DataFrame(t1, columns=["Video Title", "Channel Name"])
        st.write(df)

    elif question == "2. Channels with most number of videos":
        query2 = '''SELECT channel_name AS channelname, total_videos AS no_videos FROM channels 
                ORDER BY total_videos DESC'''
        cursor.execute(query2)
        t2 = cursor.fetchall()
        df2 = pd.DataFrame(t2, columns=["Channel Name", "No of Videos"])
        st.write(df2)

    elif question == "3. 10 most viewed videos":
        query3 = '''SELECT views AS views, channel_name AS channelname, title AS videotitle FROM videos 
                    WHERE views IS NOT NULL ORDER BY views DESC LIMIT 10'''
        cursor.execute(query3)
        t3 = cursor.fetchall()
        df3 = pd.DataFrame(t3, columns=["Views", "Channel Name", "Video Title"])
        st.write(df3)

    elif question == "4. Comments in each video":
        query4 = '''SELECT comments AS no_comments, title AS videotitle FROM videos WHERE comments IS NOT NULL'''
        cursor.execute(query4)
        t4 = cursor.fetchall()
        df4 = pd.DataFrame(t4, columns=["No of Comments", "Video Title"])
        st.write(df4)

    elif question == "5. Videos with highest likes":
        query5 = '''SELECT title AS videotitle, channel_name AS channelname, likes AS likecount
                    FROM videos WHERE likes IS NOT NULL ORDER BY likes DESC'''
        cursor.execute(query5)
        t5 = cursor.fetchall()
        df5 = pd.DataFrame(t5, columns=["Video Title", "Channel Name", "Like Count"])
        st.write(df5)

    elif question == "6. Likes of all videos":
        query6 = '''SELECT likes AS likecount, title AS videotitle FROM videos'''
        cursor.execute(query6)
        t6 = cursor.fetchall()
        df6 = pd.DataFrame(t6, columns=["Like Count", "Video Title"])
        st.write(df6)

    elif question == "7. Views of each channel":
        query7 = '''SELECT channel_name AS channelname, views AS totalviews FROM channels'''
        cursor.execute(query7)
        t7 = cursor.fetchall()
        df7 = pd.DataFrame(t7, columns=["Channel Name", "Total Views"])
        st.write(df7)

    elif question == "8. Videos published in the year of 2022":
        query8 = '''SELECT title AS video_title, published_date AS videorelease, channel_name AS channelname FROM videos
                    WHERE EXTRACT(YEAR FROM published_date) = 2022'''
        cursor.execute(query8)
        t8 = cursor.fetchall()
        df8 = pd.DataFrame(t8, columns=["Video Title", "Published Date", "Channel Name"])
        st.write(df8)

    elif question == "9. Average duration of all videos in each channel":
        query9 = '''SELECT channel_name AS channelname, AVG(duration) AS averageduration FROM videos GROUP BY channel_name'''
        cursor.execute(query9)
        t9 = cursor.fetchall()
        df9 = pd.DataFrame(t9, columns=["Channel Name", "Average Duration"])
        st.write(df9)

    elif question == "10. Videos with highest number of comments":
        query10 = '''SELECT title AS videotitle, channel_name AS channelname, comments AS comments FROM videos WHERE comments IS
                    NOT NULL ORDER BY comments DESC'''
        cursor.execute(query10)
        t10 = cursor.fetchall()
        df10 = pd.DataFrame(t10, columns=["Video Title", "Channel Name", "Comments"])
        st.write(df10)

    cursor.close()
    conn.close()
    
def choices(choice):
    if choice == "Home":
        for_choice_home()
    
    elif choice == "Add/Remove":
        for_choice_Add_Remove()

    elif choice=="Search":
        for_choice_Search()
    
    elif choice == "Analysis":
        for_choice_Analysis()

choices(choice)
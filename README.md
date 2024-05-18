# YouTube Data Harvesting Application

## Overview
This YouTube Data Harvesting application allows users to collect, store, view, and analyze data from YouTube channels. It is built using Streamlit for the frontend interface and uses the YouTube Data API to fetch data. The data is stored in a PostgreSQL database.

## Features
1. **Add/Remove YouTube Channels**: Users can add new channels to the database or remove existing channels.
2. **Search**: Users can search and view data from the database including channels, playlists, videos, and comments.
3. **Analysis**: Provides various analytical insights based on the harvested data.

## Installation

### Prerequisites
- Python
- PostgreSQL
- Streamlit
- Google API Client for Python
- pandas
- psycopg2

### Setup

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/yourusername/yourrepository.git
   cd yourrepository
   ```

2. **Install Dependencies**:
   ```bash
   pip install streamlit pandas google-api-python-client psycopg2
   ```

3. **Database Setup**:
   Ensure that PostgreSQL is installed and running. Create a database named `DATAS` and configure the database connection in the script if necessary.

4. **API Key Setup**:
   Obtain a YouTube Data API v3 key from the Google Developer Console and replace the `Api_Id` in the `Api_connect()` function.

## Running the Application
Run the following command in your terminal to start the Streamlit application:
```bash
streamlit run yourscript.py
```

## Code Explanation

### 1. API and Database Connection
```python
from googleapiclient.discovery import build
import psycopg2
import pandas as pd
import streamlit as st

def Api_connect():
    Api_Id = "YOUR_API_KEY"
    api_service_name = "youtube"
    api_version = "v3"
    return build(api_service_name, api_version, developerKey=Api_Id)

def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        user="postgres",
        password="Ajay",
        database="DATAS",
        port="5432"
    )
```
This section sets up connections to the YouTube Data API and PostgreSQL database.

### 2. Fetching YouTube Data
Functions to fetch channel, video, and comment data from YouTube.

### 3. Inserting Data into PostgreSQL
Functions to create tables and insert fetched data into the PostgreSQL database.

### 4. Streamlit Interface
Streamlit functions to create a user interface for adding/removing channels, searching data, and performing analysis.

### 5. Streamlit Menu
The main function that controls the menu and directs to respective functionalities.

## Using the Application

### Home
Displays a welcome message.

### Add/Remove
Allows users to add a new channel to the database by entering the channel ID or remove an existing channel by entering the channel name.

### Search
Users can select a channel and view data from the channels, playlists, videos, or comments tables.

### Analysis
Provides analytical insights such as:
1. All videos and their channel names.
2. Channels with the most number of videos.
3. Top 10 most viewed videos.
4. Comments in each video.
5. Videos with the highest likes.
6. Likes of all videos.
7. Views of each channel.
8. Videos published in 2022.
9. Average duration of all videos in each channel.
10. Videos with the highest number of comments.

## Conclusion
This application provides a comprehensive solution for harvesting and analyzing YouTube data. Users can easily manage YouTube channel data and gain valuable insights through a simple and intuitive interface.


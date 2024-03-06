# Import necessary libraries
from googleapiclient.discovery import build
import pymongo
import mysql.connector
import pandas as pd
from datetime import datetime
import re
import streamlit as st

 
#define the function to establish API connection
def api_connect():
    #API key for accessing the youtube API
    api_ID = 'AIzaSyCqfhCj2FiLuTqknpTTbl24eFcRe05WpzM'
    #API service name and version
    api_service_name = "youtube" 
    api_version = "v3"
    #Build a service object for interacting with the youtube API
    youtube=build(api_service_name,api_version,developerKey=api_ID)
    #Return the youtube service object 
    return youtube
#call the api_connect function to get a youtube service
youtube = api_connect()


#define function getting to retrieve channel info based on channel id using API connect function
def get_channel_info(channel_id):
    #create a request to fetch channel information using the provided channel id 
    request = youtube.channels().list(
        part = "snippet,contentDetails,statistics", #specfic the parts of the channel information to retrieved
        id = channel_id) #specfic the channel id for which information is requested
    #execute the request and store the response in channel_data
    channel_data = request.execute()
    #loop throught the items in the channel_data
    for index in channel_data['items']:
        #extract relevant information form the channel data and store it in a dictionary
        data = dict(channel_name = index['snippet']['title'],
                channel_id = index['id'],
                subscriber_count = index['statistics']['subscriberCount'],
                channel_views = index['statistics']['viewCount'],
                channel_description = index['snippet']['description'],
                playlist_id = index['etag'],
                video_count = index['statistics']['videoCount']
                )
        #return the dicitonary containing channel information
        return data 


#define function for getting video id for channel id using API connect function
def get_video_ids(channel_id):
    #Initialize an empty list to store data in video_ids
    video_ids= []
    #create a request to fetch content detial of the channel using channel id
    request = youtube.channels().list(
            part = "contentDetails",
            id = channel_id)
    #executing the request and storing the response
    response = request.execute()
    #extract video ID from the playlist containing the channel's uploads
    Playlist_id = (response['items'][0]['contentDetails']['relatedPlaylists']['uploads'])
    #initalize the variable to store the token for next page of result 
    next_page_token = None
    #iterate through the page of video in the playlist 
    while True:
        #request the list of video in the playlist 
        channel_videos= youtube.playlistItems().list(
                                            part= "snippet", #specfic the part of video id information
                                            playlistId = Playlist_id, #fetch the list of video ids for channel id
                                            maxResults= 50, #maximum number of videos to be returned per request (maximum allowed is 50)
                                            pageToken = next_page_token #token for retrieving the next page of results (pages)
                                            ).execute() #execute the API request and retrieve the response
        #extract video IDs form the response and add then to the list
        for index in range(len(channel_videos['items'])):
            video_ids.append(channel_videos['items'][index]['snippet']['resourceId']['videoId'])
        #check if there is a next page,exit loop
        next_page_token = channel_videos.get('nextPageToken')
        #if there is no next page, exit the loop 
        if next_page_token is None:
            break
    #return the list of video IDs
    return video_ids


#define function to get video info for video ids
def get_video_detial(video_ids): 
    #intialize an empty list to store video data
    video_data= []
    #iterate throught each video ID
    for video_id in video_ids:
        #create a request to fetch channel information using the provided channel id 
        request = youtube.videos().list(
                                        part="snippet,contentDetails,statistics", #specify the parts of video data to retrieve
                                        id = video_id) #specify the video ID
        #executing the request and storing the response
        response = request.execute()
        #iterate through each item in the response
        for item in response["items"]:
            #extract video data revelant information form a video id and create a dictionary 
            data = dict(channel_name = item['snippet']['channelTitle'],
                        channel_id = item['snippet']['channelId'],
                        video_id = item['id'],
                        video_title = item['snippet']['title'],
                        descrpition = item['snippet']['description'],
                        tags= item['snippet'].get('tags'),
                        published_At = item['snippet']['publishedAt'],
                        view_count = item['statistics']['viewCount'],
                        likes_count= item['statistics'].get('likeCount'),
                        favorite_count= item['statistics']['favoriteCount'],
                        comment_count= item['statistics'].get('commentCount'),
                        duration = item['contentDetails']['duration'],
                        thumbnail = item['snippet']['thumbnails']['default']['url'],
                        caption = item['contentDetails']['caption']
                        )
            #append the video data dictionary into list 
            video_data.append(data) 
    #return the list of video data        
    return video_data 


#define function for get comment info  for video id
def get_comment_info(video_ids):
    #intialize an empty list to store comment data
    comment_data = []
    try:
        for video_id in video_ids:
            #loop throught each video id in the list 
            request = youtube.commentThreads().list(
                                        part="snippet", #specify the parts of comment data to retrieve
                                        videoId = video_id, #specify the video ID
                                        maxResults = 50) #maximum number of videos to be returned per request (maximum allowed is 50)
            #executing the request and storing the response
            response = request.execute()
            #iterate through each item in the response
            for item in response["items"]:
                    #extract comment data revelant information form a video id and create a dictionary 
                    data = dict(comment_id = item ['snippet']['topLevelComment']['id'],
                                video_id = item ['snippet']['topLevelComment']['snippet']['videoId'],
                                comment_text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                                comment_author = item['snippet'][ 'topLevelComment']['snippet']['authorDisplayName'],
                                comment_published_At = item['snippet'][ 'topLevelComment']['snippet']['publishedAt']
                                )
                    #append the comment data dictionary into list 
                    comment_data.append(data)
    # handle exceptions because some of comment does not avaiable 
    except: 
         pass
    #return the list of comment data
    return comment_data



#main function to get playlist detial for channel id
def get_playlist_detial(channel_id): 
    #initalize the variable to store the token for next page of result 
    next_page_token = None
    #intialize an empty list to store playlist detial
    playlist_detial = []
    #iterate through the page of the playlist 
    while True:
        #request the list of video in the playlist
        request = youtube.playlists().list(
                part = "snippet,contentDetails", #specify the parts of playlist data to retrieve
                channelId = channel_id, #specify the channel ID
                maxResults = 50, #maximum number of videos to be returned per request (maximum allowed is 50)
                pageToken = next_page_token) #token for retrieving the next page of results (pages)
        #executing the request and storing the response
        response = request.execute()
        #iterate through each item in the response
        for item in response["items"]:
            #extract playlist data revelant information form a channel id and create a dictionary 
            data = dict(playlist_id = item['id'],
                        playlist_title = item['snippet']['title'],
                        channel_id = item['snippet']['channelId'],
                        channel_name =item['snippet']['channelTitle'],
                        published_At =item['snippet']['publishedAt'],
                        video_count = item['contentDetails']['itemCount'] 
                        )
            #append the playlist data dictionary into list 
            playlist_detial.append(data)
        #check if there is a next page,exit loop
        next_page_token = response.get('nextPageToken')
        #if there is no next page, exit the loop 
        if next_page_token is None:
            break
        #return the playlist detial
    return playlist_detial

#Create connection to mongo db
client = pymongo.MongoClient("mongodb+srv://ramkumarkannan14:oy5lnSKFfmKhooK9@cluster0.pnkpsmo.mongodb.net/")
db = client["Youtube_Data"]

#main fucntion extract the data for youtube channel for define function 
def channel_details(channel_id): 
    #getting channel info
    ch_detial = get_channel_info(channel_id)
    #getting playlist info
    pl_detial = get_playlist_detial(channel_id)
    #getting video IDs 
    vid_ids = get_video_ids(channel_id)
    #getting video detial
    vid_detial = get_video_detial(vid_ids)
    #getting comment detial
    com_detial = get_comment_info(vid_ids)

    #access the mongodb collection
    coll1 = db["channel_detials"]
    #insert extract data into the collection
    coll1.insert_one({"channel_info": ch_detial,
                        "playlist_info": pl_detial,
                        "video_ids_info": vid_ids,
                        "video_detail": vid_detial,
                        "video_comment":com_detial
                        })
    #return success message
    return "Data extract successfully"

#Streamlit part

st.sidebar.title(':rainbow[YOUTUBE DATA HAVERSTING TOOL]') #create title in streamlit in rainbow font color
selection = st.sidebar.selectbox("Menu", ['Application Details', 'Sample Process','Extraction Data', 'View Data in MongoDB',
                                          'Migrate to MySQL','View Data in MySQL','Analysis using SQL']) #create the selectbox in streamlit code

#select condition for selectbox to view
if selection == 'Application Details': #select condition for selectbox to view menu application details
    st.title(''' YouTube Data Harvesting and Warehousing using SQL, MongoDB, and Streamlit''') 
    st.markdown('''This is a project aimed at collecting,storing, and visualizing data from 
            YouTube using various technologies. Let's break down each component:''')

    st.header("YouTube Data Harvesting:")

    st.markdown('''This involves extracting data from YouTube, such as video metadata (title, description, publish date, etc.), 
    video statistics (views, likes, dislikes, comments, etc.), and channel information.YouTube Data API can be utilized for
    this purpose. It provides endpoints for fetching information about videos, channels, playlists, etc.''')
            
    st.header("SQL Database (MySQL):")

    st.markdown('''SQL databases are used for structured data storage and management.
    The harvested YouTube data can be structured and stored in SQL tables. For example, a table for videos, 
    a table for channels, etc. SQL queries can be used to perform data analysis, filtering, and aggregation.''')
            
    st.header("MongoDB:")
    st.markdown('''MongoDB is a NoSQL database known for its flexibility with unstructured or semi-structured data.
    Some of the YouTube data may not fit neatly into a structured SQL schema, such as video comments or user-generated tags.
    MongoDB can be used to store this semi-structured data in a more flexible document-based format.''')
            
    st.header("Streamlit:")
    st.markdown('''Streamlit is a Python library used for building interactive web applications for data science and machine learning projects.
    It allows for the creation of web-based dashboards and visualizations with minimal code. Streamlit can be used to build a user interface for
    querying the YouTube data stored in SQL and MongoDB databases. Users can interact with the data through filters, search functionality, and
    visualizations (charts, graphs, etc.) generated dynamically based on user inputs.''')

    st.header("Project Workflow:")
    st.markdown('''YouTube data is harvested periodically using the YouTube Data API and stored in both SQL and MongoDB databases.
    SQL tables are used for structured data like video metadata and channel information.
    MongoDB is utilized for semi-structured data such as video comments.
    Streamlit is employed to develop a web application interface for users to interact with the stored data.
    Users can query the data using filters, search functionality, and visualize insights through dynamic charts and graphs.''')
            
    st.header("Key Features:")
    st.markdown('''Data harvesting from YouTube using the YouTube Data API.
    Storage of structured data in SQL databases and semi-structured data in MongoDB.
    Interactive web application interface built with Streamlit for querying and visualizing YouTube data.
    Features like search, filtering, and dynamic visualizations for data exploration.''')
            
    st.header("Potential Use Cases:")
    st.markdown('''Content creators can analyze the performance o f their videos over time.
    Marketers can study trends and user engagement on specific topics or channels.
    Researchers can analyze user behavior and content consumption patterns on YouTube.
    By integrating SQL, MongoDB, and Streamlit, this project provides a comprehensive solution for harvesting, storing,
    and visualizing YouTube data, catering to various analytical and exploratory needs.''')


#select condition for selectbox to view Sample Process
elif selection == 'Sample Process':
        st.title("Extract Detial for Channel ID")

        def api_connect():
        # Define the API key
            api_key = "AIzaSyCqfhCj2FiLuTqknpTTbl24eFcRe05WpzM" 
        # Define the API service name and version
            api_service_name = "youtube"
            api_version = "v3"
        # Build and return the YouTube API object
            youtube = build(api_service_name, api_version, developerKey=api_key)
            return youtube
    
        # Function to get channel data from YouTube API
        def get_channel_data(youtube, channel_id):
            request = youtube.channels().list(
                part = "snippet,contentDetails,statistics",
                id = channel_id
            )
            channel_data = request.execute()
            return channel_data
        
        #function to get video data from YouTube API
        def get_video_data(youtube, channel_id):
            request = youtube.search().list(
                part="snippet",
                channelId=channel_id,
                type="video"
            )
            video_data = request.execute()
            return video_data
        
        # Function to get comment data from YouTube API
        def get_comment_data(youtube, video_id):
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id
            )
            comment_data = request.execute()
            return comment_data

        channel_id = st.text_input("Enter Channel ID:")

        if st.button("Extract Data"):
            
            if channel_id:
                with st.spinner("Fetching channel data..."):
                # Call the api_connect function to establish connection to the YouTube API
                    youtube = api_connect()
                
                # Get channel details
                channel_data = get_channel_data(youtube, channel_id)
                if 'items' in channel_data and len(channel_data['items']) > 0:
                    channel_info = channel_data['items'][0]
                    snippet = channel_info['snippet']
                    statistics = channel_info['statistics']
                    thumbnail = channel_info['snippet']['thumbnails']['medium']

                    st.subheader(":red[Channel Details :]")

                    st.write(":red[Channel Logo :]")
                    st.image(thumbnail.get('url'))
                    st.write(":red[Channel Name :]", snippet.get('title'))
                    st.write(":red[Channel Description :]", snippet.get('description'))
                    st.write(":red[Subscriber Count:]", statistics.get('subscriberCount'))
                    st.write(":red[Channel Views :]", statistics.get('viewCount'))
                    st.write(":red[Video Count:]", statistics.get('videoCount'))
                    st.write(":red[Published at:]", snippet.get('publishedAt'))

                    # Get video details
                    video_data = get_video_data(youtube, channel_id)
                    if 'items' in video_data and len(video_data['items']) > 0:
                        video_info = video_data['items'][0]
                        video_snippet = video_info['snippet']
                        video_thumbnail = video_snippet['thumbnails']['medium']
                        video_id = video_info['id']['videoId']

                        st.subheader(":red[Video Details :]")

                        st.write(":red[Video Thumbnail :]")
                        st.image(video_thumbnail.get('url'))
                        st.write(":red[Video Title:]", video_snippet.get('title'))
                        st.write(":red[Video Description:]", video_snippet.get('description'))
                        st.write(":red[Video View Count:]", statistics.get('viewCount'))
                        st.write(":red[Video Published at:]", video_snippet.get('publishedAt'))
                        
                        # Get comment details
                        comment_data = get_comment_data(youtube, video_id)
                        if 'items' in comment_data and len(comment_data['items']) > 0:
                            comment_info = comment_data['items'][0]['snippet']['topLevelComment']['snippet']

                            st.subheader(":red[Video Comment:]")

                            st.write(":red[Comment Text:]", comment_info.get('textDisplay'))
                            st.write(":red[Comment Author:]", comment_info.get('authorDisplayName'))
                            st.write(":red[Published at:]", comment_info.get('publishedAt'))

                    else:
                        st.warning("No comments found for this video.")
                else:
                    st.warning("No videos found for this channel.")
            else:
                st.warning("Channel ID not valid.")

elif selection == 'Extraction Data':
    st.title('Extraction Data for Youtube and store in MangoDB')

    channel_ids = st.text_area("Enter the Channel IDs (separated by comma)")

    if st.button("Collect and Store"):
        with st.spinner("Fetching channel information..."):
            ch_ids = []
            db = client["Youtube_Data"]
            coll1 = db["channel_detials"]
            
            # Split the input by comma and strip any extra spaces
            input_ids = [channel_id.strip() for channel_id in channel_ids.split(",")]

            for ch_data in coll1.find({}, {'_id': 0, 'channel_info': 1}):
                ch_ids.append(ch_data["channel_info"]["channel_id"])

            # Check if any of the input IDs already exist in the database
            existing_ids = set(input_ids).intersection(ch_ids)

            if existing_ids:
                st.warning(f"Channel Detail of the following channel IDs already exist: {', '.join(existing_ids)}")

            new_ids = [channel_id for channel_id in input_ids if channel_id not in ch_ids][:10]

            if new_ids:
                inserts = []
                for new_id in new_ids:
                    inserts.append(channel_details(new_id))
                st.success(f"Successfully inserted details for {len(inserts)} new channels.")

            else:
                st.info("No new channel IDs to insert.")

elif selection == 'View Data in MongoDB':
    st.title('View Data in MongoDB')
    
    #retrieves data from a MongoDB database and channel detial displays it in a dataFrame using streamlit library
    def show_channel_table():
        channel_name_list = [] #initialize an empty list to store channel detial
        db = client["Youtube_Data"] #connect to the MongoDB client and select the database
        coll1 = db["channel_detials"] #select the collection containing channel details

        #iterate over each document in the collection and extract channel info
        for ch_data in coll1.find({},{'_id':0, 'channel_info':1}):
            channel_name_list.append(ch_data['channel_info'])  #append the channel info to the list
        channel_table= st.dataframe(channel_name_list) #convert the list to a DataFrame

        return channel_table #return the DataFrame


    #retrieves data from a MongoDB database and playlist detial displays it in a dataFrame using streamlit library
    def show_playlist_table():
        playlist_name_list = [] #initialize an empty list to store playlist detial
        db = client["Youtube_Data"] #connect to the MongoDB client and select the database
        coll1 = db["channel_detials"] #select the collection containing channel details

        #iterate over each document in the collection and extract playlist info
        for pl_data in coll1.find({},{'_id':0, 'playlist_info':1}):
            for index in range(len(pl_data['playlist_info'])):
                playlist_name_list.append(pl_data['playlist_info'][index]) #append the playlist info to the list
        playlist_table= st.dataframe(playlist_name_list) #convert the list to a DataFrame

        return playlist_table  #return the DataFrame


    #retrieves data from a MongoDB database and video detial displays it in a dataFrame using streamlit library
    def show_video_table():
        videos_name_list = [] #initialize an empty list to store video detial
        db = client["Youtube_Data"] #connect to the MongoDB client and select the database
        coll1 = db["channel_detials"] #select the collection containing channel details

        #iterate over each document in the collection and extract video info
        for vi_data in coll1.find({},{'_id':0, 'video_detail':1}):
            for index in range(len(vi_data['video_detail'])):
                videos_name_list.append(vi_data['video_detail'][index]) #append the video info to the list
        video_table= st.dataframe(videos_name_list) #convert the list to a DataFrame

        return video_table  #return the DataFrame


    #retrieves data from a MongoDB database and comment detial displays it in a dataFrame using streamlit library
    def show_comment_table():
        comment_name_list = [] #initialize an empty list to store comment detial
        db = client["Youtube_Data"] #connect to the MongoDB client and select the database
        coll1 = db["channel_detials"] #select the collection containing channel details

        #iterate over each document in the collection and extract comment info
        for com_data in coll1.find({},{'_id':0, 'video_comment':1}):
            for index in range(len(com_data['video_comment'])):
                comment_name_list.append(com_data['video_comment'][index]) #append the comment info to the list
        comment_table= st.dataframe( comment_name_list) #convert the list to a DataFrame

        return comment_table  #return the DataFrame

    show_table=st.selectbox("SELECT THE TABLE FOR VIEW", ("CHANNELS","VIDEOS","COMMENTS","PLAYLISTS"))

    if show_table=="CHANNELS":
        show_channel_table()

    elif show_table=="VIDEOS":
        show_video_table()

    elif show_table=="COMMENTS":
        show_comment_table()

    elif show_table=="PLAYLISTS":
        show_playlist_table()


elif selection == 'Migrate to MySQL':
    st.title('Migrate to MySQL Database')
    
    #Create connection to mongo db
    client=pymongo.MongoClient("mongodb+srv://ramkumarkannan14:oy5lnSKFfmKhooK9@cluster0.pnkpsmo.mongodb.net/")
    db = client["Youtube_Data"]

    def mysql_connect():
        return mysql.connector.connect(host='localhost', user='root', password='12345', database='Youtube')

    def create_mysql_tables():
        connection = mysql_connect()
        mycursor = connection.cursor()
        
        # Create query for 'channels' table
        create_channels_table_query  = '''CREATE TABLE IF NOT EXISTS channels(
                                            channel_name VARCHAR(225),
                                            channel_id VARCHAR(225) PRIMARY KEY, 
                                            subscriber_count BIGINT, 
                                            channel_views BIGINT,
                                            channel_description TEXT,
                                            playlist_id  VARCHAR(225),
                                            video_count INT
                                            )'''

        # Create query for 'playlists' table
        create_playlists_table_query = """CREATE TABLE IF NOT EXISTS playlists(playlist_id VARCHAR(225) PRIMARY KEY, 
                                                        playlist_title VARCHAR(225),
                                                        channel_id VARCHAR(225), 
                                                        channel_name VARCHAR(225), 
                                                        video_count INT, 
                                                        published_At TIMESTAMP
                                                        )"""
            
        # Create query for 'videos' table
        create_videos_table_query = """CREATE TABLE IF NOT EXISTS videos(
                        channel_Id VARCHAR(225), 
                        channel_Name VARCHAR(225),
                        video_id VARCHAR(225) PRIMARY KEY, 
                        video_title VARCHAR(225), 
                        Descrpition TEXT,
                        Tags TEXT,
                        Published_At TIMESTAMP,
                        View_count INT,
                        Likes_count INT,
                        favorite_count INT,
                        comment_count INT,
                        Duration INT, 
                        thumbnail VARCHAR(225),
                        caption TEXT
                        )"""
    
        # Create query for 'comments' table
        create_comments_table_query = """CREATE TABLE IF NOT EXISTS comments(comment_id VARCHAR(225) PRIMARY KEY, 
                                                        comment_text TEXT,
                                                        video_id VARCHAR(225), 
                                                        comment_author VARCHAR(225),
                                                        comment_published_At TIMESTAMP
                                                        )"""
                                                        
        try:                                                
            mycursor.execute(create_channels_table_query)
            mycursor.execute(create_playlists_table_query)                                         
            mycursor.execute(create_videos_table_query)
            mycursor.execute(create_comments_table_query)
            connection.commit()
            st.success("MySQL tables created successfully.")
        except mysql.connector.Error as err:
            print(f"Error creating MySQL tables: {err}")

    # Define the function to insert channel details into MySQL
    def insert_channel_data(channel_data):
        connection = mysql_connect()
        mycursor = connection.cursor()

        insert_query = '''INSERT INTO channels(
                            channel_name,
                            channel_id,
                            subscriber_count,
                            channel_views,
                            channel_description,
                            playlist_id,
                            video_count)
                            VALUES(%s, %s, %s, %s, %s, %s, %s)'''

        values = (
            channel_data['channel_name'],
            channel_data['channel_id'],
            channel_data['subscriber_count'],
            channel_data['channel_views'],
            channel_data['channel_description'],
            channel_data['playlist_id'],
            channel_data['video_count']
        )
        try:                     
            mycursor.execute(insert_query, values)
            connection.commit()   
        except mysql.connector.Error as err:
            print(f"An error occurred: {err}")

    # Define the function to insert playlist data into MySQL
    def insert_playlist_data(playlist_data):
        connection = mysql_connect()
        mycursor = connection.cursor()

        insert_query = """INSERT INTO playlists(
                            playlist_id, 
                            playlist_title,
                            channel_id, 
                            channel_name, 
                            video_count,
                            published_At
                            )
                            VALUES(%s,%s,%s,%s,%s,%s)"""

        #assume 'row' is a dictionary-like object contanining data, and 'published_At' is one of its keys.
        #retrieve the value of 'published_At' from the row
        publised_at = playlist_data['published_At']
        #check if the value of 'published_At' is NaN (Not a Number)
        if pd.isna(publised_at):
            #if it is NaN, set a default datetime value
            published_at_mysql = '1970-01-01 00:00:00'  #example default datetime value
        else:
            #if it's not NaN, convert the string to a datetime object
            published_at = datetime.strptime(str(publised_at), '%Y-%m-%dT%H:%M:%SZ')
            #convert the datetime object to a string formatted as 'YYYY-MM-DD HH:MM:SS'
            published_at_mysql = published_at.strftime('%Y-%m-%d %H:%M:%S')

        values = (playlist_data['playlist_id'],
                playlist_data['playlist_title'],
                playlist_data['channel_id'],
                playlist_data['channel_name'],
                playlist_data['video_count'],
                published_at_mysql
                )

        try:                     
            mycursor.execute(insert_query, values) 
            connection.commit() 
        except mysql.connector.Error as err:
            print(f"An error occurred: {err}")
            
    # Define the function to insert video data into MySQL
    def insert_video_data(video_data):
        connection = mysql_connect()
        mycursor = connection.cursor()

        # Insert data into MySQL table
        #convert string to datetime object
        published_at = datetime.strptime(video_data['published_At'], '%Y-%m-%dT%H:%M:%SZ')
        #format datetime object as MySQL datetime string
        published_at_mysql = published_at.strftime('%Y-%m-%d %H:%M:%S')
    
        #calculate duration in seconds
        duration_str = video_data['duration']
        duration_regex = r'PT(\d+)M(\d+)S'
        duration_match = re.match(duration_regex, duration_str)
    
        if duration_match:
            minutes = int(duration_match.group(1))
            seconds = int(duration_match.group(2))
            duration_seconds = minutes + seconds/60 # Convert seconds to minutes
        else:
            duration_seconds = None  #set duration_seconds to None if duration format is invalid

        #convert likes count to integer if not none,else set to none
        likes_count = int(video_data['likes_count']) if video_data['likes_count'] is not None else None

        insert_query = """INSERT INTO videos(
                                channel_id, 
                                channel_name,
                                video_id, 
                                video_title, 
                                descrpition,
                                tags,
                                published_At,
                                view_count,
                                likes_count,
                                favorite_count,
                                comment_count,
                                duration,
                                thumbnail,
                                caption
                                )
                                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        
        values = (
            str(video_data['channel_id']),
            str(video_data['channel_name']),
            str(video_data['video_id']),
            str(video_data['video_title']),
            str(video_data['descrpition']),
            str(video_data['tags']),
            published_at_mysql,
            int(video_data['view_count']),
            likes_count,
            int(video_data['favorite_count']),
            int(video_data['comment_count']),
            duration_seconds,
            str(video_data['thumbnail']),
            str(video_data['caption'])
        ) 
    
        try:                     
            mycursor.execute(insert_query, values) 
            connection.commit() 
        except mysql.connector.Error as err:
            print(f"An error occurred: {err}")

# Define the function to insert comment data into MySQL
    def insert_comment_data(comment_data):
        connection = mysql_connect()
        mycursor = connection.cursor()

        insert_query = """INSERT INTO comments(comment_id, 
                                                comment_text,
                                                comment_author,
                                                video_id,
                                                comment_published_At                
                                                )
                                                VALUES(%s,%s,%s,%s,%s)"""
        
        # Convert string to datetime object
        published_at = datetime.strptime(comment_data['comment_published_At'], '%Y-%m-%dT%H:%M:%SZ')
        # Format datetime object as MySQL datetime string
        published_at_mysql = published_at.strftime('%Y-%m-%d %H:%M:%S')
    
        values=(comment_data['comment_id'],
                comment_data['comment_text'],
                comment_data['comment_author'],
                comment_data['video_id'],
                published_at_mysql
                ) 
    
        try:                     
            mycursor.execute(insert_query, values) 
            connection.commit() 
        except mysql.connector.Error as err:
            print(f"An error occurred: {err}") 
        
# Define the main function to transfer data from MongoDB to MySQL
    def transfer_data_to_mysql(selected_channel):
        create_mysql_tables()
        db = client["Youtube_Data"] 
        coll1 = db["channel_detials"]

    # Loop through each document in the MongoDB collection
        for ch_data in coll1.find({"channel_info.channel_name": selected_channel}):
            channel_data = ch_data["channel_info"]
            playlist_data = ch_data.get("playlist_info", [])
            video_data = ch_data.get("video_detail", [])
            comment_data = ch_data.get("video_comment", [])

            # Insert channel data
            insert_channel_data(channel_data)

            # Insert playlist data
            for playlist in playlist_data:
                insert_playlist_data(playlist)

            # Insert video data
            for video in video_data:
                insert_video_data(video)

            # Insert comment data
            for comment in comment_data:
                insert_comment_data(comment)
    
    db = client["Youtube_Data"] 
    coll1 = db["channel_detials"]
    
    channel_name_list = [ch_data["channel_info"]['channel_name'] for ch_data in coll1.find({}, {"_id": 0, "channel_info": 1})]

    selected_channel = st.selectbox("Select Channel Name:", channel_name_list)  
    
    connection = mysql_connect()
    mycursor = connection.cursor()

    question7 = '''SELECT channel_name FROM channels'''
    mycursor.execute(question7)

    t7=mycursor.fetchall()
    my_sql_channel_name =pd.DataFrame(t7,columns=["channel_name"])
       
    if st.button("Transfer MongoDB Data to SQL Server"):
        if selected_channel not in my_sql_channel_name['channel_name'].values:
            with st.spinner("Please wait, transferring data from MongoDB to MySQL server..."):
                transfer_data_to_mysql(selected_channel)

                st.success("Selected Data transferred successfully to MySQL server.")    
        else:
            st.warning("Channel data already updated in SQL")
elif selection == 'View Data in MySQL':
    st.title('View Data in MySQL')
    
    connection = mysql.connector.connect(host='localhost',user='root',password='12345',database='Youtube')
    mycursor = connection.cursor()

    channel_list = '''SELECT channel_name,channel_id FROM channels'''
    mycursor.execute(channel_list)

    d1=mycursor.fetchall()
    my_sql_channel_name =pd.DataFrame(d1,columns=["channel_name","channel_id"])
    st.write(my_sql_channel_name)
    
elif selection == 'Analysis using SQL':
    st.title('Analysis using SQL')

    connection = mysql.connector.connect(host='localhost',user='root',password='12345',database='Youtube')
    mycursor = connection.cursor()

    question=st.selectbox("Select your Query",("1: What are the names of all the videos and their corresponding channels?",
                                                "2: Which channels have the most number of videos, and how many videos do they have?",
                                                "3: What are the top 10 most viewed videos and their respective channels?",
                                                "4: How many comments were made on each video, and what are their corresponding video names?",
                                                "5: Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                "6: What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                                "7: What is the total number of views for each channel, and what are their  corresponding channel names?",
                                                "8: What are the names of all the channels that have published videos in the year 2022?",
                                                "9: What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                "10: Which videos have the highest number of comments, and what are their  corresponding channel names?"))

    if question == "1: What are the names of all the videos and their corresponding channels?":
        question1 = '''SELECT video_title, channel_name FROM videos'''
        mycursor.execute(question1)

        t1=mycursor.fetchall()
        df=pd.DataFrame(t1,columns=["video_title","channel_name"])
        st.write(df)

    elif question == "2: Which channels have the most number of videos, and how many videos do they have?":
        question2 = '''SELECT video_count, channel_name FROM channels ORDER BY video_count DESC'''
        mycursor.execute(question2)

        t2=mycursor.fetchall()
        df2=pd.DataFrame(t2,columns=["channel_name","video_count"])
        st.write(df2)

    elif question == "3: What are the top 10 most viewed videos and their respective channels?":
        question3 = '''SELECT view_count, channel_name FROM videos ORDER BY view_count DESC LIMIT 10'''
        mycursor.execute(question3)

        t3=mycursor.fetchall()
        df3=pd.DataFrame(t3,columns=["view_count","channel_name"])
        st.write(df3)

    elif question == "4: How many comments were made on each video, and what are their corresponding video names?":
        question4 = '''SELECT comment_count, video_title FROM videos'''
        mycursor.execute(question4)

        t4=mycursor.fetchall()
        df4=pd.DataFrame(t4,columns=["comment_count","video_title"])
        st.write(df4)

    elif question == "5: Which videos have the highest number of likes, and what are their corresponding channel names?":

        question5 = '''SELECT likes_count, video_title, channel_name FROM videos ORDER BY likes_count DESC'''
        mycursor.execute(question5)

        t5=mycursor.fetchall()
        df5=pd.DataFrame(t5,columns=["likes_count","video_title", "channel_name"])
        st.write(df5)

    elif question == "6: What is the total number of likes and dislikes for each video, and what are their corresponding video names?":

        question6 = '''SELECT likes_count,video_title FROM videos'''
        mycursor.execute(question6)

        t6=mycursor.fetchall()
        df6=pd.DataFrame(t6,columns=["likes_count","video_title"])
        st.write(df6)

    elif question == "7: What is the total number of views for each channel, and what are their  corresponding channel names?":

        question7 = '''SELECT channel_name, SUM(channel_views) AS total_views FROM channels GROUP BY channel_name'''
        mycursor.execute(question7)

        t7=mycursor.fetchall()
        df7=pd.DataFrame(t7,columns=["channel_name", "channel_views"])
        st.write(df7)


    elif question == "8: What are the names of all the channels that have published videos in the year 2022?":

        question8 = '''SELECT channel_name, YEAR(published_at) AS publication_year FROM videos WHERE YEAR(published_at) = 2022'''
        mycursor.execute(question8)

        t8=mycursor.fetchall()
        df8=pd.DataFrame(t8,columns=["channel_name", "publication_year"])
        st.write(df8)


    elif question == "9: What is the average duration of all videos in each channel, and what are their corresponding channel names?":

        question9 = '''SELECT AVG(duration) AS average_duration, channel_name FROM videos GROUP BY channel_name'''
        mycursor.execute(question9)

        t9=mycursor.fetchall()
        df9=pd.DataFrame(t9,columns=["average_duration", "channel_name"])
        st.write(df9)


    elif question == "10: Which videos have the highest number of comments, and what are their corresponding channel names?":

        question10 = '''SELECT channel_name, comment_count FROM videos ORDER BY comment_count DESC LIMIT 10'''
        mycursor.execute(question10)

        t10=mycursor.fetchall()
        df10=pd.DataFrame(t10,columns=["channel_name","comment_count"])
        st.write(df10)

# Import necessary libraries
from googleapiclient.discovery import build
import pymongo
import mysql.connector
import pandas as pd
from datetime import datetime
import re
import streamlit as st

def api_connect():
    api_ID = 'AIzaSyCqfhCj2FiLuTqknpTTbl24eFcRe05WpzM'
    api_service_name = "youtube" 
    api_version = "v3"
    youtube=build(api_service_name,api_version,developerKey=api_ID)
    return youtube
youtube = api_connect()

def get_channel_info(channel_id):
    request = youtube.channels().list(part = "snippet,contentDetails,statistics", id = channel_id) 
    channel_data = request.execute()
    for index in channel_data['items']:
        data = dict(channel_name = index['snippet']['title'],
                channel_id = index['id'],
                subscriber_count = index['statistics']['subscriberCount'],
                channel_views = index['statistics']['viewCount'],
                channel_description = index['snippet']['description'],
                playlist_id = index['etag'],
                video_count = index['statistics']['videoCount'])
        return data 

def get_video_ids(channel_id):
    video_ids= []
    request = youtube.channels().list(
            part = "contentDetails",
            id = channel_id)
    response = request.execute()
    Playlist_id = (response['items'][0]['contentDetails']['relatedPlaylists']['uploads'])
    next_page_token = None
    while True:
        channel_videos= youtube.playlistItems().list(part= "snippet",playlistId = Playlist_id, 
                                            maxResults= 50,pageToken = next_page_token ).execute() 
        for index in range(len(channel_videos['items'])):
            video_ids.append(channel_videos['items'][index]['snippet']['resourceId']['videoId'])
        next_page_token = channel_videos.get('nextPageToken')
        if next_page_token is None:
            break
    return video_ids

def get_video_detial(video_ids): 
    video_data= []
    for video_id in video_ids:
        request = youtube.videos().list(part="snippet,contentDetails,statistics",id = video_id)
        response = request.execute()
        for item in response["items"]:
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
                        caption = item['contentDetails']['caption']) 
            video_data.append(data)        
    return video_data 

def get_comment_info(video_ids):
    comment_data = []
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(part="snippet",videoId = video_id,maxResults = 50)
            response = request.execute()
            for item in response["items"]:
                    data = dict(comment_id = item ['snippet']['topLevelComment']['id'],
                                video_id = item ['snippet']['topLevelComment']['snippet']['videoId'],
                                comment_text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                                comment_author = item['snippet'][ 'topLevelComment']['snippet']['authorDisplayName'],
                                comment_published_At = item['snippet'][ 'topLevelComment']['snippet']['publishedAt'])
                    comment_data.append(data)
    except: 
         pass
    return comment_data

def get_playlist_detial(channel_id): 
    next_page_token = None
    playlist_detial = []
    while True:
        request = youtube.playlists().list(
                part = "snippet,contentDetails",channelId = channel_id,maxResults = 50, pageToken = next_page_token) 
        response = request.execute()
        for item in response["items"]:
            data = dict(playlist_id = item['id'],
                        playlist_title = item['snippet']['title'],
                        channel_id = item['snippet']['channelId'],
                        channel_name =item['snippet']['channelTitle'],
                        published_At =item['snippet']['publishedAt'],
                        video_count = item['contentDetails']['itemCount'] )
            playlist_detial.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break
    return playlist_detial

#Create connection to mongo db
client=pymongo.MongoClient("mongodb+srv://ramkumarkannan14:oy5lnSKFfmKhooK9@cluster0.pnkpsmo.mongodb.net/")
db = client["Youtube_Data"]

def channel_details(channel_id): 
    ch_detial = get_channel_info(channel_id)
    pl_detial = get_playlist_detial(channel_id)
    vid_ids = get_video_ids(channel_id)
    vid_detial = get_video_detial(vid_ids)
    com_detial = get_comment_info(vid_ids)

    coll1 = db["channel_detials"]
    coll1.insert_one({"channel_info": ch_detial,"playlist_info": pl_detial,"video_ids_info": vid_ids,
                        "video_detail": vid_detial,"video_comment":com_detial})
    return "Data extract successfully"

def channel_table():
    connection = mysql.connector.connect(host='localhost',user='root',password='12345',database='Youtube')
    mycursor = connection.cursor()
       
    create_query = '''CREATE TABLE IF NOT EXISTS channels(
                            channel_name VARCHAR(225),
                            channel_id VARCHAR(225) PRIMARY KEY, 
                            subscriber_count BIGINT, 
                            channel_views BIGINT,
                            channel_description TEXT,
                            playlist_id  VARCHAR(225),
                            video_count INT
                            )'''
    mycursor.execute(create_query)

    ch_id_list_mgdb = []
    db = client["Youtube_Data"] 
    coll1 = db["channel_detials"]
    for ch_data in coll1.find({},{"_id":0,"channel_info":1}):
        ch_id_list_mgdb.append(ch_data["channel_info"]['channel_id'])

    ch_id_list_sql = []
    query = '''SELECT channel_id FROM channels'''
    mycursor.execute(query)

    result = mycursor.fetchall()
    for data in result:
        ch_id_list_sql.append(data[0])  # type: ignore

    new_channel_info = []
    for ch_id_mgdb in ch_id_list_mgdb:
        if ch_id_mgdb not in ch_id_list_sql:
            new_channel_info.append(ch_id_mgdb)

    for ch_id_mgdb in new_channel_info:
        ch_data = coll1.find_one({"channel_info.channel_id": ch_id_mgdb}, {"_id": 0, "channel_info": 1})
        if ch_data:
            df=pd.DataFrame(ch_data)
    
            for index, row in df.iterrows():
                insert_query = '''INSERT INTO channels(
                                    channel_name,
                                    channel_id,
                                    subscriber_count,
                                    channel_views,
                                    channel_description,
                                    playlist_id,
                                    video_count
                                    )
                                    VALUES(%s, %s, %s, %s, %s, %s, %s)''' 

                values = (
                    row['channel_name'],
                    row['channel_id'],
                    row['subscriber_count'],
                    row['channel_views'],
                    row['channel_description'],
                    row['playlist_id'],
                    row['video_count']
                    ) 
                
                try:                     
                    mycursor.execute(insert_query, values) 
                    connection.commit()
                except mysql.connector.Error as err:
                    print(f"An error occurred: {err}") 
 
def playlist_table():
    connection = mysql.connector.connect(host='localhost', user='root', password='12345', database='Youtube')
    mycursor = connection.cursor()

    try:
        query = """CREATE TABLE IF NOT EXISTS playlists(playlist_id VARCHAR(225) PRIMARY KEY, 
                                                    playlist_title VARCHAR(225),
                                                    channel_id VARCHAR(225), 
                                                    channel_name VARCHAR(225), 
                                                    video_count INT, 
                                                    published_At TIMESTAMP
                                                    )"""

        mycursor.execute(query)

    except mysql.connector.Error as err:
        print(f"An error occurred: {err}") 

    pl_id_list_mgdb = [] 
    db = client["Youtube_Data"] 
    coll1 = db["channel_detials"] 

    for pl_data in coll1.find({},{"_id":0,"playlist_info":1}):
        pl_id_list_mgdb.append(pl_data["playlist_info"])

    pl_id_list_sql = []
    query = '''SELECT playlist_id FROM playlists'''
    mycursor.execute(query)

    result = mycursor.fetchall()
    for data in result:
        pl_id_list_sql.append(data[0]) # type: ignore   

    new_playlist_info = []
    for pl_id_mgdb in pl_id_list_mgdb:
        if pl_id_mgdb not in pl_id_list_sql:
            new_playlist_info.append(pl_id_mgdb)

    for pl_id_mgdb in new_playlist_info:
        pl_data = coll1.find_one({"channel_info.channel_id": pl_id_mgdb}, {"_id": 0, "channel_info": 1})
        if pl_data:
            df1=pd.DataFrame(pl_data)
            for index, row in df1.iterrows():
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
                publised_at = row['published_At']
                #check if the value of 'published_At' is NaN (Not a Number)
                if pd.isna(publised_at):
                    #if it is NaN, set a default datetime value
                    published_at_mysql = '1970-01-01 00:00:00'  #example default datetime value
                else:
                    #if it's not NaN, convert the string to a datetime object
                    published_at = datetime.strptime(str(publised_at), '%Y-%m-%dT%H:%M:%SZ')
                    #convert the datetime object to a string formatted as 'YYYY-MM-DD HH:MM:SS'
                    published_at_mysql = published_at.strftime('%Y-%m-%d %H:%M:%S')

                values = (row['playlist_id'],
                        row['playlist_title'],
                        row['channel_id'],
                        row['channel_name'],
                        row['video_count'],
                        published_at_mysql
                        ) 

                try:                     
                    mycursor.execute(insert_query, values) 
                    connection.commit() 
                except mysql.connector.Error as err:
                    print(f"An error occurred: {err}")

def videos_table():
    connection = mysql.connector.connect(host='localhost', user='root', password='12345', database='Youtube')
    mycursor = connection.cursor()
    
    query = """CREATE TABLE IF NOT EXISTS videos(
                    channel_id VARCHAR(225), 
                    channel_name VARCHAR(225),
                    video_id VARCHAR(225) PRIMARY KEY, 
                    video_title VARCHAR(225), 
                    descrpition TEXT,
                    tags TEXT,
                    published_At TIMESTAMP,
                    view_count INT,
                    likes_count INT,
                    favorite_count INT,
                    comment_count INT,
                    duration INT, 
                    thumbnail VARCHAR(225),
                    caption TEXT
                    )"""
    mycursor.execute(query)

    vid_id_list_mgdb = [] 
    db = client["Youtube_Data"] 
    coll1 = db["channel_detials"] 
    for vid_data in coll1.find({},{"_id":0,"video_detail":1}):
        vid_id_list_mgdb.append(vid_data["video_detail"])

    vid_id_list_sql = []
    query = '''SELECT video_id FROM videos'''
    mycursor.execute(query)

    result = mycursor.fetchall()
    for data in result:
        vid_id_list_sql.append(data[0]) # type: ignore   

    new_video_info = []
    for vid_id_mgdb in vid_id_list_mgdb:
        if vid_id_mgdb not in vid_id_list_sql:
            new_video_info.append(vid_id_mgdb)

    for vid_id_mgdb in new_video_info:
    
        vid_data = coll1.find_one({"channel_info.channel_id": vid_id_mgdb}, {"_id": 0, "channel_info": 1})
        if vid_data:
            df2 = pd.DataFrame(vid_data) 
            for index, row in df2.iterrows():
                #convert string to datetime object
                published_at = datetime.strptime(row['published_At'], '%Y-%m-%dT%H:%M:%SZ')
                #format datetime object as MySQL datetime string
                published_at_mysql = published_at.strftime('%Y-%m-%d %H:%M:%S')
            
                #calculate duration in seconds
                duration_str = row['duration']
                duration_regex = r'PT(\d+)M(\d+)S'
                duration_match = re.match(duration_regex, duration_str)
            
                if duration_match:
                    minutes = int(duration_match.group(1))
                    seconds = int(duration_match.group(2))
                    duration_seconds = minutes * 60 + seconds
                else:
                    duration_seconds = None  #set duration_seconds to None if duration format is invalid

                #convert likes count to integer if not none,else set to none
                likes_count = int(row['likes_count']) if row['likes_count'] is not None else None

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
                    str(row['channel_id']),
                    str(row['channel_name']),
                    str(row['video_id']),
                    str(row['video_title']),
                    str(row['descrpition']),
                    str(row['tags']),
                    published_at_mysql,
                    int(row['view_count']),
                    likes_count,
                    int(row['favorite_count']),
                    int(row['comment_count']),
                    duration_seconds,
                    str(row['thumbnail']),
                    str(row['caption'])
                ) 
            
                try:                     
                    mycursor.execute(insert_query, values) 
                    connection.commit() 
                except mysql.connector.Error as err:
                    print(f"An error occurred: {err}") #

def comment_table():
    connection = mysql.connector.connect(host='localhost',user='root',password='12345',database='Youtube')
    mycursor = connection.cursor()
    
    try:
        query = """CREATE TABLE IF NOT EXISTS comments(comment_id VARCHAR(225) PRIMARY KEY, 
                                                    comment_text TEXT,
                                                    video_id VARCHAR(225), 
                                                    comment_author VARCHAR(225),
                                                    comment_published_At TIMESTAMP
                                                    )"""
    
        mycursor.execute(query)

    except mysql.connector.Error as err:
        print(f"An error occurred: {err}") 

    com_id_list_mgdb = [] 
    db = client["Youtube_Data"] 
    coll1 = db["channel_detials"]
    for com_data in coll1.find({},{"_id":0,"video_comment":1}):
        com_id_list_mgdb.append(com_data["video_comment"])

    com_id_list_sql = []
    query = '''SELECT comment_id FROM comments'''
    mycursor.execute(query)

   
    result = mycursor.fetchall()
    for data in result:
        com_id_list_sql.append(data[0]) # type: ignore   

    new_comment_info = []
    for com_id_mgdb in com_id_list_mgdb:
        if com_id_mgdb not in com_id_list_sql:
            new_comment_info.append(com_id_mgdb)

    for com_id_mgdb in new_comment_info:
        com_data = coll1.find_one({"channel_info.channel_id": com_id_mgdb}, {"_id": 0, "channel_info": 1})
        if com_data:
            df3=pd.DataFrame(com_data)
    
            for index,row in df3.iterrows():
                insert_query = """INSERT INTO comments(comment_id, 
                                                        comment_text,
                                                        comment_author,
                                                        video_id,
                                                        comment_published_At                
                                                        )
                                                        VALUES(%s,%s,%s,%s,%s)"""
                
                # Convert string to datetime object
                published_at = datetime.strptime(row['comment_published_At'], '%Y-%m-%dT%H:%M:%SZ')
                # Format datetime object as MySQL datetime string
                published_at_mysql = published_at.strftime('%Y-%m-%d %H:%M:%S')
            
                values=(row['comment_id'],
                        row['comment_text'],
                        row['comment_author'],
                        row['video_id'],
                        published_at_mysql
                        ) 
            
                try:                     
                    mycursor.execute(insert_query, values) 
                    connection.commit() 
                except mysql.connector.Error as err:
                    print(f"An error occurred: {err}") 

def show_channel_table():
    channel_name_list = [] 
    db = client["Youtube_Data"] 
    coll1 = db["channel_detials"] 

    for ch_data in coll1.find({},{'_id':0, 'channel_info':1}):
        channel_name_list.append(ch_data['channel_info']) 
    channel_table= st.dataframe(channel_name_list)
    return channel_table 

def show_playlist_table():
    playlist_name_list = [] 
    db = client["Youtube_Data"] 
    coll1 = db["channel_detials"] 

    for pl_data in coll1.find({},{'_id':0, 'playlist_info':1}):
        for index in range(len(pl_data['playlist_info'])):
            playlist_name_list.append(pl_data['playlist_info'][index]) 
    playlist_table= st.dataframe(playlist_name_list) 
    return playlist_table  

def show_video_table():
    videos_name_list = [] 
    db = client["Youtube_Data"] 
    coll1 = db["channel_detials"] 

    for vi_data in coll1.find({},{'_id':0, 'video_detail':1}):
        for index in range(len(vi_data['video_detail'])):
            videos_name_list.append(vi_data['video_detail'][index]) 
    video_table= st.dataframe(videos_name_list) 
    return video_table  

def show_comment_table():
    comment_name_list = [] 
    db = client["Youtube_Data"] 
    coll1 = db["channel_detials"] 

    for com_data in coll1.find({},{'_id':0, 'video_comment':1}):
        for index in range(len(com_data['video_comment'])):
            comment_name_list.append(com_data['video_comment'][index]) 
    comment_table= st.dataframe( comment_name_list) 
    return comment_table  

#Streamlit part
st.sidebar.title(':rainbow[YOUTUBE DATA HAVERSTING TOOL]') #create title in streamlit in rainbow font color
selection = st.sidebar.selectbox("Menu", ['Application Details', 'Sample Process','Extraction Data', 'View Data',
                                          'Migrate to MySQL','Analysis using SQL']) #create the selectbox in streamlit code

#select condition for selectbox to view
if selection == 'Application Details': #select condition for selectbox to view menu application details
    st.title(''' YouTube Data Harvesting and Warehousing using SQL, MongoDB, and Streamlit''') 
    st.markdown('''This is a project aimed at collecting,storing, and visualizing data from YouTube using various technologies.
                Let's break down each component:''')

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


#select condition for selectbox to view Sample Process
elif selection == 'Sample Process':
        st.title("Extract Detial for Channel ID")

        def api_connect():
            api_key = "AIzaSyCqfhCj2FiLuTqknpTTbl24eFcRe05WpzM" 
            api_service_name = "youtube"
            api_version = "v3"
            youtube = build(api_service_name, api_version, developerKey=api_key)
            return youtube
    
        def get_channel_data(youtube, channel_id):
            request = youtube.channels().list(part = "snippet,contentDetails,statistics",id = channel_id)
            channel_data = request.execute()
            return channel_data
        
        def get_video_data(youtube, channel_id):
            request = youtube.search().list(part="snippet",channelId=channel_id,type="video")
            video_data = request.execute()
            return video_data

        def get_comment_data(youtube, video_id):
            request = youtube.commentThreads().list(part="snippet",videoId=video_id)
            comment_data = request.execute()
            return comment_data

        channel_id = st.text_input("Enter Channel ID:")

        if st.button("Extract Data"):
            if channel_id:
                with st.spinner("Fetching channel data..."):
                    youtube = api_connect()

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
                    st.write(":red[Channel Description  :]", snippet.get('description'))
                    st.write(":red[Subscriber Count :]", statistics.get('subscriberCount'))
                    st.write(":red[Channel Views    :]", statistics.get('viewCount'))
                    st.write(":red[Video Count  :]", statistics.get('videoCount'))
                    date_string = snippet.get('publishedAt')
                    parsed_date = datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%SZ')
                    formatted_date = parsed_date.strftime("%d-%m-%Y")
                    st.write(":red[Published at :]", formatted_date)

                    video_data = get_video_data(youtube, channel_id)
                    if 'items' in video_data and len(video_data['items']) > 0:
                        video_info = video_data['items'][0]
                        video_snippet = video_info['snippet']
                        video_thumbnail = video_snippet['thumbnails']['medium']
                        video_id = video_info['id']['videoId']

                        st.subheader(":red[Video Details :]")

                        st.write(":red[Video Thumbnail :]")
                        st.image(video_thumbnail.get('url'))
                        st.write(":red[Video Title :]", video_snippet.get('title'))
                        st.write(":red[Video Description  :]", video_snippet.get('description'))
                        st.write(":red[Video View Count :]", statistics.get('viewCount'))
                        st.write(":red[Video Published at :]", video_snippet.get('publishedAt'))
                        
                        comment_data = get_comment_data(youtube, video_id)
                        if 'items' in comment_data and len(comment_data['items']) > 0:
                            comment_info = comment_data['items'][0]['snippet']['topLevelComment']['snippet']

                            st.subheader(":red[Video Comment:]")

                            st.write(":red[Comment Text :]", comment_info.get('textDisplay'))
                            st.write(":red[Comment Author :]", comment_info.get('authorDisplayName'))
                            st.write("Published at:", comment_info.get('publishedAt'))

                    else:
                        st.warning("No comments found for this video.")
                else:
                    st.warning("No videos found for this channel.")
            else:
                st.warning("Channel ID not valid.")

elif selection == 'Extraction Data':
    st.title('Extraction Data for Youtube')

    channel_id = st.text_input("Enter the Channel ID")

    if st.button("Collect and Store"):
        with st.spinner("Fetching channel information all....."):
            ch_ids = []
            db = client["Youtube_Data"]
            coll1 = db["channel_detials"]
            for ch_data in coll1.find({},{'_id':0, 'channel_info':1}):
                ch_ids.append(ch_data["channel_info"]['channel_id'])
        
            if channel_id in ch_ids:
                st.success("Channel Detail of the given channel id already exists")

            else:
                insert = channel_details(channel_id)
                st.success(insert)

elif selection == 'View Data':
    st.title('View Data in MongoDB')

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

    def tables():
        channel_table()
        playlist_table()
        videos_table()
        comment_table()

        return "Table Create Successfully to SQL!"

    if st.button("Transfer MongoDB Data to SQL Server"):
        with st.spinner("Please wait data transfer form MongoDB to Mysql server..."):
            Table = tables()
            st.success(Table)

    st.markdown('''Data Already in MySQL''')

    connection = mysql.connector.connect(host='localhost',user='root',password='12345',database='Youtube')
    mycursor = connection.cursor()

    query = '''SELECT channel_name,channel_id FROM channels'''
    mycursor.execute(query)
    result = mycursor.fetchall()
    df=pd.DataFrame(result,columns=["channel_name","channel_id"])
    st.write(df)

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

        q1=mycursor.fetchall()
        df=pd.DataFrame(q1,columns=["video_title","channel_name"])
        st.write(df)

    elif question == "2: Which channels have the most number of videos, and how many videos do they have?":
        question2 = '''SELECT video_count, channel_name FROM channels ORDER BY video_count DESC'''
        mycursor.execute(question2)

        q2=mycursor.fetchall()
        df2=pd.DataFrame(q2,columns=["channel_name","video_count"])
        st.write(df2)

    elif question == "3: What are the top 10 most viewed videos and their respective channels?":
        question3 = '''SELECT view_count, channel_name FROM videos ORDER BY view_count DESC LIMIT 10'''
        mycursor.execute(question3)

        q3=mycursor.fetchall()
        df3=pd.DataFrame(q3,columns=["view_count","channel_name"])
        st.write(df3)

    elif question == "4: How many comments were made on each video, and what are their corresponding video names?":
        question4 = '''SELECT comment_count, video_title FROM videos'''
        mycursor.execute(question4)

        q4=mycursor.fetchall()
        df4=pd.DataFrame(q4,columns=["comment_count","video_title"])
        st.write(df4)

    elif question == "5: Which videos have the highest number of likes, and what are their corresponding channel names?":
        question5 = '''SELECT likes_count, video_title, channel_name FROM videos ORDER BY likes_count DESC'''
        mycursor.execute(question5)

        q5=mycursor.fetchall()
        df5=pd.DataFrame(q5,columns=["likes_count","video_title", "channel_name"])
        st.write(df5)

    elif question == "6: What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
        question6 = '''SELECT likes_count,video_title FROM videos'''
        mycursor.execute(question6)

        q6=mycursor.fetchall()
        df6=pd.DataFrame(q6,columns=["likes_count","video_title"])
        st.write(df6)

    elif question == "7: What is the total number of views for each channel, and what are their  corresponding channel names?":
        question7 = '''SELECT channel_name, SUM(channel_views) AS total_views FROM channels GROUP BY channel_name'''
        mycursor.execute(question7)

        q7=mycursor.fetchall()
        df7=pd.DataFrame(q7,columns=["channel_name", "channel_views"])
        st.write(df7)

    elif question == "8: What are the names of all the channels that have published videos in the year 2022?":
        question8 = '''SELECT channel_name, YEAR(published_at) AS publication_year FROM videos WHERE YEAR(published_at) = 2022'''
        mycursor.execute(question8)

        q8=mycursor.fetchall()
        df8=pd.DataFrame(q8,columns=["channel_name", "publication_year"])
        st.write(df8)

    elif question == "9: What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        question9 = '''SELECT AVG(duration) AS average_duration, channel_name FROM videos GROUP BY channel_name'''
        mycursor.execute(question9)

        q9=mycursor.fetchall()
        df9=pd.DataFrame(q9,columns=["average_duration", "channel_name"])
        st.write(df9)

    elif question == "10: Which videos have the highest number of comments, and what are their  corresponding channel names?":
        question10 = '''SELECT channel_name, comment_count FROM videos ORDER BY comment_count DESC LIMIT 10'''
        mycursor.execute(question10)

        q10=mycursor.fetchall()
        df10=pd.DataFrame(q10,columns=["channel_name","comment_count"])
        st.write(df10)
import pymongo
import sqlite3
import streamlit as st
import json

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["youtube_database"]
collection = db["videos"]

# Connect to SQLite
sqlite_conn = sqlite3.connect("youtube_data.db")
sqlite_cursor = sqlite_conn.cursor()

# Create a table in SQLite to store the data
sqlite_cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS videos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT,
        channel TEXT,
        views INTEGER,
        likes INTEGER,
        dislikes INTEGER
    )
    """
)

# Streamlit application
def main():
    st.title("YouTube Data Harvesting and Warehousing")

    # Collect data and store in both MongoDB and SQLite
    if st.button("Harvest Data"):
        video_title = st.text_input("Video Title")
        video_channel = st.text_input("Channel")
        video_views = st.number_input("Views", min_value=0, step=1)
        video_likes = st.number_input("Likes", min_value=0, step=1)
        video_dislikes = st.number_input("Dislikes", min_value=0, step=1)

        if st.button("Save"):
            # Store in MongoDB
            video_data = {
                "title": video_title,
                "channel": video_channel,
                "views": video_views,
                "likes": video_likes,
                "dislikes": video_dislikes,
            }
            collection.insert_one(video_data)

            # Store in SQLite
            sqlite_cursor.execute(
                """
                INSERT INTO videos (title, channel, views, likes, dislikes)
                VALUES (?, ?, ?, ?, ?)
                """,
                (video_title, video_channel, video_views, video_likes, video_dislikes),
            )
            sqlite_conn.commit()

            st.success("Video data saved successfully.")

    # Import and insert multiple videos from a JSON file
    if st.button("Import Videos"):
        uploaded_file = st.file_uploader("Upload JSON file", type="json")

        if uploaded_file is not None:
            videos = json.load(uploaded_file)

            # Insert videos in MongoDB
            collection.insert_many(videos)

            # Insert videos in SQLite
            sqlite_cursor.executemany(
                """
                INSERT INTO videos (title, channel, views, likes, dislikes)
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    (
                        video["title"],
                        video["channel"],
                        video["views"],
                        video["likes"],
                        video["dislikes"],
                    )
                    for video in videos
                ],
            )
            sqlite_conn.commit()

            st.success("Videos imported successfully.")

    # Show the collected data
    if st.button("Show Data"):
        st.subheader("Data from MongoDB")
        mongo_data = list(collection.find())
        for video in mongo_data:
            st.write(video)

        st.subheader("Data from SQLite")
        sqlite_cursor.execute("SELECT * FROM videos")
        sqlite_data = sqlite_cursor.fetchall()
        for video in sqlite_data:
            st.write(video)


if __name__ == "__main__":
    main()


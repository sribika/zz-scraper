import sqlite3
import time
import requests
from bs4 import BeautifulSoup
from keep_alive import keep_alive

keep_alive()

# Function to fetch links from the database that don't have data yet
def fetch_unscraped_video_links(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT video_id, video_url FROM video_details WHERE video_title IS NULL;")
    video_links = cursor.fetchall()
    conn.close()
    print('links fetched')
    return video_links

# Function to scrape data from each URL
def scrape_video_data(video_url):
    try:
        # Make an HTTP GET request to the URL
        response = requests.get(video_url)
        response.raise_for_status()  # Raise an exception for bad responses

        soup = BeautifulSoup(response.text, 'html.parser')

        # Extracting title
        title_element = soup.select_one('.sc-1b6bgon-3.iTXrhy')
        video_title = title_element.text if title_element else ""

        # Extracting date
        date_element = soup.select_one('.sc-1b6bgon-2.jkEzeg')
        video_date = date_element.text if date_element else ""

        # Extracting description
        description_element = soup.select_one('.sc-xz1bz0-0.lgrCSo p.font-primary')
        description = description_element.text if description_element else ""

        # Extracting image URL and ID
        poster_div = soup.select_one('.vjs-poster')
        if poster_div:
            style_attribute = poster_div.get('style')
            image_url = style_attribute.split('url("')[1].split('")')[0]
        else:
            alternative_image_div = soup.select_one('.sc-tg5e7m-3.eogrCF img')
            image_url = alternative_image_div['src'] if alternative_image_div else ""

        # Extracting video trailer
        video_element = soup.select_one('video.vjs-tech')
        video_trailer = video_element['src'] if video_element else ""

        # Extracting tags if the button is found
        tags_button = soup.select('.sc-1rsnn24-3.kLrKBL')
        tags = [tag.get_text() for tag in tags_button]

        # Extracting cast
        cast_elements = soup.select(".sc-1b6bgon-5.dOlAiq a.sc-1b6bgon-8.YbRYu")
        cast_names = [element.get_text() for element in cast_elements]
        cast_names_string = ', '.join(cast_names)
        cast_names = [cast.strip() for cast in cast_names_string.split(',')]

        print('data scraped')
        return {
            'video_title': video_title,
            'video_date': video_date,
            'description': description,
            'image_url': image_url,
            'video_trailer': video_trailer,
            'tags': tags,
            'cast_names': cast_names,
        }

    except requests.exceptions.RequestException as e:
        print(f"Error scraping data for {video_url}: {e}")
        return None

# Function to insert data into the database
def insert_data(video_id, data, cast_names, tags, db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        UPDATE video_details
        SET video_title=?, video_date=?, description=?, image_url=?, video_trailer=?
        WHERE video_id=?
        """, (data['video_title'], data['video_date'], data['description'],
              data['image_url'], data['video_trailer'], video_id))

    for cast_name in cast_names:
        cursor.execute(
            '''
            INSERT OR IGNORE INTO cast (cast_name) VALUES (?)
            ''', (cast_name, ))

    for tag in tags:
        cursor.execute(
            '''
            INSERT OR IGNORE INTO tags (tag_name) VALUES (?)
            ''', (tag, ))

    cursor.execute("SELECT video_id FROM video_details WHERE video_id = ?", (video_id,))
    result = cursor.fetchone()

    for cast_name in cast_names:
        cursor.execute(
            '''
            INSERT INTO video_cast (video_id, cast_id)
            VALUES (?, (SELECT cast_id FROM cast WHERE cast_name = ?))
            ''', (video_id, cast_name))

    for tag in tags:
        cursor.execute(
            '''
            INSERT INTO video_tags (video_id, tag_id)
            VALUES (?, (SELECT tag_id FROM tags WHERE tag_name = ?))
            ''', (video_id, tag))

    print('data inserted for video:', video_id)

    conn.commit()
    conn.close()

# Provide the path to your SQLite database file
db_path = 'DB-13plus-without-vid-prev.db'

# Fetch video links from the database that don't have data yet
video_links = fetch_unscraped_video_links(db_path)

# Scrape data and update the database
for video_id, video_url in video_links:
    scraped_data = scrape_video_data(video_url)

    if scraped_data is not None:
        insert_data(video_id, scraped_data, scraped_data['cast_names'],
                    scraped_data['tags'], db_path)
    else:
        print('Failed to scrape data for video ID', video_id)

    time.sleep(2)

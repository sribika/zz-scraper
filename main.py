import sqlite3
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import WebDriverException
from keep_alive import keep_alive

keep_alive()


# Function to fetch links from the database that don't have data yet
def fetch_unscraped_video_links(db_path):
  conn = sqlite3.connect(db_path)
  cursor = conn.cursor()
  cursor.execute(
    "SELECT video_id, video_url FROM video_details WHERE video_title IS NULL;")
  video_links = cursor.fetchall()
  conn.close()
  print('links fetched')
  return video_links


# Function to scrape data from each URL
def scrape_video_data(driver, video_url):
  # Scraping logic for each video_url
  try:
    driver.get(video_url)
    print('connecting to url:', video_url)
    soup = BeautifulSoup(driver.page_source, 'html.parser')

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
    #   image_url_id = image_url.split(
    #     'media-public-ht.project1content.com/m=eaSaaTbWx/')[-1].split(
    #       '/poster/')[0]
    else:
      # If no poster_div is found, check for the alternative structure
      alternative_image_div = soup.select_one('.sc-tg5e7m-3.eogrCF img')
      image_url = alternative_image_div['src'] if alternative_image_div else ""
    #   image_url_id = image_url.split(
    #     'media-public-ht.project1content.com/m=eaSaaTbWx/')[-1].split(
    #       '/poster/')[0]

    # Extracting video trailer
    video_element = soup.select_one('video.vjs-tech')
    video_trailer = video_element['src'] if video_element else ""
    # video_trailer_id = video_trailer.split('project1content.com/')[-1].split(
    #   '/video/')[0]

    # Creating video preview (currently disabled)
    # /mediabook/mediabook_320p.mp4
    # /video/trailer_320p.mp4
    # video_preview = 'https://prog-public-ht.project1content.com/{}/mediabook/mediabook_320p.mp4'.format(
    #   image_url_id)

    # Extracting tags if the button is found
    tags_button = driver.find_elements(By.CSS_SELECTOR, '.sc-1rsnn24-3.kLrKBL')
    if tags_button:
      driver.execute_script("arguments[0].click();", tags_button[0])
      initial_tags = soup.select('.sc-vdkjux-0.hVYLuF')
      tags = [tag.get_text() for tag in initial_tags if tag.get_text().strip()]
    else:
      tags = []

    # Extracting cast
    cast_elements = soup.select(".sc-1b6bgon-5.dOlAiq a.sc-1b6bgon-8.YbRYu")
    cast_names = [element.get_text() for element in cast_elements]
    cast_names_string = ', '.join(cast_names)
    # Split cast_names_string into a list of cast names
    cast_names = [cast.strip() for cast in cast_names_string.split(',')]

    print('data scraped')
  except WebDriverException as e:
    print(f"Error scraping data for {video_url}: {e}")
    # Optionally, you can add code here to handle the error, such as reloading the page
    # driver.refresh()
    time.sleep(2)  # Add a brief pause before retrying
    return None

  return {
    'video_title': video_title,
    'video_date': video_date,
    'description': description,
    'image_url': image_url,
    # 'image_url_id': image_url_id,
    'video_trailer': video_trailer,
    # 'video_trailer_id': video_trailer_id,
    # 'video_preview': video_preview,
    'tags': tags,
    'cast_names': cast_names,  # Use cast_names directly
  }


# Function to insert data into the database
def insert_data(video_id, data, cast_names, tags, db_path):
  conn = sqlite3.connect(db_path)
  cursor = conn.cursor()

  # Update video_details table
  cursor.execute(
    """
        UPDATE video_details
        SET video_title=?, video_date=?, description=?, image_url=?, video_trailer=?
        WHERE video_id=?
    """, (data['video_title'], data['video_date'], data['description'],
          data['image_url'], data['video_trailer'], video_id))

  # Insert cast names into the cast table if not already present
  for cast_name in cast_names:
    cursor.execute(
      '''
            INSERT OR IGNORE INTO cast (cast_name) VALUES (?)
            ''', (cast_name, ))

  # Insert tags into the tags table if not already present
  for tag in tags:
    cursor.execute(
      '''
            INSERT OR IGNORE INTO tags (tag_name) VALUES (?)
            ''', (tag, ))

  # Get the last inserted video_id
  cursor.execute("SELECT video_id FROM video_details WHERE video_id = ?",
                 (video_id, ))
  result = cursor.fetchone()

  # Insert relations between video and cast into video_cast table
  for cast_name in cast_names:
    cursor.execute(
      '''
            INSERT INTO video_cast (video_id, cast_id)
            VALUES (?, (SELECT cast_id FROM cast WHERE cast_name = ?))
            ''', (video_id, cast_name))

  # Insert relations between video and tags into video_tags table
  for tag in tags:
    cursor.execute(
      '''
            INSERT INTO video_tags (video_id, tag_id)
            VALUES (?, (SELECT tag_id FROM tags WHERE tag_name = ?))
            ''', (video_id, tag))

  print('data inserted for video:', video_id)

  # Commit changes to the database
  conn.commit()
  conn.close()


# Provide the path to your SQLite database file
db_path = 'DB-13plus-without-vid-prev.db'

# Fetch video links from the database that don't have data yet
video_links = fetch_unscraped_video_links(db_path)

# Initialize the Chrome driver outside the loop
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
driver = webdriver.Chrome(options=chrome_options)

# Scrape data and update the database
for video_id, video_url in video_links:
  scraped_data = scrape_video_data(driver, video_url)

  # Check if the scraping was successful
  if scraped_data is not None:
    insert_data(video_id, scraped_data, scraped_data['cast_names'],
                scraped_data['tags'], db_path)
  else:
    # Optionally, you can add code here to handle the case when scraping fails
    # For example, you may want to log the failure or take some other action
    print('Failed to scrape data for video ID', video_id)

  # Optionally, add a time delay between requests to avoid overloading the server
  time.sleep(2)

# Close the browser outside the loop
driver.quit()

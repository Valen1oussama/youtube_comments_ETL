import pandas as pd
from sqlalchemy import create_engine
from google.oauth2 import service_account

# Load the service account credentials from a JSON file you downloaded when creating your credentials
credentials = service_account.Credentials.from_service_account_file(
    'retrieve-comments-fc6e2ce1ebdd.json', scopes=['https://www.googleapis.com/auth/youtube.force-ssl']
  )

# Create a YouTube Data API client with the credentials
from googleapiclient.discovery import build

youtube = build('youtube', 'v3', credentials=credentials)
video_id = 'qFaaKme5eDE'
comments = []
next_page_token = None

while True:
    request = youtube.commentThreads().list(
        part='snippet',
        videoId=video_id,
        maxResults=100,  # You can adjust this as needed
        pageToken=next_page_token
    )
    
    response = request.execute()
    comments.extend(response['items'])
    
    next_page_token = response.get('nextPageToken')
    
    if not next_page_token:
        break
# Now 'comments' contains the comments from the video.


import pandas as pd

# Assuming you have a 'comments' list containing comment data

# Create a DataFrame from the 'comments' list
df = pd.DataFrame(comments)

# Extract relevant columns (you may need to adjust this depending on your comment data structure)
df = df[['id', 'snippet']]

# Rename columns for clarity
df.rename(columns={'id': 'Comment ID', 'snippet': 'Snippet'}, inplace=True)

# Extract specific snippet details
df['Author'] = df['Snippet'].apply(lambda x: x['topLevelComment']['snippet']['authorDisplayName'])
df['Comment'] = df['Snippet'].apply(lambda x: x['topLevelComment']['snippet']['textDisplay'])
df['Published At'] = df['Snippet'].apply(lambda x: x['topLevelComment']['snippet']['publishedAt'])

# Drop the 'Snippet' column as it's no longer needed
df.drop(columns=['Snippet'], inplace=True)

# Specify the CSV file name
csv_file_name = 'comments.csv'

# Save the DataFrame to a CSV file
df.to_csv(csv_file_name, index=False, encoding='utf-8')

print(f'CSV file "{csv_file_name}" has been created.')



    # ... (your existing code for retrieving and processing data)

    # Read the CSV file into a DataFrame
df = pd.read_csv('comments.csv', encoding='utf-8')

    # Create a database connection
db_uri = 'mysql://root:oussama2001@localhost:3306/youtube_db'
engine = create_engine(db_uri)

    # Define the table name
table_name = 'youtube_db'

    # Write the DataFrame to the MySQL database
df.to_sql(table_name, engine, if_exists='replace', index=False)

    # Close the database connection
engine.dispose()

print(f'CSV data has been stored in the MySQL database.')


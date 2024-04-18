import pandas as pd
import requests
import base64
import pandas_gbq as pd_gbq
import os


client_id         = os.environ.get("CLI_ID")
client_secret     = os.environ.get("CLI_SEC")
project_id        = os.environ.get("PROJECT_ID")
table_path        = os.environ.get("TAB_PATH")
url_api           = os.environ.get("API_URL")

def api_spotify(event, context):

  limit=50
  offset=0
  df = []

  auth_header = base64.b64encode(bytes(client_id + ':' + client_secret, 'utf-8')).decode('utf-8')
  auth_url = 'https://accounts.spotify.com/api/token'

  auth_data = {
    'grant_type': 'client_credentials'
      }

  auth_response = requests.post(
    auth_url,
    headers={
        'Authorization': 'Basic ' + auth_header
    },
    data=auth_data
  )

  while True:  
    if auth_response.status_code == 200:
      token = auth_response.json()['access_token']
      page = f'&offset={offset}&limit={limit}&'
      url = url_api + page
      headers = {'Authorization': f'Bearer {token}'}
      response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
      data = response.json()
      print(data)
      
      df = []
      if 'items'in data:
        for episode in data['items']:
          id = episode['id']
          name = episode['name']
          description = episode['description']
          release_date = episode['release_date']
          duration_ms = episode['duration_ms']
          language = episode['language']
          explicit = episode['explicit']
          tp = episode['type']

          if 'Grupo Boticário' in name or 'Grupo Boticário' in description:
            dict = {'ID': id,'NAME': name,'DESCRIPTION': description, 'RELEASE_DATE' : release_date, 'DURATION_MS': duration_ms,'LANGUAGE': language,'EXPLICIT': explicit, 'TYPE': tp}
            df.append(dict)
      if not data['next']:
        break
        print('sem mais dados')
      else:
        offset += limit
    else:
      print(response.status_code)
        
    df = pd.DataFrame(df, columns=['ID', 'NAME', 'DESCRIPTION', 'RELEASE_DATE', 'DURATION_MS','LANGUAGE','EXPLICIT','TYPE'],dtype=str)
    pd_gbq.to_gbq(df, table_path, project_id = project_id, if_exists = 'append')  
import pandas as pd
import pandas_gbq as pd_gbq
from google.oauth2 import service_account
import requests
import base64
import os

client_id         = os.environ.get('CLI_ID')
client_secret     = os.environ.get('CLI_SEC')
project_id        = os.environ.get('PROJECT_ID')
table_path        = os.environ.get('TAB_PATH')
api_url           = os.environ.get('URL_API')

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

def api_spotify(event, context):
  if auth_response.status_code == 200:
    token = auth_response.json()['access_token']
    url = api_url
    headers = {'Authorization': f'Bearer {token}'}
    response = requests.get(url, headers=headers)

  df = []

  if response.status_code == 200:
    data = response.json()
    if "shows" in data:
      for show in data['shows']['items']:
          if show is not None:
            name = show['name']
            description = show['description']
            id = show['id']
            total_ep = show['total_episodes']
            dict = {'id': id,'name': name,'description': description, 'total_episodes': total_ep}
            df.append(dict)
    else:
      print("Pesquisa sem resultados.")
  else:
    print('Erro ao acessar api')
    
  df = pd.DataFrame(df, columns=['id', 'name', 'description', 'total_episodes'])
  pd_gbq.to_gbq(df, table_path, project_id = project_id, if_exists = 'append')
  
  return None

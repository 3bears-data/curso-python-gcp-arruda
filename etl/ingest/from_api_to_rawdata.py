import requests
import pandas as pd
from google.cloud import storage
import yaml


def gettoken():
    url = 'https://southamerica-east1-aula-gcp-arruda.cloudfunctions.net/api-for-class-get-customers/'
    nome = "leandroalves"


    #corpo json da requisicao
    data = {
        'username': nome  + '_aluno'
        ,'password': 'aulapythonapi'
    }

    # enviar a req post com o corpo json
    response = requests.post(url, json=data)

    # armazenar a resposta
    result = response.json()
    token = result['bearer'] 

    return token



if __name__ == '__main__':
    # ---- alimentar as variaveis a partir do yaml -----
    with open(r'C:\Users\leand\Desktop\py-gcp\config\config.yaml') as file:
        config = yaml.safe_load(file)

    bucket_raw = config['bucket-rawdata']
    credentials_path = config['credentials_path']

    tabela= "costumers"

    # ----- PRIMEIRO FAZ O POST PARA OBTER O TOKEN BEARER
    token = gettoken()
    print(token)

    # ----- DEPOIS FAZ O GET PRA OBTER O RESULTADO DO DADO
    header = {'token': 'Bearer ' + token}

    primeira_pagina = 1
    url = f"https://southamerica-east1-aula-gcp-arruda.cloudfunctions.net/api-for-class-get-customers?page={primeira_pagina}"

    response = requests.get(url, headers=header)

    data = response.json()['data']
    df = pd.DataFrame(data)

    # obter o total de paginas
    total_paginas = int(response.json()['admpages']['total_pages'])

    #lista de df's
    lista_de_dfs = [df]


    for pagina in range(2, total_paginas + 1):
        print(f"{pagina} de {total_paginas}")
        url = f"https://southamerica-east1-aula-gcp-arruda.cloudfunctions.net/api-for-class-get-customers?page={pagina}"

        response = requests.get(url, headers=header)
        data = response.json()['data']
        df = pd.DataFrame(data)
        lista_de_dfs.append(df)

    #concatena a lista de df em unico df
    df_final = pd.concat(lista_de_dfs, ignore_index=True)

    df = df_final.copy()

    # etapa de ingestao para o storage
    client_storage = storage.Client.from_service_account_json(credentials_path)

    if not df.empty:
        folder_path = f"oltp-north/{tabela}_x/{tabela}.csv"
        bucket = client_storage.bucket(bucket_raw)

        #cria csv a partir do df
        csv_data = df.to_csv(index=False, encoding='utf-8-sig', sep=";")
        csv_data = csv_data.encode('iso-8859-1')
    
        #upload pro storage
        blob = bucket.blob(folder_path)
        blob.upload_from_string(csv_data)

        print(f"Tabela {tabela} carregada no destino {folder_path}")

        

    


    







    



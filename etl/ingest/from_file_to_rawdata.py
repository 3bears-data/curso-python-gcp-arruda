import pandas as pd
from google.cloud import storage
import yaml

if __name__ == '__main__':
    # ---- alimentar as variaveis a partir do yaml -----
    with open(r'C:\Users\leand\Desktop\py-gcp\config\config.yaml') as file:
        config = yaml.safe_load(file)

    bucket_raw = config['bucket-rawdata']
    credentials_path = config['credentials_path']
    
    tabela= "products"

    # -----  lê o arquivo local
    df = pd.read_csv(r"C:\Users\leand\Desktop\py-gcp\docs\products.csv", sep=";")

    # ---- encaminhar para o storage
    client_storage = storage.Client.from_service_account_json(credentials_path)

    if not df.empty:
        folder_path = f"oltp-north/{tabela}_x/{tabela}.csv"
        bucket = client_storage.bucket(bucket_raw)

        #cria csv a partir do df
        df['reg'] = 1
        csv_data = df.to_csv(index=False, encoding='utf-8-sig', sep=";")
        csv_data = csv_data.encode('iso-8859-1')
    
        #upload pro storage
        blob = bucket.blob(folder_path)
        blob.upload_from_string(csv_data)

        print(f"Tabela {tabela} carregada no destino {folder_path}")


    
import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

# Carrega as variáveis de ambiente
load_dotenv()
AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

# Configurações do Azure e diretórios
CONTAINER_NAME = "bronze"
CLOUD_FOLDER = "olist/"  # Subpasta na nuvem para organizar os dados de e-commerce
LOCAL_DATA_DIR = "../data"

def upload_arquivos():
    print("Conectando ao Azure Data Lake...")
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    
    # Busca todos os arquivos CSV na pasta data local
    arquivos_csv = [f for f in os.listdir(LOCAL_DATA_DIR) if f.endswith('.csv')]
    
    if not arquivos_csv:
        print("Nenhum arquivo CSV encontrado na pasta data/.")
        return

    print(f"Encontrados {len(arquivos_csv)} arquivos. Iniciando upload para a Camada Bronze...")
    
    for arquivo in arquivos_csv:
        caminho_local = os.path.join(LOCAL_DATA_DIR, arquivo)
        nome_blob = f"{CLOUD_FOLDER}{arquivo}"  # Ex: olist/olist_customers_dataset.csv
        
        print(f"Enviando: {arquivo}")
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=nome_blob)
        
        with open(caminho_local, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
            
    print("\nSucesso! Todos os arquivos do Olist foram enviados para o Azure.")

if __name__ == "__main__":
    upload_arquivos()
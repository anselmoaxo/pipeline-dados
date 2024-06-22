import requests
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi


def connect_mongo(uri):
    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))

    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)
    
    return client

def create_connect_db(client, db_name):
    db = client[db_name]
    return db
    
    
def create_connect_collection(db, col_name):
    return db[col_name]
   
    
def extract_api_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        response = response.json()
    else:
        print(f'Erro da API : {response.status_code}')
    return response

def insert_data(col, data):
    result = col.insert_many(data)
    n_docs_inseridos = len(result.inserted_ids)
    return n_docs_inseridos 



if __name__ == "__main__":
    
    client = connect_mongo("mongodb+srv://anselmoaxo:1234@cluster-pipeline.tpta7jc.mongodb.net/?retryWrites=true&w=majority&appName=Cluster-pipeline")
    db = create_connect_db(client, "db_produtos_desafio")
    col = create_connect_collection(db, "produtos")

    data = extract_api_data("https://labdados.com/produtos")
    print(f"\nQuantidade de dados extraidos: {len(data)}")

    n_docs = insert_data(col, data)
    print(f"\nDocumentos inseridos na colecao: {n_docs}")

    client.close()
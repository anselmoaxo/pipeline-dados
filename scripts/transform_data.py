from extract_and_save_data import connect_mongo, create_connect_db, create_connect_collection
import pandas as pd

def visualize_collection(col):
    for doc in col.find():
        print(doc)
        
def rename_column(col, col_name, new_name):
    col.update_many({},{"$rename" : {"lat": "lagitude", "lon": "longitude" }})
    
def select_category(col, category):
    cursor = col.find({"Categoria do Produto": "livros"})

    lista_livros =[]
    for doc in cursor:
        lista_livros.append(doc)
    return lista_livros
        
def make_regex(col, regex):
    query = {"Data da Compra": {"$regex": f"{regex}"}}

    lista_regex = []
    for doc in col.find(query):
        lista_regex.append(doc)
    
    return lista_regex

def create_dataframe(lista):
    df =  pd.DataFrame(lista)
    return df

def format_date(df):
    df["Data da Compra"] = pd.to_datetime(df["Data da Compra"], format="%d/%m/%Y")
    df['Data da Compra'] = df['Data da Compra'].dt.strftime('%Y-%m-%d')

def save_csv(df, path):
    df.to_csv(path, index=False)
    print(f"\nO arquivo {path} foi salvo")
    


if __name__ == "__main__":

    # estabelecendo a conexão e recuperando os dados do MongoDB
    client = connect_mongo("mongodb+srv://anselmoaxo:1234@cluster-pipeline.tpta7jc.mongodb.net/?retryWrites=true&w=majority&appName=Cluster-pipeline")
    db = create_connect_db(client, "db_produtos_desafio")
    col = create_connect_collection(db, "produtos")

    # renomeando as colunas de latitude e longitude
    rename_column(col, "lat", "Latitude")
    rename_column(col, "lon", "Longitude")

    # salvando os dados da categoria livros
    lst_livros = select_category(col, "livros")
    df_livros = create_dataframe(lst_livros)
    format_date(df_livros)
    save_csv(df_livros, "data_teste/tb_livros.csv")

    # salvando os dados dos produtos vendidos a partir de 2021
    lst_produtos = make_regex(col, "/202[1-9]")
    df_produtos = create_dataframe(lst_produtos)
    format_date(df_produtos)
    save_csv(df_produtos, "data_teste/tb_produtos.csv")
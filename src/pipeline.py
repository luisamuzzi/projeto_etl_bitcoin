#==============================================
# Libraries
#==============================================
import requests
import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime
import time

#==============================================
# Carga das configurações
#==============================================

# Carregar as configurações do arquivo .env
load_dotenv()

#==============================================
# Funções
#==============================================

# Função para extrair dados do Bitcoin
def extract_bitcoin_data():
    """
    Essa função extrai os dados do Bitcoin via API.
    
    Parâmetros:
    - A função não recebe parêmetros de entrada.

    Retorno:
    - Os dados extraídos em formato JSON.
    """
    url = "http://api.coinbase.com/v2/prices/spot"

    response = requests.get(url)
    data = response.json()

    return data

# Função para transformar os dados
def transform_bitcoin_data(data):
    """
    Essa função realiza transformações nos dados extraídos para prepará-los para inserção em banco de dados estruturado.

    Parâmetros:
    - data: Dados em formato JSON do Bitcoin extraídos via API

    Retorno:
    - Um dicionário contendo os dados de valor, criptomoeda, moeda e timestamp.
    """
    valor = data['data']['amount']
    criptomoeda = data['data']['base']
    moeda = data['data']['currency']
    timestamp = datetime.now()

    transformed_data = {
        'valor': valor,
        'criptomoeda': criptomoeda,
        'moeda': moeda,
        'timestamp': timestamp
    }
    return transformed_data

# Função para criar tabela no banco de dados
def create_table():
    """
    Essa função se conecta ao banco de dados PostgreSQL e cria uma tabela na qual serão armazenados os dados do Bitcoin, caso ela ainda não exista.

    Parâmetros:
    - A função não recebe parêmetros de entrada.

    Retorno:
    - A função não tem valor de retorno.
    """
    try:
        conn = psycopg2.connect(
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT')
        )
        with conn.cursor() as cur:
            cur.execute("""
                        CREATE TABLE IF NOT EXISTS bitcoin_table (
                        id SERIAL PRIMARY KEY,
                        valor NUMERIC NOT NULL,
                        criptomoeda VARCHAR(10) NOT NULL,
                        moeda VARCHAR(10) NOT NULL,
                        timestamp TIMESTAMP NOT NULL
                        )
                        """)
            conn.commit()
            print("Tabela criada/verificada com sucesso!")
    except Exception as e:
        print(f'Erro ao criar/verificar tabela {e}')
    finally:
        if conn:
            conn.close()

# Função para carregar os dados no PostgreSQL
def load_bitcoin_postgres(data):
    """
    Essa função se conecta ao banco de dados PostgreSQL e insere os dados de valor, criptomoeda, moeda e timestamp.

    Parâmetros:
    - data: um dicionário contendo os dados de valor, criptomoeda, moeda e timestamp.

    Retorno:
    - A função não tem valor de retorno.
    """
    try:
        conn=psycopg2.connect(
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT')
        )
        with conn.cursor() as cur:
            cur.execute("""
                        INSERT INTO bitcoin_table (valor, criptomoeda, moeda, timestamp)
                        VALUES(%s, %s, %s, %s)
                        """, (data['valor'], data['criptomoeda'], data['moeda'], data['timestamp'])
                        )
            conn.commit()
            print('Carregamento realizado com sucesso!')
    except Exception as e:
        print(f'Erro ao carregar os dados: {e}')
    finally:
        if conn:
            conn.close()

#----------------------------------------Execução----------------------------------------

if __name__=='__main__':
    # Criação da tabela para inicialização
    create_table()

    try:
        # Loop principal
        while True: 
            data = extract_bitcoin_data()
            transformed_data = transform_bitcoin_data(data)
            load_bitcoin_postgres(transformed_data)
            time.sleep(15)
    except KeyboardInterrupt:
        print('Execução foi interrompida pelo usuário.')
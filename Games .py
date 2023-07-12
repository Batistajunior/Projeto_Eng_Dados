#!/usr/bin/env python
# coding: utf-8

# In[1]:


import csv
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound

# Configurações do BigQuery
project_id = "essential-storm-358400"
dataset_id = "games_db"
table_id = "games"

# Credenciais de autenticação para o cliente BigQuery
games_credentials_path = "/Users/batistajunior/Downloads/essential-storm-358400-0a368f1550ff.json"
games_creds = service_account.Credentials.from_service_account_file(games_credentials_path)

# Criar uma instância do cliente BigQuery com as credenciais
client = bigquery.Client(project=project_id, credentials=games_creds)

# Verificar se o conjunto de dados já existe
dataset_ref = client.dataset(dataset_id)

try:
    dataset = client.get_dataset(dataset_ref)
    print(f"Conjunto de dados '{dataset_id}' já existe.")
except NotFound:
    # Criar o conjunto de dados
    dataset = bigquery.Dataset(dataset_ref)
    dataset = client.create_dataset(dataset)
    print(f"Conjunto de dados '{dataset_id}' criado com sucesso.")

# URL da API para obter informações sobre jogos populares
games_url = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"

# Número máximo de jogos a serem obtidos
games_max_games = 220

# Envie uma solicitação GET para a API
games_response = requests.get(games_url)

# Verifique se a solicitação foi bem-sucedida (código de status 200)
if games_response.status_code == 200:
    # A resposta da API contém um objeto JSON com uma lista de jogos
    games_data = games_response.json()

    # Acesse a lista de jogos
    games_list = games_data["applist"]["apps"]

    # Limita o número de jogos de acordo com o valor máximo definido
    games_list = games_list[:games_max_games]

    # Caminho do arquivo CSV de saída para "games"
    games_output_file = "/Users/batistajunior/Downloads/games_data.csv"

    # Crie uma lista para armazenar os dados a serem inseridos no BigQuery
    games_rows_to_insert = []

    # Itere sobre os jogos e obtenha informações detalhadas
    for game in games_list:
        app_id = game["appid"]
        app_name = game["name"]

        # URL da API para obter informações detalhadas sobre um jogo específico
        game_info_url = f"https://store.steampowered.com/api/appdetails?appids={app_id}"

        # Envie uma solicitação GET para a API de informações do jogo
        game_info_response = requests.get(game_info_url)

        # Verifique se a solicitação foi bem-sucedida (código de status 200)
        if game_info_response.status_code == 200:
            # A resposta da API contém um objeto JSON com as informações do jogo
            game_info_data = game_info_response.json()

            # Verifique se as informações do jogo estão disponíveis
            if str(app_id) in game_info_data and "data" in game_info_data[str(app_id)]:
                game_info = game_info_data[str(app_id)]["data"]

                # Acesse as informações detalhadas do jogo relevantes para o BigQuery
                game_description = game_info.get("short_description", "N/A")
                game_developer = game_info.get("developers", ["N/A"])[0]
                game_publisher = game_info.get("publishers", ["N/A"])[0]
                game_release_date = game_info.get("release_date", {}).get("date", "N/A")
                game_genre = game_info.get("genres", [{"description": "N/A"}])[0]["description"]

                # Adicione os dados à lista de linhas a serem inseridas no BigQuery
                games_rows_to_insert.append((app_id, app_name, game_description, game_developer, game_publisher,
                                             game_release_date, game_genre))

    # Escreva os dados em um arquivo CSV
    with open(games_output_file, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["ID", "Nome", "Descrição", "Desenvolvedor", "Publicador", "Data de lançamento", "Gênero"])
        writer.writerows(games_rows_to_insert)

    print("Dados da tabela 'games' exportados para o arquivo CSV.")

    # Crie um DataFrame pandas com os dados
    df = pd.DataFrame(games_rows_to_insert, columns=["ID", "Nome", "Descrição", "Desenvolvedor", "Publicador",
                                                     "Data de lançamento", "Gênero"])

    # Exiba o DataFrame usando o método display
    display(df)

    # Caminho do arquivo CSV de entrada
    input_file = "/Users/batistajunior/Downloads/games_data.csv"

    # Crie o job de carregamento para a tabela "games"
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1  # Pular a primeira linha (cabeçalho) do CSV
    job_config.autodetect = True  # Permitir que o BigQuery determine automaticamente o esquema
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE  # Sobrescrever dados existentes

    # Carregar o arquivo CSV na tabela do BigQuery para "games"
    with open(input_file, "rb") as data_file:
        job = client.load_table_from_file(data_file, table_ref, job_config=job_config)

    job.result()  # Aguardar a conclusão do job

    print("Dados da tabela 'games' carregados com sucesso no BigQuery.")
else:
    print("Falha na solicitação. Código de status:", games_response.status_code)


# In[ ]:





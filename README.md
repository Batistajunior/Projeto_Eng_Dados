# Projeto_Eng_Dados

## Automação para obter informações de jogos populares do Steam e carregá-las no BigQuery


### Executando a automação

Para executar a automação e obter informações de jogos populares do Steam e carregá-las no BigQuery, siga as etapas abaixo:

1. Clone este repositório em sua máquina local.

2. Certifique-se de ter as seguintes dependências instaladas:
   - Python 3.x
   - Pacotes: google-cloud-bigquery, google-auth, pandas, requests

3. Abra o arquivo `Games.py` em seu editor de texto.

4. No início do arquivo, defina as credenciais de autenticação para o BigQuery e o caminho para o arquivo JSON de credenciais.

5. Configure as variáveis `games_url`, `games_max_games`, `games_output_file`, `input_file` de acordo com suas necessidades.

6. Execute o seguinte comando no terminal para instalar as dependências necessárias:
7. pip install -r requirements.txt

   
8. Execute o seguinte comando no terminal para iniciar a automação:
9. python Game.py


10. Aguarde até que a automação seja concluída. Os dados serão exportados para o arquivo CSV especificado em `games_output_file` e, em seguida, carregados no BigQuery.

11. Verifique a tabela "games_db" no BigQuery para confirmar que os dados foram carregados com sucesso.

12. Links do google sheets e do google BigQuery
13. https://docs.google.com/spreadsheets/d/1WDK9wgrbmSwZQLVEvieBv0ySLtEEwapBvHF0qBVPTv0/edit#gid=1091232862
14. https://console.cloud.google.com/bigquery?sq=844404026566:d6fb37ea074648a1beba7d8575c45585




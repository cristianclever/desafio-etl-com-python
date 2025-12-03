# Desafio ETL com Python

## Stack Tecnológico
* Python 3.13
* Mongodb
* Docker
## Pré requisitos



### Docker
Instalar Docker. O docker sera utilizado para executar uma instancia do mongodb ja com alguns dados
 
## Iniciando a aplicação


1) Iniciando o mongodb
Na pasta deploy execute o comando:
docker compose up

2) executar o script main.py

* script obtem via chamadas rest uma base complementar de ativos listados na B3 (www.genialinvestimentos.com.br). 
  Os dados serão persistidos em uma nova collection "listagem-de-acoes" 
* A aplicação utiliza duas collections do mongodb "listagem-de-acoes"  e "b3-tickers" via aggregation.
* O script obtem dados de noticias disponibilizados em um csv "/resources/Historico_de_materias.csv"
* O Script analisa o conteudo de cada noticia avaliando se existem ativos relacionados a esta noticia.
  Caso existam, a noticia é enriquecida com "tags" correspondentes aos ativos encontrados
* A noticias que forem classificadas, são inseridas na base do mongo, na collection "news"  
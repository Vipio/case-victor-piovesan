# case-victor-piovesan
A proposta do desafio pode ser lida no arquivo "Case Engenharia de Dados - Grupo Boticário"

Para solucionar esse desafio foi pensado as seguintes ferramentas:

  - Bigquery
  - Cloud Storage
  - Cloud Functions
  - Cloud Pub/Sub
  - Cloud Composer
  - GitLab

E Linguagens:

  - Python
  - SQL

A seguir temos um Diagrama do fluxo pensando, e a explicação de cada passo.

![image](https://github.com/Vipio/case-victor-piovesan/assets/24757502/b063c7a3-d9f5-499d-988c-8c72f894a4e1)


  ### Base de Vendas
  O arquivos ".xlsx" que é inserido manualmente pelo "cliente" ou via SFTP ao nosso Data Lake no Cloud Storage, Nossa Dag no Cloud Composer capta esse inserção e dispara uma mensagem via Pub/Sub para a função no Cloud Functions que lê o arquivo insere os dados em um Dataframe e do Dataframe para o Bigquery feito o processamento o arquivo é movido da pasta "entrada" para a pasta "backup" em nosso bucket, o mantendo para possíveis necessidades futuras de reprocessamento/análise, continuamos no fluxo da Dag que irá agora chamar a Procedure no Bigquery que realizará a limpeza de dados duplicados da tabela "bases_vendas", e após chamará outras quatro procedures referentes as tabelas derivadas solicitadas no case para fazer a criação ou merge delas, assim finalizando esta parte do case.

  ### API Spotify
  Foram criadas três Cloud Functions , uma para caso do desafio, que realizam o request na API do Spotify, O Cloud Composer executa uma Dag schedulada e envia uma mensagem para o topico do Pub/Sub onde as três functions estão inscritas, iniciando a execução das mesmas, as functions realizam a coleta dos dados no endpoint da API aplicam filtro e paginação caso necessário, escrevem em um Datafram e inserem em suas respectivas tabelas no BigQuery.

  ### Arquivos
  Os arquivos dos SQL's executados encontram-se em [SQL](https://github.com/Vipio/case-victor-piovesan/tree/main/SQL). <br>
  Os códigos da DAG'S encontra-se em [DAG'S](https://github.com/Vipio/case-victor-piovesan/tree/main/Dags). <br>
  Os códigos Python executados na Cloud Function encontram-se em [Python](https://github.com/Vipio/case-victor-piovesan/tree/main/Python) e cada "main" dentro da pasta com o nome da function.

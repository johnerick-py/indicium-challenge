# Challenge-code
## Desafio pipeline proposto pela empresa indicium.
Você poderá ver mais detalhes do desafio em -> https://github.com/techindicium/code-challenge

Desafio e aceito e cumprido por -> John Erick https://github.com/johnerick-py

## Como executar esta pipeline:
Você precisará das seguintes dependendicas e softwares:
 - Postgres >= 12
 - Python >= 3.7
 
 
  Primeiro passo dê um git clone neste projeto dentro da pasta documents.
   -> https://github.com/johnerick-py/indicium-challenge
 
  Começando pela criação do seu banco de dados:
   
   Neste repositório na pasta dump esta disponibilizado um arquivo .sql que contem os scripts para criação do banco, vá em seu SGDB de preferência, crie    um banco postgreSQL chamado northwind e outro chamado new_northwind (para a segunda etapa) garanta que em ambos o seu usuário seja 'postgres' e a senha '123' ou altere essas informações de conexão nos arquivos connect.py e no arquivo up_to_db.py na linha 13 para as suas credencias de costume.
   
   connect.py
<div align="center">
  <img height="300em" src="https://user-images.githubusercontent.com/63692868/205198193-692f26fb-88f8-4ba2-8d18-a0e569705833.png">
</div>
up_to_db.py
<div align="center">
  <img height="300em" src="https://user-images.githubusercontent.com/63692868/205198257-c1b33878-8ee0-4b07-b7b1-32d1f0ed2b1e.png">
</div>
    
   Apos a criação dos bancos vá ao banco northwind clique com o botão direito sobre ele e abra o Query tool do seu SGDB (no meu caso, estou utilizando o pgadmin4) abra o arquivo northwind.sql em seu editor de texto de preferência e copie todo o conteudo do arquivo, em seguida cole tudo o que copiou dentro da aba aberta pelo  Query tool (DENTRO DO SGDB), selecione tudo e rode o script, após o sinal de sucesso veja se todas as tabelas foram criadas e populadas.
    
 exemplo dentro do SGDB:
   
<div align="center">
 <img height="300em" src="https://user-images.githubusercontent.com/63692868/205199873-303e9f63-5ffc-4261-93ad-83ca2a89a3c0.png">
</div>   
   

 ## Configurando o ambiente python:
  Para rodarmos o pipeline primeiramente precisamos criar um ambiente virtual em python:
   Execute no terminal dentro da pasta do pipeline o seguinte comando - python3 -m venv tutorial-env
   para entrar em seu ambiente virtual digite no terminal
    
  Ubuntu ->
  `source venv/bin/activate`
  
  
  Windows ->
  `venv\Scripts\activate.bat`
     
  Após iniciar o seu ambiente virtual execute o seguinte comando no terminal para instalar as bibliotecas que estão no arquivo requirements.txt
  
 `pip install -r requirements.txt`
 
 Pronto, fizemos todas as configurações e instalamos as libs que são necessarias.
 
 ## Utilizando o pipeline
 
 Primeiro verifique se dentro do diretorio "data" existem as pastas "csv" e "postgres", se não existir crie elas(dentro de data).
 
 ### Executando a etapa 1:
A etapa um se divide em duas partes, a primeira em transformar todas as tabelas do banco northwind em arquivos CSV e a segunda parte que e responsável por transformar o order_details.csv em um arquivo JSON.

Ao executar a primeira etapa ele ira salvar todas as tabelas do banco northwind em arquivos CSV dentro da pasta postgres seguindo o seguinte caminho `data/postgres/{pasta-com-nome-tabela}/{pasta-data-executado}/tabela.csv`

Ao executar a segunda etapa ele ira salvar o order_details como json dentro do seguinte caminho
`data/csv/{nome-csv}/{pasta-data-executado}/order_details.json`

#### Executando a conversão de SQL para CSV

No terminal execute o script sql_to_csv.py 

`python3 sql_to_csv.py`

Ao executar veja no terminal que ele ira pedir que você insira as informações de que você deseja como: executar na data de hoje ou executar em uma data que você deseja.

Veja na imagem, selecionei a opção 1 e ele pegou o horario do meu computador e criou os diretorios e os arquivos csv:

<div align="center">
 <img height="300em" src="https://user-images.githubusercontent.com/63692868/205206180-b08d474a-02cc-4a81-995c-f10503ebf941.png">
</div>   

Agora execute a segunda parte da etapa 1, no terminal execute o script csv_to_json.py

`python3 csv_to_json.py`

Veja na imagem, selecionei a opção 1 e ele pegou o horario do meu computador e criou os diretorios e o arquivo json:
<div align="center">
 <img height="300em" src="https://user-images.githubusercontent.com/63692868/205207006-ddad3a0a-9508-4401-a484-d9ccbb18d436.png">
</div>   

Com isso finalizamos a primeira etapa.

### Executando a segunda etapa e convertendo o resultado de uma query que une order e order_details em csv.

No começo criamos dois bancos (northwind) o qual fizemos a restauração e (new_northwind) o qual vamos usar agora para importar os arquivos CSV e JSON como tabelas.

Este script realiza a tarefa de transformar os arquivos em tabelas no postgreSQL.

Entre na pasta data
no terminal -> `cd data`
execute o arquivo up_to_db.py
`python3 up_to_db.py`

Veja na imagem, novamente o script pergunta se quer executar na data atual ou a sua data de preferencia, no meu caso selecionei a data atual,
em seguida ele nos mostra uma mensagem de sucesso para cada tabela criada no banco new_northwind, vá ao seu bando new_northwind e dê um refresh
para visualizar as novas tabelas.

Observe que fiz uma query que busca os dados de order e order_details onde order.order_id = order_details.order_id assim relacionando-as.

<div align="center">
 <img height="300em" src="https://user-images.githubusercontent.com/63692868/205209020-fa8d427c-283c-4321-ac30-582664634500.png">
</div>

Para finalizar a segunda etapa iremos executar o arquivo query_to_csv.py onde iremos realizar a seguinte query:

`select * from orders as ord, order_details as dt where ord.order_id = dt.order_id`

e converter o seu resultado em um arquivo csv.

execute no terminal:
`python3 query_to_csv.py`
 
 Observe a imagem, e criado um csv do resultado da query dentro da pasta data.
 
<div align="center">
 <img height="300em" src="https://user-images.githubusercontent.com/63692868/205209548-30d3d18f-ee6a-4ffa-9c68-1d82f8f8f243.png">
</div>
 
 com isso finalizamos nosso pipeline.
 
 #Observações e detalhes finais
 
 Em todo o projeto utilizei dos recursos da biblioteca pandas,sys,os e datetime, na primeira etapa os dois scripts fazem como foi pedido eles efetuam a conversão dos arquivos para outros formatos e salve-os na filesystem, veja que o caminho final de onde e salvo os scripts cumpre com o pedido do desafio assim como as nomenclaturas.

Sobre a segunda etapa, repare que dentro da pasta data existem um txt chamado logfile eu o utilizei para salvar a data em que a etapa um dê erro, assim posso captar a data do erro e aplicar a penalidade de 24 horas., pois a etapa dois não pode e não será executada sem a etapa um, então em caso de erro sera registrado no logfile e sera exibido o aviso e terminara o script, em caso de a etapa um não ter sido executada o arquivo ira cair na tratativa de erro ira exibir o traceback e será fechado.

Ainda sobre a segunda etapa, se você selecionar a opção "other date" e já tiver executado antes a etapa dois e criado as tabelas no banco new_northwind ele ira pedir para você apagar as tabelas antigas para poder criar as novas tabelas, pois a própria biblioteca sqlalchemy nos devolve o erro que e gerado no postegreSQL ao criar uma tabela com o nome já existente, lembrando que ira apagar e criar novamente com a data que você deseja somente no banco, as pastas da primeira etapa permanecerão, pois não sofrem influência do script.


Finalizando:

Por favor verifique se a pasta csv dentro de data foi importada quando clonar o repositório, o github não está dando aceitando o push da própria por estar vazia.

O código foi executado em dois ambientes diferentes, Ubuntu e Linux Mint.

Agradeço pela oportunidade de participar desta etapa, fico a disposição.
Att John Erick.
 
 
 

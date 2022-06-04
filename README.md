# Datalake utilizando Spark na GCP

#### Objetivo do projeto

<img src="imagens\Desenho.jpg">

<p>
O seguinte projeto consome dados de um bucket público na Google Coud Storage. Depois, utilizamos processamento distribuído para tratar os dados e entregar as camadas Trusted e Refined do nosso datalake. 
 </p>

#### Tecnologias utilizadas
- ``Spark``
- ``Google Cloud Storage``
- ``Google Dataproc``
- ``Python``
### Como rodar e testar
<p> Os dados utilizados como camada Raw do nosso datalake são um bucket já aberto com dados de estabelecimentos. 

Para rodar meu código, basta baixar os arquivos, criar um cluster na GCP com o código do arquivo create-cluster.

O bucket datalake, que armazena as camadas trusted e refined, foi criado manualmente.

Depois de criá-lo, basta alterar a nomenclatura do bucket no código. 

O arquivo functions.py contem as funções que utilizei ao longo do desenvolvimento. 

O arquivo spark_app_trusted.py leva os dados até a camada trusted. Já o spark_app_refined é a execução que leva até a camada refined. Eles devem ser rodados na linha de comando da nuvem da Google exatamente na ordem que citei.

O comando para rodar um job é o:

gcloud dataproc jobs submit pyspark
--region <Sua regiao>
--cluster <Nome do cluster>
--py-files <Arquivo auxiliar. No nosso caso, o functions.py>
<seu arquivo que deseja rodar, o spark_app_refined.py ou o spark_app_trusted.py>

Para o comando funcionar, é importante os códigos estarem salvos em um bucket da Google, sugiro salvar em uma pasta chamada script. A partir daí, você copia o path dela e coloca no comando acima.

</p>
import pyspark.sql.functions as f
"""
Arquivo com as funções utilizadas nos arquivos principais. 
Aqui, temos as funções que leem os dados da base, fazem as transformações e salvam no bucket de destino

@Igor Augusto
03/06/2022 

"""

def read_data(spark):
    path = ['gs://desafio-final/F.K03200$Z.D10710.CNAE.csv',
            'gs://desafio-final/F.K03200$Z.D10710.MUNIC.csv',
            'gs://desafio-final/estabelecimentos'
            ]
    schemas = ['cod_cnae STRING, descricao_cnae STRING',
               'cod_municipio INT, nm_cidade STRING',
               """
                cod_cnpj_basico STRING, 
                cod_cnpj_ordem STRING, 
                cod_cnpj_dv STRING, 
                cod_matriz_filial INT, 
                nm_fantasia STRING,
                cod_situacao INT,
                dt_situacao_cadastral STRING,
                cod_motivo_situacao INT,
                nm_cidade_exterior STRING,
                cod_pais STRING,
                dt_atividade STRING,
                cod_CNAE_principal STRING,
                cod_CNAE_secundario STRING,
                ds_tipo_logradouro STRING,
                ds_logradouro STRING,
                num_endereco INT,
                ds_complemento STRING,
                nm_bairro STRING,
                cod_cep STRING,
                sg_uf STRING,
                cod_municipio STRING,
                num_ddd STRING,
                num_telefone STRING,
                num_ddd2 STRING,
                num_telefone2 STRING,
                num_ddd_fax STRING,
                num_fax STRING,
                nm_email STRING,
                ds_situacao_especial STRING,
                dt_situacao_especial STRING
               """
               ]

    df_list = []
    c = 0
    for i in path:
        df = (
            spark
                .read
                .format('csv')
                .option('sep', ';')
                .option('encoding', 'ISO-8859-1')
                .option('escape', "\"")
                .schema(schemas[c])
                .load(path[c])
        )
        df_list.append(df)
        c += 1
    return df_list[0], df_list[1], df_list[2]


def processing(df):
    # limpa string
    df_estabelecimento_processado = df
    str_cols = ['cod_cnpj_basico',
                'cod_cnpj_ordem',
                'cod_cnpj_dv',
                'nm_fantasia',
                'nm_cidade_exterior',
                'cod_pais',
                'cod_CNAE_principal',
                'cod_CNAE_secundario',
                'ds_tipo_logradouro',
                'ds_logradouro',
                'ds_complemento',
                'nm_bairro',
                'cod_cep',
                'sg_uf',
                'cod_municipio',
                'num_ddd',
                'num_telefone',
                'num_ddd2',
                'num_telefone2',
                'num_ddd_fax',
                'num_fax',
                'nm_email',
                'ds_situacao_especial',
                'dt_situacao_especial']
    for c in str_cols:
        df_estabelecimento_processado = (
            df_estabelecimento_processado
                .withColumn(c, f.trim(f.col(c)))
        )
    # converte data e trata numeros
    df_estabelecimento_processado = (
        df_estabelecimento_processado
            .withColumn("dt_situacao_cadastral", f.to_date(f.col("dt_situacao_cadastral"), "yyyyMMdd"))
            .withColumn("dt_atividade", f.to_date(f.col("dt_atividade"), "yyyyMMdd"))
            .withColumn("dt_situacao_especial", f.to_date(f.col("dt_situacao_especial"), "yyyyMMdd"))
            .withColumn("num_endereco", f.coalesce(f.col("num_endereco"), f.lit(0)))
    )

    # cuida das datas anteriores a 1900
    df_estabelecimento_processado = (
        df_estabelecimento_processado
            .withColumn("dt_atividade", f.when(f.col("dt_atividade") <= "1850-01-01", None)
                        .otherwise(f.col("dt_atividade")))
            .withColumn("dt_situacao_cadastral", f.when(f.col("dt_situacao_cadastral") <= "1850-01-01", None)
                        .otherwise(f.col("dt_situacao_cadastral")))

    )
    return df_estabelecimento_processado


def write_trusted(df, path):
    for c in range(3):
        try:
            (
                df[c]
                .write
                .format('parquet')
                .mode('overwrite')
                .save(path[c])
            )
            print("Operacao concluida com sucesso no path:  " + path[c])
        except Exception as e:
            print(e)


def read_data_trusted(spark):
    path = ["gs://datalake-demo-spark/trusted/estabelecimento",
               "gs://datalake-demo-spark/trusted/cnae",
               "gs://datalake-demo-spark/trusted/municipio"]
    df_list = []
    c = 0
    for i in path:
        df = (
            spark
            .read
            .format('parquet')
            .load(path[c])
        )
        df_list.append(df)
        c+=1
    return df_list[0], df_list[1], df_list[2]


def join_data(df_estabelecimento, df_cnae, df_municipio):
    df_final = (
        df_estabelecimento
        .join(df_cnae.hint('broadcast'), f.col("cod_CNAE_principal") == f.col("cod_cnae"), 'left')
        .join(df_municipio.hint('broadcast'), "cod_municipio", 'left')
    )
    return df_final


def write_refined(df):
    try:
        (
            df
                .write
                .format('parquet')
                .partitionBy('sg_uf')
                .mode('overwrite')
                .save("gs://datalake-demo-spark/refined")
        )
        print("Operacao concluida com sucesso")
    except:
        print("Erro. Operacao nao concluida")

def run_trusted(spark):
    df_cnae, df_municipio, df_estabelecimento = read_data(spark)
    df_estabelecimento = processing(df_estabelecimento)
    write_trusted([df_estabelecimento, df_cnae, df_municipio],
                  ["gs://datalake-demo-spark/trusted/estabelecimento",
                   "gs://datalake-demo-spark/trusted/cnae",
                   "gs://datalake-demo-spark/trusted/municipio"])

def run_refined(spark):
    df_estabelecimento, df_cnae, df_municipio = read_data_trusted(spark)
    df_final = join_data(df_estabelecimento, df_cnae, df_municipio)
    write_refined(df_final)
from pyspark.sql import functions as f
from pyspark.sql import SparkSession

rais = (
    spark
    .read
    .text("s3://datalake-jeff-igti-edc-tf/raw-data/rais/year=2020/", inferSchema=True, header=True, sep=';', encoding="latin1")
)         

#renomeando colunas com caracteres especiais
rais.printSchema()

rais = (
    rais
    .withColumnRenamed(rais.columns[7], 'cbo_ocupacao_2002')
    .withColumnRenamed(rais.columns[11], 'vinculo_ativo_31_12')
    .withColumnRenamed(rais.columns[12], 'faixa_etaria')
    .withColumnRenamed(rais.columns[15], 'faixa_remun_media_sm')
    .withColumnRenamed(rais.columns[17], 'escolaridade_apos_2005')
    .withColumnRenamed(rais.columns[22], 'mes_admissao')
    .withColumnRenamed(rais.columns[23], 'mes_desligamento')
    .withColumnRenamed(rais.columns[25], 'municipio')
    .withColumnRenamed(rais.columns[27], 'natureza_juridica')
    .withColumnRenamed(rais.columns[30], 'raca_cor')
    .withColumnRenamed(rais.columns[31], 'regioes_adm_df')
    .withColumnRenamed(rais.columns[34], 'vl_remun_media_nom')
    .withColumnRenamed(rais.columns[35], 'vl_remun_media_sm')
    .withColumnRenamed(rais.columns[40], 'tipo_admissao')
    .withColumnRenamed(rais.columns[44], 'tipo_vinculo')
    .withColumnRenamed(rais.columns[48], 'vl_rem_marco_sc')
)


#substituição de espaço por underline no nome das colunas
rais = rais.toDF(*(c.replace(' ', '_') for c in rais.columns))

#colocando os nomes das colunas em minúsculo
for c in rais.columns:
    rais = rais.withColumnRenamed(c, lower(c))

#conversão do tipo das colunas
ListaColunasToInt=["mes_desligamento"]
ListaColunasToDouble=["vl_remun_dezembro_nom","vl_remun_dezembro_sm","vl_remun_media_nom",
"vl_remun_media_sm","vl_rem_janeiro_sc","vl_rem_fevereiro_sc","vl_rem_marco_sc",
"vl_rem_abril_sc","vl_rem_maio_sc","vl_rem_junho_sc","vl_rem_julho_sc", "vl_rem_agosto_sc",
 "vl_rem_setembro_sc", "vl_rem_outubro_sc","vl_rem_novembro_sc"]

for i in ListaColunasToInt:
    rais = rais.withColumn( i,  f.regexp_replace( i, ',', '.').cast('int'))
for i in ListaColunasToDouble:
    rais = rais.withColumn( i,  f.regexp_replace( i, ',', '.').cast('double'))





#criação da coluna uf
rais = rais.withColumn("uf",f.col("municipio").cast('string').substr(1,2).cast('int'))





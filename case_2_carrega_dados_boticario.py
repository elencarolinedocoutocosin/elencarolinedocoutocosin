# Databricks notebook source
jdbcHostname = "database-2.cjhvvs9p0qo8.sa-east-1.rds.amazonaws.com"
jdbcDatabase = "boticario"
jdbcPort = 3306
#jdbcUrl = "jdbc:mysql://{0}:{1}/{2}?user={3}&password={4}".format(jdbcHostname, jdbcPort, jdbcDatabase, 'admin', 'adminboticario')

# COMMAND ----------

jdbcUrl = "jdbc:mysql://{0}:{1}/{2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
connectionProperties = {
  "user" : 'admin',
  "password" : 'adminboticario',
  "driver" : "com.mysql.jdbc.Driver"
}

# COMMAND ----------

sparkDF_Base_2019_3 = spark.read.csv("/FileStore/tables/Base_2019_3.csv", header="true", inferSchema="true", sep=";")
sparkDF_Base_2018_2 = spark.read.csv("/FileStore/tables/Base_2018_2.csv", header="true", inferSchema="true", sep=";")
sparkDF_Base_2017_1 = spark.read.csv("/FileStore/tables/Base_2017_1.csv", header="true", inferSchema="true", sep=";")


# COMMAND ----------

sparkDF_Base_2019_3.createOrReplaceTempView('tb_temp_Base_2019_3')
sparkDF_Base_2018_2.createOrReplaceTempView('tb_temp_Base_2018_2')
sparkDF_Base_2017_1.createOrReplaceTempView('tb_temp_Base_2017_1')

# COMMAND ----------

from pyspark.sql import *
df_union = spark.sql("""
select * from tb_temp_Base_2019_3 
union all 
select * from tb_temp_Base_2018_2 
union all 
select * from tb_temp_Base_2017_1""")

df_union.createOrReplaceTempView('temp_df_union')

# COMMAND ----------

df_union.write.jdbc(url=jdbcUrl, table="VENDAS_BOTICARIO", mode='append', properties=connectionProperties)

# COMMAND ----------

df_tab_1 = spark.sql("""
select substr(data_venda,4,2) || '-' || substr(data_venda,7,4) as mes_ano, 
sum(qtd_venda) as qtd_venda
from temp_df_union
group by substr(data_venda,4,2) || '-' || substr(data_venda,7,4)
order by 1
""")

# COMMAND ----------

df_tab_1.write.jdbc(url=jdbcUrl, table="TABELA_1", mode='append', properties=connectionProperties)

# COMMAND ----------

df_tab_2 = spark.sql("""
select 
MARCA,
LINHA,
sum(qtd_venda) as qtd_venda
from temp_df_union
group by MARCA,LINHA
order by 1,2
""")

# COMMAND ----------

df_tab_2.write.jdbc(url=jdbcUrl, table="TABELA_2", mode='append', properties=connectionProperties)

# COMMAND ----------

df_tab_3 = spark.sql("""
select MARCA,substr(data_venda,4,2) || '-' || substr(data_venda,7,4) as mes_ano, 
sum(qtd_venda) as qtd_venda
from temp_df_union
group by MARCA,substr(data_venda,4,2) || '-' || substr(data_venda,7,4)
order by 1,2
""")

# COMMAND ----------

df_tab_3.write.jdbc(url=jdbcUrl, table="TABELA_3", mode='append', properties=connectionProperties)

# COMMAND ----------

df_tab_4 = spark.sql("""
select LINHA,substr(data_venda,4,2) || '-' || substr(data_venda,7,4) as mes_ano, 
sum(qtd_venda) as qtd_venda
from temp_df_union
group by LINHA,substr(data_venda,4,2) || '-' || substr(data_venda,7,4)
order by 1,2
""")

# COMMAND ----------

df_tab_4.write.jdbc(url=jdbcUrl, table="TABELA_4", mode='append', properties=connectionProperties)

# COMMAND ----------

pushdown_query_tab4 = "(select LINHA from TABELA_4 WHERE MES_ANO='12-2018' AND QTD_VENDA = (SELECT MAX(QTD_VENDA) FROM TABELA_4 WHERE MES_ANO='12-2018')) tb4"
df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query_tab4, properties=connectionProperties)
display(df)

# COMMAND ----------

import tweepy

consumer_key = 'jMLXnjPgNyGjuWwxyVdPUVAQc'
consumer_secret = '7DZ5nUDWV4ko9MS9vXTz9fBS0D1LxYXfhFlRer28a9E3bFCtpy'
access_token = '1394719258249154561-2K1Y8PFlfwgLyocBtp4uBETTw7l78w'
access_token_secret = 'vq6BIdVnxm3D3MCJrZjaDzej6yVYwq6sNbF4EEFDwn9bs'

autenticar = tweepy.OAuthHandler(consumer_key, consumer_secret)
autenticar.set_access_token(access_token, access_token_secret)
api = tweepy.API(autenticar)


# COMMAND ----------

palavra_1='Boticário'
palavra_2=df.collect()[0].asDict()['LINHA']
#palavra_2='hidratante'

procura= palavra_1 + ' ' + palavra_2

contador = 50

tweets_e = api.search(q=procura, tweet_mode = 'extended', lang='pt', count=contador*2)
print(procura)

# COMMAND ----------

import pandas as pd
import unidecode

output = []
ct = 0
for item in tweets_e:
  if unidecode.unidecode(palavra_1.lower()) in unidecode.unidecode(item.full_text.lower()) and unidecode.unidecode(palavra_2.lower()) in unidecode.unidecode(item.full_text.lower()):
    #print(item.full_text)
    line = {'data_criacao' : item.created_at, 'nome' : item.user.name, 'texto' : item.full_text.replace('\n', ' ').replace('\r', '')}
    output.append(line) 
    
    ct=ct+1
    if ct==contador:
      break    

df_output = pd.DataFrame(output)
display(df_output)

# COMMAND ----------

df_output.info()

# COMMAND ----------

df_insert_tab_twitter = spark.createDataFrame(df_output)

df_insert_tab_twitter.write.jdbc(url=jdbcUrl, table="TABELA_TWITTER", mode='append', properties=connectionProperties)


# Databricks notebook source
jdbcHostname = "database-2.cjhvvs9p0qo8.sa-east-1.rds.amazonaws.com"
jdbcDatabase = "boticario"
jdbcPort = 3306
#jdbcUrl = "jdbc:mysql://{0}:{1}/{2}?user={3}&password={4}".format(jdbcHostname, jdbcPort, jdbcDatabase, 'admin', 'adminboticario')

# COMMAND ----------

jdbcUrl = "jdbc:mysql://{0}:{1}/{2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
connectionProperties = {
  "user" : 'admin',
  "password" : '',
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

# COMMAND ----------

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
sum(qtd_venda) 
from temp_df_union
group by substr(data_venda,4,2) || '-' || substr(data_venda,7,4)
order by 1
""")

# COMMAND ----------

display(df_tab_1)

# COMMAND ----------

df_tab_1.write.jdbc(url=jdbcUrl, table="TABELA_1", mode='append', properties=connectionProperties)

# COMMAND ----------

df_tab_2 = spark.sql("""
select 
MARCA,
LINHA,
sum(qtd_venda) 
from temp_df_union
group by MARCA,LINHA
order by 1,2
""")

# COMMAND ----------

display(df_tab_2)

# COMMAND ----------

df_tab_2.write.jdbc(url=jdbcUrl, table="TABELA_2", mode='append', properties=connectionProperties)

# COMMAND ----------

df_tab_3 = spark.sql("""
select MARCA,substr(data_venda,4,2) || '-' || substr(data_venda,7,4) as mes_ano, 
sum(qtd_venda) 
from temp_df_union
group by MARCA,substr(data_venda,4,2) || '-' || substr(data_venda,7,4)
order by 1,2
""")

# COMMAND ----------

display(df_tab_3)

# COMMAND ----------

df_tab_3.write.jdbc(url=jdbcUrl, table="TABELA_3", mode='append', properties=connectionProperties)

# COMMAND ----------

df_tab_4 = spark.sql("""
select LINHA,substr(data_venda,4,2) || '-' || substr(data_venda,7,4) as mes_ano, 
sum(qtd_venda) 
from temp_df_union
group by LINHA,substr(data_venda,4,2) || '-' || substr(data_venda,7,4)
order by 1,2
""")

# COMMAND ----------

display(df_tab_4)

# COMMAND ----------

df_tab_4.write.jdbc(url=jdbcUrl, table="TABELA_4", mode='append', properties=connectionProperties)

# COMMAND ----------

pushdown_query_tab2 = "(select * from TABELA_2) tb2"
df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query_tab2, properties=connectionProperties)
display(df)

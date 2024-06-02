// Databricks notebook source
// MAGIC %python
// MAGIC dbutils.fs.ls("mnt/dados/inbound")

// COMMAND ----------

val path = "dbfs:/mnt/dados/inbound/dados_brutos_imoveis.json"
val dados = spark.read.json(path)

// COMMAND ----------

display(dados)

// COMMAND ----------

// MAGIC %md
// MAGIC Removendo colunas
// MAGIC
// MAGIC

// COMMAND ----------

val dados_anuncio = dados.drop("imagens","usuario")
display(dados_anuncio)


// COMMAND ----------

// DBTITLE 1,Criando uma coluna de identificação
import org.apache.spark.sql.functions.col
val df_bronze = dados_anuncio.withColumn("id",col("anuncio.id"))
display(df_bronze)

// COMMAND ----------

// DBTITLE 1,Salvando na camada bronze
var path = "dbfs:/mnt/dados/bronze/dataset_imoveis"
df_bronze.write.format("delta").mode(SaveMode.Overwrite).save(path)

// COMMAND ----------

// DBTITLE 1,Conferindo se os dados foram montados e se temos acesso a pasta bronze
// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt/dados/bronze")

// COMMAND ----------

// DBTITLE 1,Lendo os dados na camada bronze
val path = "dbfs:/mnt/dados/bronze/dataset_imoveis/"
val df = spark.read.format("delta").load(path)
display(df)

// COMMAND ----------

// DBTITLE 1,Transformando campos do json em colunas
display(df.select("anuncio.*"))

// COMMAND ----------

display(
  df.select("anuncio.*","anuncio.endereco.*")
  )

// COMMAND ----------

val dados_detalhados = df.select("anuncio.*","anuncio.endereco.*")
display(dados_detalhados)

// COMMAND ----------

// DBTITLE 1,removendo coluna
val df_silver = dados_detalhados.drop("caracteristicas","endereco")
display(df_silver)

// COMMAND ----------

// DBTITLE 1,Salvando na camada Silver
val path = "dbfs:/mnt/dados/silver/dataset_imoveis/"
df_silver.write.format("delta").mode("overwrite").save(path)

// COMMAND ----------

val columnNames = df_silver.columns
columnNames


// COMMAND ----------

val columnNames: Array[String] = df_silver.columns
columnNames


// COMMAND ----------

columnNames.foreach(println)


// COMMAND ----------



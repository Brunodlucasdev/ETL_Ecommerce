# Databricks notebook source
# Definindo o caminho da subpasta olist na Camada Bronze
STORAGE_ACCOUNT = "datalakebrunodev"
path_bronze = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/olist/"

print("Carregando as tabelas brutas do Olist a partir do Azure Data Lake...")

# Leitura dos CSVs da Bronze para DataFrames PySpark
df_pedidos = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{path_bronze}olist_orders_dataset.csv")
df_itens = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{path_bronze}olist_order_items_dataset.csv")
df_clientes = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{path_bronze}olist_customers_dataset.csv")
df_produtos = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{path_bronze}olist_products_dataset.csv")
df_vendedores = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{path_bronze}olist_sellers_dataset.csv")
df_pagamentos = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{path_bronze}olist_order_payments_dataset.csv")

print("Tabelas carregadas com sucesso na memória!")

# Exibe uma prévia da tabela principal de Pedidos
display(df_pedidos)

# COMMAND ----------


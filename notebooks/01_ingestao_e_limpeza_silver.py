# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Ingestão e limpeza (Bronze -> Silver)

# Configurações gerais

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp, coalesce, lit, round as spark_round, trim, initcap, upper, lower, when

STORAGE_ACCOUNT = "datalakebrunodev"
PATH_BRONZE = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/olist/"
PATH_SILVER = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/olist/"

CATALOG = "dtb_projeto_etl_7405616024732022"              # confira no painel Catalog
SCHEMA_SILVER = "olist_silver"

# Horários do Olist são de Brasília
spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_SILVER}")
print("Configurações carregadas.")



# Leitura da Bronze

def ler_csv(nome_arquivo: str) -> DataFrame:
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("encoding", "UTF-8")
        .csv(f"{PATH_BRONZE}{nome_arquivo}")
    )

df_pedidos_raw    = ler_csv("olist_orders_dataset.csv")
df_itens_raw      = ler_csv("olist_order_items_dataset.csv")
df_clientes_raw   = ler_csv("olist_customers_dataset.csv")
df_produtos_raw   = ler_csv("olist_products_dataset.csv")
df_vendedores_raw = ler_csv("olist_sellers_dataset.csv")
df_pagamentos_raw = ler_csv("olist_order_payments_dataset.csv")
df_traducao_raw   = ler_csv("product_category_name_translation.csv")

print("Arquivos da Bronze lidos.")


# Funções de limpeza reutilizáveis

def tratar_datas(df: DataFrame, colunas: list) -> DataFrame:
    for c in colunas:
        df = df.withColumn(c, to_timestamp(col(c)))
    return df

def tratar_texto(df: DataFrame, colunas: list, modo: str = "initcap") -> DataFrame:
    """Remove espaços extras e padroniza a caixa. modo: 'initcap' ou 'upper'."""
    for c in colunas:
        base = trim(col(c))
        df = df.withColumn(c, initcap(base) if modo == "initcap" else upper(base))
    return df

def remover_nulos_chave(df: DataFrame, colunas_chave: list) -> DataFrame:
    return df.dropna(subset=colunas_chave)

def remover_duplicados(df: DataFrame, colunas_chave: list) -> DataFrame:
    return df.dropDuplicates(colunas_chave)



# Limpeza por tabela

# 1. Pedidos
df_pedidos_silver = (
    df_pedidos_raw
    .transform(lambda d: tratar_datas(d, [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]))
    .transform(lambda d: remover_nulos_chave(d, ["order_id", "customer_id"]))
    .transform(lambda d: remover_duplicados(d, ["order_id"]))
    .withColumn("order_status", trim(lower(col("order_status"))))
)

# 2. Itens do pedido
df_itens_silver = (
    df_itens_raw
    .transform(lambda d: tratar_datas(d, ["shipping_limit_date"]))
    .transform(lambda d: remover_nulos_chave(d, ["order_id", "product_id", "seller_id"]))
    .withColumn("price", spark_round(col("price").cast("double"), 2))
    .withColumn("freight_value", spark_round(col("freight_value").cast("double"), 2))
)

# 3. Clientes
df_clientes_silver = (
    df_clientes_raw
    .transform(lambda d: remover_nulos_chave(d, ["customer_id"]))
    .transform(lambda d: remover_duplicados(d, ["customer_id"]))
    .transform(lambda d: tratar_texto(d, ["customer_city"], "initcap"))
    .transform(lambda d: tratar_texto(d, ["customer_state"], "upper"))
)

# 4. Vendedores
df_vendedores_silver = (
    df_vendedores_raw
    .transform(lambda d: remover_nulos_chave(d, ["seller_id"]))
    .transform(lambda d: remover_duplicados(d, ["seller_id"]))
    .transform(lambda d: tratar_texto(d, ["seller_city"], "initcap"))
    .transform(lambda d: tratar_texto(d, ["seller_state"], "upper"))
)

# 5. Produtos (categoria traduzida para inglês e nulos tratados)
df_traducao = (
    df_traducao_raw
    .select(
        trim(col("product_category_name")).alias("product_category_name"),
        trim(col("product_category_name_english")).alias("product_category_name_english"),
    )
)

df_produtos_silver = (
    df_produtos_raw
    .transform(lambda d: remover_nulos_chave(d, ["product_id"]))
    .transform(lambda d: remover_duplicados(d, ["product_id"]))
    .withColumn("product_category_name", trim(lower(col("product_category_name"))))
    .join(df_traducao, "product_category_name", "left")
    .withColumn(
        "product_category_name_english",
        coalesce(col("product_category_name_english"), col("product_category_name"), lit("not_informed"))
    )
    .withColumn(
        "product_category_name",
        coalesce(col("product_category_name"), lit("nao_informado"))
    )
)

# 6. Pagamentos
df_pagamentos_silver = (
    df_pagamentos_raw
    .transform(lambda d: remover_nulos_chave(d, ["order_id"]))
    .withColumn("payment_value", spark_round(col("payment_value").cast("double"), 2))
)

print("Limpeza concluída para todas as tabelas.")


# Gravação na Silver: arquivo Delta + tabela registrada no catálogo

def gravar_silver(df: DataFrame, nome: str):
    caminho = f"{PATH_SILVER}{nome}"
    nome_tabela = f"{CATALOG}.{SCHEMA_SILVER}.{nome}"

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("path", caminho)
        .saveAsTable(nome_tabela)
    )
    print(f"✓ {nome_tabela} | {df.count()} linhas | {caminho}")

tabelas_silver = {
    "pedidos":      df_pedidos_silver,
    "itens_pedido": df_itens_silver,
    "clientes":     df_clientes_silver,
    "produtos":     df_produtos_silver,
    "vendedores":   df_vendedores_silver,
    "pagamentos":   df_pagamentos_silver,
}

for nome, df in tabelas_silver.items():
    gravar_silver(df, nome)

print("\nCamada Silver gravada e registrada no catálogo.")


# Validação rápida

display(spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA_SILVER}"))
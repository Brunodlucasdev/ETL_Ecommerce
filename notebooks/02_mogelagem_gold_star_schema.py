# Databricks notebook source
# Databricks notebook source
from pyspark.sql import Window
from pyspark.sql.functions import (
    col, row_number, to_date, year, month, dayofmonth, quarter,
    dayofweek, date_format, create_map, lit
)
from itertools import chain

STORAGE_ACCOUNT = "datalakebrunodev"
path_silver = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/olist/"
path_gold = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/olist/"

df_pedidos = spark.read.format("delta").load(f"{path_silver}pedidos")
df_itens = spark.read.format("delta").load(f"{path_silver}itens_pedido")
df_clientes = spark.read.format("delta").load(f"{path_silver}clientes")
df_produtos = spark.read.format("delta").load(f"{path_silver}produtos")
df_vendedores = spark.read.format("delta").load(f"{path_silver}vendedores")

print("Tabelas da camada Silver carregadas!")


def com_chave_substituta(df, coluna_natural, nome_sk):
    """Gera uma chave inteira sequencial, determinística, a partir do ID natural."""
    janela = Window.orderBy(coluna_natural)
    return df.withColumn(nome_sk, row_number().over(janela))

# Dimensão Clientes (uma linha por customer_id, que é o registro usado nos pedidos)
dim_cliente = com_chave_substituta(
    df_clientes
        .select("customer_id", "customer_unique_id", "customer_city", "customer_state")
        .dropDuplicates(["customer_id"]),
    "customer_id", "sk_cliente"
).select(
    col("sk_cliente"),
    col("customer_id").alias("id_cliente"),
    col("customer_unique_id").alias("id_cliente_unico"),
    col("customer_city").alias("cidade"),
    col("customer_state").alias("estado")
)

# Dimensão Produtos
dim_produto = com_chave_substituta(
    df_produtos
        .select("product_id", "product_category_name", "product_weight_g",
                "product_length_cm", "product_height_cm", "product_width_cm")
        .dropDuplicates(["product_id"]),
    "product_id", "sk_produto"
).select(
    col("sk_produto"),
    col("product_id").alias("id_produto"),
    col("product_category_name").alias("categoria"),
    col("product_weight_g").alias("peso_g"),
    col("product_length_cm").alias("comprimento_cm"),
    col("product_height_cm").alias("altura_cm"),
    col("product_width_cm").alias("largura_cm")
)

# Dimensão Vendedores
dim_vendedor = com_chave_substituta(
    df_vendedores
        .select("seller_id", "seller_city", "seller_state")
        .dropDuplicates(["seller_id"]),
    "seller_id", "sk_vendedor"
).select(
    col("sk_vendedor"),
    col("seller_id").alias("id_vendedor"),
    col("seller_city").alias("cidade"),
    col("seller_state").alias("estado")
)

# Dimensão Tempo: uma linha por DIA, com chave inteira no formato yyyymmdd
nomes_dias = {1: "domingo", 2: "segunda", 3: "terça", 4: "quarta",
              5: "quinta", 6: "sexta", 7: "sábado"}
mapa_dias = create_map(*[lit(x) for x in chain.from_iterable(nomes_dias.items())])

datas = (
    df_pedidos
        .select(to_date("order_purchase_timestamp").alias("data"))
        .where(col("data").isNotNull())
        .distinct()
)

dim_tempo = datas.select(
    date_format("data", "yyyyMMdd").cast("int").alias("sk_tempo"),
    col("data"),
    year("data").alias("ano"),
    month("data").alias("mes"),
    dayofmonth("data").alias("dia"),
    quarter("data").alias("trimestre"),
    mapa_dias[dayofweek("data")].alias("dia_semana")
)

print("Dimensões construídas!")

# Fato de vendas: granularidade = 1 linha por item de pedido
fato_vendas = (
    df_itens
        .join(
            df_pedidos.select("order_id", "customer_id", "order_status",
                              "order_purchase_timestamp"),
            "order_id", "inner")
        .join(dim_cliente.select(col("id_cliente").alias("customer_id"), "sk_cliente"),
              "customer_id", "inner")
        .join(dim_produto.select(col("id_produto").alias("product_id"), "sk_produto"),
              "product_id", "inner")
        .join(dim_vendedor.select(col("id_vendedor").alias("seller_id"), "sk_vendedor"),
              "seller_id", "inner")
        .withColumn("sk_tempo",
                    date_format(to_date("order_purchase_timestamp"), "yyyyMMdd").cast("int"))
        .select(
            col("order_id").alias("id_pedido"),
            col("order_item_id").alias("id_item"),
            "sk_cliente", "sk_produto", "sk_vendedor", "sk_tempo",
            col("order_status").alias("status_pedido"),
            col("price").alias("valor_produto"),
            col("freight_value").alias("valor_frete"),
            (col("price") + col("freight_value")).alias("valor_total")
        )
)

# Validação: nenhuma linha deve ter sido perdida nos joins
total_itens = df_itens.count()
total_fato = fato_vendas.count()
print(f"Itens na Silver: {total_itens} | Linhas na fato: {total_fato}")
assert total_itens == total_fato, "Joins descartaram linhas! Verifique as chaves."


from pyspark.sql.functions import col
from pyspark.sql.types import DecimalType

fato_vendas = (
    fato_vendas
    .withColumn("valor_produto", col("valor_produto").cast(DecimalType(10, 2)))
    .withColumn("valor_frete",   col("valor_frete").cast(DecimalType(10, 2)))
    .withColumn("valor_total",
                (col("valor_produto") + col("valor_frete")).cast(DecimalType(10, 2)))
)

gravar_gold(fato_vendas, "fato_vendas")


# Gravação da camada Gold em Delta
tabelas_gold = {
    "dim_cliente": dim_cliente,
    "dim_produto": dim_produto,
    "dim_vendedor": dim_vendedor,
    "dim_tempo": dim_tempo,
    "fato_vendas": fato_vendas
}

CATALOG = "dtb_projeto_etl_7405616024732022"
SCHEMA_GOLD = "olist_gold"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_GOLD}")

def gravar_gold(df, nome):
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("path", f"{path_gold}{nome}")
        .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.{nome}")
    )
    print(f"✓ {CATALOG}.{SCHEMA_GOLD}.{nome} registrada")

for nome, df in tabelas_gold.items():
    gravar_gold(df, nome)

print("Camada Gold (star schema) gravada com sucesso!")


# %sql
# SELECT valor_total
# FROM dtb_projeto_etl_7405616024732022.olist_gold.fato_vendas
# LIMIT 5
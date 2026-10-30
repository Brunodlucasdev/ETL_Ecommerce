# Databricks notebook source
# Databricks notebook source
from pyspark.sql.functions import col, abs as fabs, min as fmin, max as fmax, year

CATALOG = "dtb_projeto_etl_7405616024732022"  # troque pelo nome do seu catálogo
SILVER = f"{CATALOG}.olist_silver"
GOLD = f"{CATALOG}.olist_gold"
PATH_BRONZE = "abfss://bronze@datalakebrunodev.dfs.core.windows.net/olist/"

resultados = []

def check(nome, condicao, detalhe=""):
    status = "PASSOU" if condicao else "FALHOU"
    resultados.append(status)
    print(f"[{status}] {nome} {detalhe}")

# 1. Existência das tabelas

tabelas_esperadas = [
    f"{SILVER}.pedidos", f"{SILVER}.itens_pedido", f"{SILVER}.clientes",
    f"{SILVER}.produtos", f"{SILVER}.vendedores", f"{SILVER}.pagamentos",
    f"{GOLD}.dim_cliente", f"{GOLD}.dim_produto", f"{GOLD}.dim_vendedor",
    f"{GOLD}.dim_tempo", f"{GOLD}.fato_vendas",
]

for t in tabelas_esperadas:
    try:
        spark.table(t).limit(1).count()
        check(f"Tabela existe: {t}", True)
    except Exception as e:
        check(f"Tabela existe: {t}", False, str(e)[:120])

# 2. Carga: Bronze x Silver x Gold

bronze_itens = (spark.read.option("header", "true")
                .csv(f"{PATH_BRONZE}olist_order_items_dataset.csv").count())
silver_itens = spark.table(f"{SILVER}.itens_pedido").count()
fato = spark.table(f"{GOLD}.fato_vendas")
qtd_fato = fato.count()

check("Itens: Bronze = Silver", bronze_itens == silver_itens,
      f"(bronze={bronze_itens}, silver={silver_itens})")
check("Itens: Silver = Fato (nenhum item perdido nos joins)",
      silver_itens == qtd_fato, f"(silver={silver_itens}, fato={qtd_fato})")

# 3. Unicidade nas tabelas Silver e chaves das dimensões

pedidos = spark.table(f"{SILVER}.pedidos")
check("Silver pedidos: order_id único",
      pedidos.count() == pedidos.select("order_id").distinct().count())

for tabela, chave in [
    ("dim_cliente", "sk_cliente"),
    ("dim_produto", "sk_produto"),
    ("dim_vendedor", "sk_vendedor"),
    ("dim_tempo", "sk_tempo"),
]:
    df = spark.table(f"{GOLD}.{tabela}")
    check(f"{tabela}: {chave} único",
          df.count() == df.select(chave).distinct().count())

# 4. Nulos nas chaves da fato

for chave in ["sk_cliente", "sk_produto", "sk_vendedor", "sk_tempo", "id_pedido", "id_item"]:
    nulos = fato.filter(col(chave).isNull()).count()
    check(f"Fato: {chave} sem nulos", nulos == 0, f"(nulos={nulos})")

# 5. Integridade referencial (cada FK da fato existe na dimensão)

ligacoes = [
    ("sk_cliente", f"{GOLD}.dim_cliente"),
    ("sk_produto", f"{GOLD}.dim_produto"),
    ("sk_vendedor", f"{GOLD}.dim_vendedor"),
    ("sk_tempo", f"{GOLD}.dim_tempo"),
]

for chave, dim in ligacoes:
    orfaos = fato.join(spark.table(dim).select(chave), chave, "left_anti").count()
    check(f"Integridade: fato -> {dim.split('.')[-1]}", orfaos == 0, f"(órfãos={orfaos})")

# 6. Granularidade: uma linha por item de pedido

pares_distintos = fato.select("id_pedido", "id_item").distinct().count()
check("Granularidade: (id_pedido, id_item) único na fato",
      qtd_fato == pares_distintos, f"(linhas={qtd_fato}, chaves={pares_distintos})")

# 7. Valores monetários e tipos

tipos = dict(fato.dtypes)
for c in ["valor_produto", "valor_frete", "valor_total"]:
    check(f"Tipo decimal: {c}", tipos[c].startswith("decimal"), f"(tipo={tipos[c]})")

soma_errada = fato.filter(
    fabs(col("valor_total") - (col("valor_produto") + col("valor_frete"))) > 0.001
).count()
check("valor_total = valor_produto + valor_frete", soma_errada == 0, f"(divergentes={soma_errada})")

negativos = fato.filter((col("valor_produto") < 0) | (col("valor_frete") < 0)).count()
check("Sem valores negativos na fato", negativos == 0, f"(negativos={negativos})")

# 8. Dimensão tempo e datas

dim_tempo = spark.table(f"{GOLD}.dim_tempo")
faixa = dim_tempo.agg(fmin("data").alias("ini"), fmax("data").alias("fim")).collect()[0]
print(f"Período das compras: {faixa['ini']} até {faixa['fim']}")

anos = [r["ano"] for r in dim_tempo.select(year("data").alias("ano")).distinct().collect()]
check("Anos dentro do período do Olist (2016 a 2018)",
      all(2016 <= a <= 2018 for a in anos), f"(anos={sorted(anos)})")

# Resumo final

total = len(resultados)
falhas = resultados.count("FALHOU")
print("\n" + "=" * 50)
print(f"Resultado: {total - falhas}/{total} testes passaram")
if falhas == 0:
    print("Pipeline validado com sucesso.")
else:
    print(f"{falhas} teste(s) falharam. Veja as linhas marcadas como FALHOU.")
# 🛒 Pipeline ETL de E-commerce: Arquitetura Medalhão & Star Schema

Pipeline de Engenharia de Dados *end-to-end* construído com o dataset público **Brazilian E-Commerce Public Dataset by Olist** (Kaggle). Os dados brutos são enviados para o Azure Data Lake Storage Gen2, processados no Azure Databricks com PySpark e Delta Lake, organizados em um modelo dimensional **Star Schema** e validados por uma suíte de testes automatizados.

---

## 🏗️ Arquitetura da Solução

```text
[ Dataset Olist (CSV) ]
        │
        ▼  Python + azure-storage-blob (upload em lote)
[ Azure Data Lake Gen2 ] ──► 🥉 Bronze: CSVs brutos            (container bronze, pasta olist/)
        │
        ▼  01_ingestao_e_limpeza_olist (PySpark)
[ Azure Data Lake Gen2 ] ──► 🥈 Silver: tabelas limpas e tipadas (Delta, catálogo olist_silver)
        │
        ▼  02_modelagem_gold_star_schema (PySpark + surrogate keys + joins)
[ Azure Data Lake Gen2 ] ──► 🥇 Gold: Star Schema              (Delta, catálogo olist_gold)
        │
        ▼  03_testes_pipeline (PySpark)
[ Validação ]            ──► 35 checagens de carga, integridade e regras de negócio
```

As tabelas Silver e Gold são registradas no **Unity Catalog** e podem ser consultadas com SQL.

---

## ⭐ Modelagem Dimensional (Star Schema) — Camada Gold

**Granularidade da fato:** uma linha por item de pedido, identificada por `(id_pedido, id_item)`.

| Tabela | Tipo | Chave | Colunas principais |
|---|---|---|---|
| `fato_vendas` | Fato | `id_pedido`, `id_item` | `sk_cliente`, `sk_produto`, `sk_vendedor`, `sk_tempo`, `status_pedido`, `valor_produto`, `valor_frete`, `valor_total` |
| `dim_cliente` | Dimensão | `sk_cliente` | `id_cliente`, `id_cliente_unico`, `cidade`, `estado` |
| `dim_produto` | Dimensão | `sk_produto` | `id_produto`, `categoria`, `peso_g`, `comprimento_cm`, `altura_cm`, `largura_cm` |
| `dim_vendedor` | Dimensão | `sk_vendedor` | `id_vendedor`, `cidade`, `estado` |
| `dim_tempo` | Dimensão | `sk_tempo` | `data`, `ano`, `mes`, `dia`, `trimestre`, `dia_semana` |

**Detalhes de modelagem:**

- As chaves `sk_*` são inteiras e geradas com `row_number()` ordenado pelo ID natural, o que mantém os valores estáveis entre execuções.
- `sk_tempo` está no formato `yyyyMMdd` (inteiro), por exemplo `20180203`.
- `id_pedido` fica na fato como *degenerate dimension*, pois é uma chave de negócio sem atributos próprios.
- As colunas monetárias (`valor_produto`, `valor_frete`, `valor_total`) são `decimal(10,2)`, para evitar erros de ponto flutuante.

---

## 📂 Estrutura do Repositório

```text
.
├── notebooks/                                   # Pipeline executado no Databricks
│   ├── 01_ingestao_e_limpeza_olist.py           # Bronze -> Silver
│   ├── 02_modelagem_gold_star_schema.py         # Silver -> Gold
│   └── 03_testes_pipeline.py                    # Suíte de testes
├── scripts/
│   └── upload_data.py                           # Upload em lote dos CSVs para a Bronze
├── .gitignore                                   # Ignora credenciais (.env), venv e dados locais
├── requirements.txt                             # Dependências Python
└── README.md
```

---

## 🚀 Como Executar

### Pré-requisitos

- Workspace Azure Databricks com Unity Catalog habilitado
- Storage account Azure Data Lake Gen2 com os containers `bronze`, `silver` e `gold`
- Permissão para criar schemas no catálogo
- Python 3 com as dependências de `requirements.txt` (para o script de upload)

### Passos

1. **Upload para a Bronze (local):** configure as credenciais no arquivo `.env` e execute `python scripts/upload_data.py`. Ele envia os CSVs para `bronze/olist/`.
2. **Silver:** rode `01_ingestao_e_limpeza_olist` no Databricks.
3. **Gold:** rode `02_modelagem_gold_star_schema`.
4. **Testes:** rode `03_testes_pipeline` e confirme que todas as checagens aparecem como `[PASSOU]`.

Ajuste a variável `CATALOG` nos notebooks para o nome do catálogo do seu workspace. Os notebooks devem ser executados nessa ordem.

---

## ✅ Testes de Qualidade

O notebook `03_testes_pipeline` executa **35 checagens** e exibe `[PASSOU]` ou `[FALHOU]` para cada uma:

| Categoria | O que é verificado |
|---|---|
| Existência | As 11 tabelas esperadas estão registradas no catálogo |
| Carga | A quantidade de itens se mantém da Bronze até a fato |
| Unicidade | `order_id` na Silver e as chaves `sk_*` das dimensões são únicas |
| Nulos | As chaves da fato não têm valores nulos |
| Integridade referencial | Nenhuma FK da fato fica sem correspondência nas dimensões |
| Granularidade | `(id_pedido, id_item)` é único na fato |
| Tipos e regras monetárias | Colunas monetárias em `decimal`, `valor_total = valor_produto + valor_frete` e nenhum valor negativo |
| Datas | O período das compras fica entre 2016 e 2018 |

---

## 🧠 Decisões Técnicas

- **Medallion em três camadas:** separa carga bruta, limpeza e modelagem, o que permite reprocessar cada etapa sem refazer as anteriores.
- **Delta Lake:** oferece escrita transacional e histórico de versões.
- **Surrogate keys inteiras:** a fato armazena inteiros em vez de strings de 32 caracteres, o que reduz o volume e acelera os joins.
- **Fuso horário de São Paulo:** os horários do Olist são de Brasília, então a sessão Spark usa `America/Sao_Paulo`.
- **Categorias traduzidas:** o join com `product_category_name_translation` traz as categorias em inglês.
- **Overwrite nas camadas:** cada execução reconstrói as tabelas, o que torna o pipeline idempotente.
- **Limpeza em funções reutilizáveis:** `tratar_datas`, `tratar_texto`, `remover_nulos_chave` e `remover_duplicados` são aplicadas a todas as tabelas.

---

## ⚠️ Limitações e Próximos Passos

- A `dim_cliente` usa `customer_id`, que identifica um cliente por pedido. O `customer_unique_id` identifica o cliente real. Uma dimensão de cliente único seria a evolução natural.
- A `dim_tempo` cobre apenas as datas de compra.
- Próximos passos: orquestração com Databricks Workflows, testes com Great Expectations ou dbt, e um dashboard no Power BI ou no Databricks SQL.

---

## 📚 Dataset

[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) (Kaggle).

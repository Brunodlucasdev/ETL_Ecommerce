# 🛒 Pipeline ETL de E-commerce: Arquitetura Medalhão & Star Schema

Pipeline de Engenharia de Dados *End-to-End* construído com dados do **Olist Store (Kaggle)**, aplicando a Arquitetura Medalhão (Bronze → Silver → Gold) na nuvem Azure e modelagem dimensional **Star Schema** no Databricks com PySpark e Delta Lake.

---

## 🏗️ Arquitetura da Solução

```text
[ Dataset Olist (CSV) ]
        │
        ▼  Python + azure-storage-blob (Upload em lote)
[ Extração Local / WSL ]
        │
        ▼ (ABFSS)
[ Azure Data Lake Gen2 ] ──► 🥉 Bronze: dados brutos em CSV (olist/)
        │
        ▼  PySpark + Unity Catalog
[ Azure Databricks ]
        │
        ▼ (Delta Lake)
[ Azure Data Lake Gen2 ] ──► 🥈 Silver: tabelas limpas e tipadas
        │
        ▼  PySpark + Surrogate Keys + Joins
[ Azure Data Lake Gen2 ] ──► 🥇 Gold: Modelo Estrela (Fato_Vendas + Dimensões)

⭐ Modelagem Dimensional (Star Schema) - Camada Gold
Tabela Fato: fato_vendas (Métricas de produto, frete e chaves de ligação).

Tabelas Dimensão:

dim_clientes: Localização e dados cadastrais do comprador.

dim_produtos: Categoria e especificações físicas dos produtos.

dim_vendedores: Cidade e estado do vendedor.

dim_tempo: Atributos temporais (ano, mês, dia, trimestre, dia da semana).

📂 Estrutura do Repositório
.
├── data/                                      # Diretório local ignorado (.gitignore)
├── notebooks/                                 # Pipelines executados no Databricks
│   ├── 01_ingestao_e_limpeza_olist.py        # Bronze -> Silver (Tratamento e limpeza)
│   ├── 02_modelagem_gold_star_schema.py       # Silver -> Gold (Star Schema com Surrogate Keys)
│   └── 03_testes_pipelines.py                # Suíte de testes e validação de dados
├── scripts/
│   └── upload_data.py                         # Ingestão em lote dos CSVs para a Bronze
├── .gitignore                                 # Proteção de credenciais (.env, venv, data)
├── requirements.txt                           # Dependências do projeto Python
└── README.md                                  # Documentação técnica do projeto


🛠️ Tecnologias Utilizadas
Linguagens: Python, PySpark, Spark SQL, Bash Shell

Nuvem: Azure Data Lake Storage Gen2 (Hierarchical Namespace)

Processamento: Azure Databricks (Serverless & Unity Catalog)

Formato de Armazenamento: Delta Lake

Versionamento & Segurança: Git/GitHub, python-dotenv


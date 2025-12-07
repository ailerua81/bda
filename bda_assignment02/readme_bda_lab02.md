
# Rapport BDA — Assignment 02  
**Big Data Analytics — ESIEE 2025–2026**

Text Analytics with PySpark (RDDs + DataFrames)

##  1. Objectif du TP

L’Assignment 02 explore l’analyse de données structurées avec **Spark SQL** et **DataFrames**, en se concentrant sur :

- L’ingestion de données tabulaires (CSV/Parquet)
- Les opérations relationnelles (sélections, filtres, jointures)
- La manipulation de schémas complexes
- Les agrégations avancées
- L’optimisation via les plans logiques et physiques
- L’usage des fonctions SQL et des UDF
- La persistance des résultats

Le jeu de données utilisé inclut des fichiers de transactions, clients, produits.


##  2. Structure du projet

```
Assignment02/
├── data/
│   ├── customers.csv
│   ├── transactions.csv
│   └── products.csv
├── Assignment02.ipynb
├── outputs/
│   ├── top_customers.csv
│   ├── category_sales.csv
│   ├── anomalies.csv
│   └── enriched_transactions.parquet
└── README_Lab02.md
```


##  3. Mise en place de l’environnement

Une SparkSession typique :

```python
spark = (
    SparkSession.builder
        .appName("BDA-A02")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
)
```

Affichage des versions pour la reproductibilité :

```python
spark.version
spark.sparkContext.getConf().getAll()
```

---

##  4. Chargement des données

Lecture sécurisée des fichiers :

```python
df_customers = spark.read.option("header", "true").csv("data/customers.csv")
df_transactions = spark.read.option("header", "true").csv("data/transactions.csv")
df_products = spark.read.option("header", "true").csv("data/products.csv")
```

Conversion des types :

```python
df_transactions = df_transactions     .withColumn("amount", col("amount").cast("double"))     .withColumn("quantity", col("quantity").cast("int"))
```

---

##  5. Analyses principales

### 5.1 Nettoyage des données

- Suppression des lignes incomplètes
- Détection d’anomalies : montants négatifs, quantités absurdes…

```python
df_clean = df_transactions.filter("amount > 0 AND quantity > 0")
```

---

### 5.2 Jointure entre clients, produits et transactions

```python
df_enriched = (
    df_clean.join(df_customers, "customer_id", "left")
            .join(df_products, "product_id", "left")
)
```

---

### 5.3 Top clients par chiffre d’affaires

```python
df_top = (
    df_enriched.groupBy("customer_id")
               .agg(sum(col("amount")).alias("total_spent"))
               .orderBy(col("total_spent").desc())
)
```

Export :

```
outputs/top_customers.csv
```

---

### 5.4 Montant des ventes par catégorie de produit

```python
df_category = (
    df_enriched.groupBy("category")
               .agg(sum(col("amount")).alias("total_sales"))
               .orderBy(col("total_sales").desc())
)
```

---

### 5.5 Détection d'anomalies

Règles :

- Montant unitaire incohérent
- Quantité trop grande
- Clients inexistants

Résultat enregistré dans :

```
outputs/anomalies.csv
```

---

##  6. Analyse des plans d’exécution

Pour chaque opération clé :

```python
df_top.explain("formatted")
```

Les plans mis en évidence :

- **Logical plan** : projection, filtres, agrégations  
- **Optimized logical plan** : élimination des colonnes inutiles  
- **Physical plan** : `HashAggregate`, `BroadcastHashJoin` si petit dataset  

Les fichiers suivants sont générés :

```
proof/
├── plan_top_customers.txt
├── plan_category_sales.txt
└── plan_anomalies.txt
```

---

##  7. Résultats principaux

### 🔹 Ventes totales par catégorie  
→ met en évidence les catégories dominantes.

### 🔹 Classement des meilleurs clients  
→ utile pour une segmentation clientèle.

### 🔹 Transactions suspectes  
→ fournit un rapport de qualité des données.

---

##  8. Persistance des résultats

```python
df_enriched.write.mode("overwrite").parquet("outputs/enriched_transactions.parquet")
```

---

##  9. Optimisations appliquées

- Broadcast des petites tables (`customers`, `products`)
- Cache intermédiaire pour jeu enrichi
- Réduction des partitions pour environnement local
- Projection des colonnes nécessaires uniquement

---

##  10. Livrables du TP

| Élément | Description |
|--------|-------------|
| readme_bda_lab02.md | Rapport complet |
| Notebook | Code du TP |
| outputs/*.csv | Résultats demandés |
| enriched_transactions.parquet | Table enrichie finale |
| plans d’exécution | Dans `/proof` |

---



# BDA Assignment 04 - Relational & Streaming Processing

##  Table des matières

- [Vue d'ensemble](#vue-densemble)
- [Structure du tp](#structure-du-tp)
- [Génération des données](#génération-des-données)
- [Partie A - Requêtes relationnelles (RDD)](#partie-a---requêtes-relationnelles-rdd)
- [Partie B - Streaming](#partie-b---streaming)
- [Résultats attendus](#résultats-attendus)
- [Troubleshooting](#troubleshooting)

---

##  Vue d'ensemble

Ce TP explore deux aspects fondamentaux du Big Data avec Apache Spark :

1. **Traitement batch relationnel** : Requêtes complexes sur des données TPC-H en utilisant uniquement les RDDs
2. **Traitement streaming** : Analyse en temps réel de données de taxis avec Structured Streaming

### Objectifs pédagogiques

- Maîtriser les opérations RDD (map, filter, join, reduce)
- Comprendre les stratégies de jointures (broadcast vs reduce-side)
- Implémenter des pipelines de streaming avec fenêtrage temporel
- Optimiser les performances avec partitionnement et caching

---




##  Structure du tp

```
bda_assignment04/
├── bda_assignment.py          # Script principal
├── README.md                  # Ce fichier
├── ENV.md                     # Documentation environnement (généré)
│
├── data/                      # Données d'entrée
│   ├── tpch/
│   │   ├── TPC-H-0.1-TXT/    # Tables au format texte pipe-delimited
│   │   │   ├── nation.tbl
│   │   │   ├── customer.tbl
│   │   │   ├── orders.tbl
│   │   │   ├── part.tbl
│   │   │   ├── supplier.tbl
│   │   │   └── lineitem.tbl
│   │   └── TPC-H-0.1-PARQUET/ # Tables au format Parquet
│   │       ├── nation/
│   │       ├── customer/
│   │       ├── orders/
│   │       ├── part/
│   │       ├── supplier/
│   │       └── lineitem/
│   └── taxi-data/             # Données de taxis NYC
│       ├── part-2021-01-01-0000.csv
│       ├── part-2021-01-01-0001.csv
│       └── part-2021-01-01-0002.csv
│
├── outputs/                   # Résultats des requêtes
│   ├── q1_results.txt
│   ├── q2_clerks.txt
│   ├── q3_parts_suppliers.txt
│   ├── q4_nation_shipments.txt
│   ├── q5_monthly_volumes.txt
│   ├── q6_pricing_summary.txt
│   ├── q7_shipping_priority.txt
│   ├── hourly_trip_count/
│   ├── region_trip_count/
│   └── trending_arrivals/
│
├── proof/                     # Preuves d'exécution
│   ├── plan_parquet_queries.txt
│   └── streaming_evidence.txt
│
└── checkpoints/               # Checkpoints streaming
    ├── hourly_trip_count/
    ├── region_trip_count/
    └── trending_arrivals/
```

---

##  Génération des données

Le script génère automatiquement des données de test si elles n'existent pas.

### Données TPC-H

**Schema TPC-H** : Modèle de base de données pour tests de benchmarking

- **nation** : 3 pays (USA, Canada, Brazil)
- **customer** : 3 clients associés à des nations
- **orders** : 4 commandes avec dates variées
- **part** : 2 pièces/produits
- **supplier** : 2 fournisseurs
- **lineitem** : 5 lignes d'articles (les items commandés)

**Formats générés** :
- **Text (pipe-delimited)** : `field1|field2|field3|...`
- **Parquet** : Format columnar optimisé

### Données Taxi

**12 trajets de taxis NYC** répartis sur 3 fichiers CSV :
- Timestamps de pickup/dropoff
- Coordonnées GPS (longitude, latitude)
- Distance et nombre de passagers

---

##  Partie A - Requêtes relationnelles (RDD)

Toutes les requêtes sont implémentées **uniquement avec des RDDs**, sans utiliser l'API DataFrame (sauf pour lecture Parquet).

### A1 - Q1 : Comptage d'items expédiés à une date

**Objectif** : Compter les items expédiés le `1995-03-15`

**Approche** :
```python
lineitem_rdd
  .filter(lambda row: row.l_shipdate == '1995-03-15')
  .count()
```

**Formats testés** :
- Text (pipe-delimited)
- Parquet (via `spark.read.parquet().rdd`)

**Résultat attendu** : `ANSWER = 3`

---

### A2 - Q2 : Clerks par order key (Reduce-side join)

**Objectif** : Joindre `orders` et `lineitem` pour obtenir les clerks des commandes expédiées

**Stratégie** : **cogroup** (reduce-side join)

**Pipeline** :
```python
orders_rdd = orders.map(lambda r: (r.o_orderkey, r.o_clerk))
lineitem_rdd = lineitem.filter(date).map(lambda r: (r.l_orderkey, 1))

joined = orders_rdd.cogroup(lineitem_rdd)
  .filter(lambda kv: len(kv[1][1]) > 0)  # Garder seulement les matches
  .flatMap(lambda kv: [(kv[0], clerk) for clerk in kv[1][0]])
  .sortByKey()
  .take(20)
```

**Explication cogroup** :
- Groupe les valeurs des deux RDDs par clé commune
- Retourne `(key, (iter_valeurs_rdd1, iter_valeurs_rdd2))`
- Plus flexible qu'un join simple

---

### A3 - Q3 : Parts & Suppliers (Broadcast join)

**Objectif** : Enrichir les lineitems avec les noms des parts et suppliers

**Stratégie** : **Broadcast join** (map-side join)

**Pourquoi broadcast ?**
- `part` et `supplier` sont de petites tables
- On les charge en mémoire et on les diffuse à tous les workers
- Évite un shuffle coûteux

**Pipeline** :
```python
# Créer des dictionnaires
part_map = {r.p_partkey: r.p_name for r in load_tpch_text('part').collect()}
supplier_map = {r.s_suppkey: r.s_name for r in load_tpch_text('supplier').collect()}

# Broadcaster
part_bc = spark.sparkContext.broadcast(part_map)
supp_bc = spark.sparkContext.broadcast(supplier_map)

# Map-side join
lineitem_rdd
  .map(lambda r: (r.l_orderkey, 
                  part_bc.value.get(r.l_partkey), 
                  supp_bc.value.get(r.l_suppkey)))
  .filter(lambda t: t[1] is not None and t[2] is not None)
```

**Avantages** :
- Pas de shuffle réseau
- Beaucoup plus rapide pour petites dimensions


### A4 - Q4 : Shipments par nation (Mixed joins)

**Objectif** : Compter les items expédiés par nation

**Stratégie** : **Combinaison de joins**
- Reduce-side join : `lineitem ⨝ orders` (grandes tables)
- Broadcast join : `customer` et `nation` (petites tables)

**Pipeline** :
```python
# Broadcast des petites tables
customer_map = {c.c_custkey: c.c_nationkey for c in customers}
nation_map = {n.n_nationkey: n.n_name for n in nations}

# Reduce-side join
lineitem_by_order = lineitem.map(lambda r: (r.l_orderkey, 1))
orders_by_key = orders.map(lambda r: (r.o_orderkey, r.o_custkey))
joined = lineitem_by_order.join(orders_by_key)

# Map-side join et agrégation
nation_counts = (joined
  .map(lambda kv: (customer_bc.value.get(kv[1][1]), kv[1][0]))
  .map(lambda t: ((t[0], nation_bc.value.get(t[0])), t[1]))
  .reduceByKey(add)
  .map(lambda t: (t[0][0], t[0][1], t[1]))  # (nationkey, name, count)
  .sortBy(lambda t: (-t[2], t[1]))
)
```

---

### A5 - Q5 : Volumes mensuels US vs Canada

**Objectif** : Calculer le nombre de shipments par mois pour USA et Canada

**Approche** :
1. Joindre toutes les tables : `lineitem ⨝ orders ⨝ customer ⨝ nation`
2. Filtrer sur `UNITED STATES` et `CANADA`
3. Extraire le mois avec `month_trunc('1995-03-15') → '1995-03'`
4. Grouper par `(nationkey, nation_name, month)` et compter

**Résultat** : Série temporelle pour analyse comparative

---

### A6 - Q6 : Résumé des prix

**Objectif** : Statistiques par `(returnflag, linestatus)`

**Métriques calculées** :
- Somme des quantités
- Somme des prix étendus
- Somme des discounts
- Nombre de lignes
- Prix moyen
- Quantité moyenne

**Pipeline** :
```python
lineitem.filter(date)
  .map(lambda r: ((r.l_returnflag, r.l_linestatus),
                  (r.l_quantity, r.l_extendedprice, r.l_discount, 1)))
  .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1], a[2]+b[2], a[3]+b[3]))
  .map(lambda t: (t[0][0], t[0][1], t[1][0], t[1][1], 
                  t[1][0]/t[1][3], t[1][1]/t[1][3]))  # Moyennes
```

---

### A7 - Q7 : Top 10 des priorités d'expédition

**Objectif** : Trouver les 10 commandes avec le plus haut revenu

**Filtres** :
- Orders : `o_orderdate < '1995-04-01'`
- Lineitem : `l_shipdate > '1995-03-01'`

**Calcul du revenu** :
```python
revenue = l_extendedprice * (1 - l_discount)
```

**Pipeline** :
```python
customer.join(orders)
  .map(lambda kv: (orderkey, (custname, shippriority)))
  .join(lineitem.map(lambda r: (r.l_orderkey, revenue)))
  .map(lambda kv: ((custname, orderkey, priority), revenue))
  .reduceByKey(add)
  .sortBy(lambda t: -t[1])
  .take(10)
```

---

##  Partie B - Streaming

Utilisation de **Structured Streaming** pour traiter des flux de données taxi en temps réel.

### B1 - HourlyTripCount

**Objectif** : Compter les trajets par heure de pickup

**Configuration** :
```python
stream = spark.readStream
  .schema(TAXI_SCHEMA)
  .option('header', True)
  .csv(str(TAXI_DIR))

hourly = stream
  .withColumn('pickup_hour', F.date_trunc('hour', 'tpep_pickup_datetime'))
  .groupBy('pickup_hour')
  .agg(F.count('*').alias('trip_count'))
```

**Output** : Mode `update` (seules les nouvelles agrégations)

---

### B2 - RegionTripCount

**Objectif** : Compter les trajets par région de dropoff (Goldman Sachs vs Citigroup)

**Bounding boxes GPS** :
```python
BOUNDING_BOXES = {
    'goldman': {
        'lon_min': -74.0145, 'lon_max': -74.0115,
        'lat_min': 40.7125, 'lat_max': 40.7155
    },
    'citigroup': {
        'lon_min': -74.0095, 'lon_max': -74.0055,
        'lat_min': 40.7190, 'lat_max': 40.7225
    }
}
```

**Enrichissement** :
```python
stream.withColumn('region',
  F.when(lon/lat dans bbox_goldman, 'goldman')
   .when(lon/lat dans bbox_citigroup, 'citigroup')
)
.filter(F.col('region').isNotNull())
.groupBy('region', F.window('tpep_dropoff_datetime', '1 hour'))
.count()
```

---

### B3 - TrendingArrivals

**Objectif** : Détecter les fenêtres avec augmentation de trafic (trending)

**Fenêtrage** : 10 minutes

**Watermark** : 5 minutes (tolérance pour données en retard)

**Détection de tendance** :
```python
def write_trending(batch_df, epoch_id):
    data = batch_df.orderBy('window_start').collect()
    for i in range(1, len(data)):
        prev = data[i-1].trip_count
        curr = data[i].trip_count
        if curr > prev * 1.5:  # +50% = trending
            print(f'ALERT: Trending at {data[i].window_start}')
```

**Concepts clés** :
- **Watermark** : Gestion des événements tardifs
- **Window** : Découpage temporel des données
- **State** : Comparaison entre fenêtres successives


## Résultats attendus

### Partie A - Outputs

| Fichier | Contenu |
|---------|---------|
| `q1_results.txt` | Comptage par format (text: 3, parquet: 3) |
| `q2_clerks.txt` | Liste (orderkey, clerk) |
| `q3_parts_suppliers.txt` | (orderkey, part_name, supplier_name) |
| `q4_nation_shipments.txt` | (nationkey, nation_name, count) |
| `q5_monthly_volumes.txt` | (nationkey, nation, month, count) |
| `q6_pricing_summary.txt` | Statistiques par (returnflag, linestatus) |
| `q7_shipping_priority.txt` | Top 10 (customer, orderkey, revenue, priority) |

### Partie B - Outputs

| Répertoire | Format | Contenu |
|------------|--------|---------|
| `hourly_trip_count/` | Parquet | (pickup_hour, trip_count) |
| `region_trip_count/` | Parquet | (region, dropoff_hour, trip_count) |
| `trending_arrivals/` | Parquet | (window_start, window_end, trip_count) |

### Evidence

| Fichier | Contenu |
|---------|---------|
| `ENV.md` | Versions complètes (Python, Spark, Java, OS) |
| `proof/plan_parquet_queries.txt` | Plans d'exécution Spark |
| `proof/streaming_evidence.txt` | Logs et configurations streaming |

---


---

##  Concepts clés abordés

### RDD Operations

| Opération | Type | Usage |
|-----------|------|-------|
| `map()` | Transformation | Transformation 1-to-1 |
| `flatMap()` | Transformation | Transformation 1-to-N |
| `filter()` | Transformation | Filtrage de données |
| `reduceByKey()` | Transformation | Agrégation par clé |
| `join()` | Transformation | Reduce-side join |
| `cogroup()` | Transformation | Join flexible multi-RDD |
| `count()` | Action | Comptage |
| `collect()` | Action | Récupération des données |
| `take(n)` | Action | Récupération de n éléments |

### Join Strategies

| Stratégie | Quand l'utiliser | Avantages | Inconvénients |
|-----------|------------------|-----------|---------------|
| **Broadcast Join** | Petite table < 10MB | Pas de shuffle, très rapide | Table doit tenir en mémoire |
| **Reduce-side Join** | Grandes tables | Scalable | Shuffle coûteux |
| **Mixed** | Combinaison | Optimal | Plus complexe |

### Streaming Concepts

| Concept | Description |
|---------|-------------|
| **Watermark** | Tolérance pour événements tardifs |
| **Window** | Découpage temporel des flux |
| **Trigger** | Fréquence de traitement (once, continuous, interval) |
| **Output Mode** | append, update, complete |
| **Checkpoint** | Sauvegarde d'état pour fault tolerance |

---


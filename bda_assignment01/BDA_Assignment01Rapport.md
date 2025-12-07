# Big Data Analytics — Assignment 01 (Chapter 1 & 2: Intro + Algorithm Design)

🎯 Objectif général du tp

Je dois :

1. Manipuler un texte (shakespeare.txt) avec PySpark.
2. Faire deux analyses :
    Partie A : mots qui suivent immédiatement “perfect” (→ “perfect followers”).
    Partie B : cooccurrences de mots (→ PMI avec deux approches : pairs et stripes).
3. Fournir des fichiers de sortie + preuves Spark UI + environnement.

## 🧩 Étape 1 — Prépareration du dossier de travail

Dans le dossier de projet (dans VSCode, en mode WSL) :


lab1-assignment
```
mkdir -p ~/bda_assign01/data ~/bda_assign01/outputs ~/bda_assign01/proof
cd ~/bda_assign01
```

Je dois placer dans data/ le fichier texte du corpus :

```
data/shakespeare.txt
```


## 🧠 Étape 2 — Ouvrir le notebook dans VSCode

1. J'active mon environnement :
```
conda activate bda-env
```

2. Je lance JupyterLab :
```
jupyter lab
```

3. J'ouvre *BDA_Assignment01.ipynb* dans le navigateur (ou directement dans VSCode). Le kernel actif est être Python(bda-env)

## 🧮 Étape 3 — Partie A : “perfect x” followers
🎯 Objectif :

Compter les mots qui suivent immédiatement le mot “perfect” (en ignorant la casse) sur la même ligne, et ne garder que ceux qui apparaissent plus d’une fois.

Code (à adapter dans le notebook) :
```
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, lower, split, col

spark = SparkSession.builder.appName("BDA_Assignment01").getOrCreate()

# Charger le texte
lines = spark.read.text("data/shakespeare.txt").toDF("line")

# Extraire les mots par ligne
words_df = lines.select(split(lower(col("line")), r"[^a-z]+").alias("tokens"))

# Créer les couples (w_i, w_{i+1})
pairs_df = words_df.selectExpr(
    "TRANSFORM(sequence(0, size(tokens)-2), i -> struct(tokens[i] as word, tokens[i+1] as follower)) as pairs"
)

# Extraire les paires sous forme plate
flat_pairs = pairs_df.select(explode("pairs").alias("p")).select("p.*")

# Filtrer sur le mot "perfect"
perfect_followers = flat_pairs.filter(col("word") == "perfect")

# Compter les followers
counts = perfect_followers.groupBy("follower").count().filter(col("count") > 1)

# Sauvegarde
counts.orderBy(col("count").desc()).write.csv("outputs/perfect_followers.csv", header=True, mode="overwrite")

# Plan d'exécution
with open("proof/plan_perfect.txt", "w") as f:
    f.write(counts._jdf.queryExecution().executedPlan().toString())

counts.show(10)
```


📷 À capturer :
 - Spark UI à http://localhost:4040
 - Rubriques : Files Read, Input Size, Shuffle Read/Write
 - A sauvegarder dans proof/plan_perfect.txt

## 📊 Étape 4 — Partie B : PMI avec pairs et stripes

🔹 Partie B1 — Méthode “Pairs”

🎯 Objectif : calculer la PMI(x,y) (Pointwise Mutual Information) des mots co-occurrents sur la même ligne :

![alt text](img/formule1.png)

en utilisant une approche RDD (x,y) → (PMI, count).

💻 Exemple de structure : 
```
import math

rdd = spark.sparkContext.textFile("data/shakespeare.txt")

# Preprocessing
def tokenize(line):
    import re
    tokens = re.split(r"[^a-z]+", line.lower())
    return [t for t in tokens if t][:40]

tokens_rdd = rdd.map(tokenize).filter(lambda x: len(x) > 1)

# Count occurrences
word_counts = tokens_rdd.flatMap(lambda ws: [(w, 1) for w in ws]).reduceByKey(lambda a,b: a+b)

# Count pair co-occurrences
pair_counts = tokens_rdd.flatMap(
    lambda ws: [((x, y), 1) for i, x in enumerate(ws) for y in ws[i+1:]]
).reduceByKey(lambda a,b: a+b)

# Total lines
num_lines = tokens_rdd.count()

# Compute PMI for pairs with co-occurrence ≥ K
K = 5

def compute_pmi(pair, count):
    x, y = pair
    if count < K:
        return None
    px = word_counts.lookup(x)[0] / num_lines
    py = word_counts.lookup(y)[0] / num_lines
    pxy = count / num_lines
    return (x, y, math.log10(pxy / (px * py)), count)

pmi_pairs = pair_counts.map(lambda kv: compute_pmi(kv[0], kv[1])).filter(lambda x: x is not None)
pmi_pairs_df = spark.createDataFrame(pmi_pairs, ["x", "y", "PMI", "count"])

pmi_pairs_df.write.csv("outputs/pmi_pairs_sample.csv", header=True, mode="overwrite")

# Plan d'exécution
with open("proof/plan_pmi_pairs.txt", "w") as f:
    f.write(pmi_pairs_df._jdf.queryExecution().executedPlan().toString())
```

🔸 Partie B2 — Méthode “Stripes”

🎯 Objectif : même calcul mais avec une structure x → { y: (PMI, count) }


💻 Exemple de structure :

```
from collections import defaultdict

def build_stripes(words):
    stripes = defaultdict(lambda: defaultdict(int))
    for i, x in enumerate(words):
        for y in words[i+1:]:
            stripes[x][y] += 1
    return list(stripes.items())

stripes_rdd = tokens_rdd.flatMap(build_stripes)

# Réduction par clé
def merge_dicts(d1, d2):
    for k, v in d2.items():
        d1[k] += v
    return d1

merged = stripes_rdd.reduceByKey(merge_dicts)

# Calcul PMI pour chaque stripe
def compute_pmi_stripe(x, stripe):
    result = {}
    px = word_counts.lookup(x)[0] / num_lines
    for y, cxy in stripe.items():
        if cxy >= K:
            py = word_counts.lookup(y)[0] / num_lines
            pxy = cxy / num_lines
            result[y] = (math.log10(pxy / (px * py)), cxy)
    return (x, result)

pmi_stripes = merged.map(lambda kv: compute_pmi_stripe(kv[0], kv[1]))

# Exemple d’enregistrement (limité)
sample = pmi_stripes.take(5)
import json
with open("outputs/pmi_stripes_sample.csv", "w") as f:
    for x, stripe in sample:
        f.write(f"{x},{json.dumps(stripe)}\n")

with open("proof/plan_pmi_stripes.txt", "w") as f:
    f.write("PMI stripes computed successfully\n")
```

## 📁 Étape 5 — Fichiers à livrer : 

| Fichier                          | Description                                                   |
| -------------------------------- | ------------------------------------------------------------- |
| `outputs/perfect_followers.csv`  | Résultats Part A                                              |
| `proof/plan_perfect.txt`         | Plan Spark Part A                                             |
| `outputs/pmi_pairs_sample.csv`   | Résultats PMI (Pairs)                                         |
| `proof/plan_pmi_pairs.txt`       | Plan Spark (Pairs)                                            |
| `outputs/pmi_stripes_sample.csv` | Résultats PMI (Stripes)                                       |
| `proof/plan_pmi_stripes.txt`     | Plan Spark (Stripes)                                          |
| `ENV.md`                         | Versions et config Spark                                      |
| `lab_metrics_log.csv`            | Journal Spark UI (Files Read, Input Size, Shuffle Read/Write) |
| `BDA_Assignment01.ipynb`         | Notebook complet et exécuté                                   |


## 🧾 Étape 6 — Exemple de ENV.md

```
# Environment Information

**Python**: 3.10  
**PySpark**: 4.0.1  
**Java**: OpenJDK 21  
**OS**: Ubuntu 24.04 (WSL2)  
**Spark master**: local[4]

## Spark Configs
spark.sql.shuffle.partitions=8
spark.executor.memory=2g
spark.driver.memory=2g

```



## 🧠 Étape 7 — Fichier `lab_metrics_log.csv`

Format CSV exemple :

```
---
| run_id | task | files_read | input_size_MB | shuffle_read_MB | shuffle_write_MB | timestamp |
|--------|------|-------------|----------------|-----------------|------------------|------------|
| 2025-11-11-01 | perfect_followers | 1 | 5 | 0.2 | 0.05 | 2025-11-11 15:00 |
| 2025-11-11-02 | pmi_pairs | 1 | 5 | 0.6 | 0.4 | 2025-11-11 15:45 |
| 2025-11-11-03 | pmi_stripes | 1 | 5 | 0.3 | 0.3 | 2025-11-11 16:00 |

---
```
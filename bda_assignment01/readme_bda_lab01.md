# Rapport BDA — Assignment 01

Text Analytics with PySpark (RDDs + DataFrames)
ESIEE 2025–2026

## 1. Overview

Ce tp met en œuvre plusieurs techniques fondamentales d’analyse de texte avec Apache Spark :
 - Manipulation de RDD et DataFrames
 - Tokenisation et nettoyage de texte
 - Calcul de co-occurrences
 - Implémentation des approches Pairs et Stripes
 - Calcul du PMI (Pointwise Mutual Information)
 - Analyse des performances avec Spark UI
 - Mise en place d’un environnement reproductible

Dataset utilisé : shakespeare.txt (~5 MB).

## 2. Structure du tp (assignment 01)
```
.
├── BDA_Assignment01.ipynb
├── data/
│   └── shakespeare.txt
├── outputs/
│   ├── perfect_followers.csv
│   ├── pmi_pairs_sample.csv
│   └── pmi_stripes_sample.csv
├── proof/
│   ├── plan_perfect.txt
│   ├── plan_pmi_pairs.txt
│   └── plan_pmi_stripes.txt
├── ENV.md
├── lab_metrics_log.csv
└── README_Lab01.md
```

##  3. Environment & Bootstrap
### 3.1 SparkSession (Profile A)

Configuration utilisée :
  - Python : 3.10
  - Spark : 3.x
  - PySpark : 3.x
  - Timezone : UTC
  - Shuffle partitions réduites pour une exécution locale :

```
spark = (
    SparkSession.builder
        .appName("BDA-A01")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
)
```

### 3.2 Versions 
détaillées dans le fichier ENV.md

## 4. Dataset

data/shakespeare.txt


## 5. Part A — Perfect X Followers

### Objectif

Compter les mots apparaissant juste après le mot perfect (insensible à la casse).
On élimine les mots dont la fréquence est égale à 1.

### Pipeline utilisé

1. Tokenisation en minuscules
2. Découpage sur caractères non alphabétiques
3. Extraction des followers
4. Explosion + agrégation
5. Filtre count > 1
6. Export CSV
7. Export plan d’exécution (formatted)


### Livrables

 - outputs/perfect_followers.csv
 - proof/plan_perfect.txt
 - Screenshot Spark UI (stages)
 - Entrée dédiée dans lab_metrics_log.csv

## 6. Part B — PMI with Pairs and Stripes (RDDs)

Nous calculons :

![alt text](img/formule1.png)



(Co-occurrence définie dans la même ligne.)

### Règles de prétraitement

 - minuscules
 - split sur [^a-z]+
 - suppression tokens vides
 - seulement les 40 premiers tokens par ligne
 - seuil K sur le nombre d’apparitions
 - log base 10

### 6.1 Variant A — Pairs
Représentation :
((x, y), count)


Étapes :    
- Tokeniser
- Générer les paires non ordonnées
- Reducer pour compter
- Compter les occurrences individuelles
- Calcul du PMI
- Appliquer le seuil
- Export des paires les plus fréquentes

Livrables :

 - outputs/pmi_pairs_sample.csv
 - proof/plan_pmi_pairs.txt
 - Captures Spark UI + entrées dans lab_metrics_log.csv

### 6.2 Variant B — Stripes
Représentation :
x → { y1: count, y2: count, ... }


Étapes :
- Map vers dictionnaires
- Merge stripes
- Recalcul des PMIs
- Export d'un échantillon

Les résultats doivent être cohérents avec l'approche Pairs.

Livrables :
- outputs/pmi_stripes_sample.csv
- proof/plan_pmi_stripes.txt
- Captures Spark UI + log CSV


## 7. Spark UI Metrics & Reproducibility
### 7.1 Fichier obligatoire : lab_metrics_log.csv

Contient pour chaque exécution :
- run_id
- task (perfect, pmi_pairs, pmi_stripes)
- input_files
- input_size
- shuffle_read
- shuffle_write
- timestamp

Ce fichier sert de journal technique pour expliquer les variations entre exécutions.

### 7.2 Captures Spark UI

Inclure dans /proof ou dans le notebook :

- DAG / Stages
- Shuffle metrics
- Jobs utilisés

## 8. Efficiency Notes

Optimisations appliquées :
- spark.sql.shuffle.partitions = 4
- Limitation à 40 tokens pour limiter la combinatoire
- Pas de collect() inutile
- Préférer mapPartitions pour les counts intermédiaires
- Dataset suffisamment petit pour tenir dans une machine locale, mais pipeline 100% distribué

## 9. Checklist des livrables finaux
Élément	Statut
- Part A : CSV   ---> OK
- Part A : plan Spark ---> OK
- Part B pairs : CSV ---> OK
- Part B pairs : plan  ---> OK
- Part B stripes : CSV	---> OK
- Part B stripes : plan	---> OK
- Spark UI screenshots	---> OK
- lab_metrics_log.csv rempli ---> OK
- ENV.md complet ---> OK


Tout le travail est centralisé dans BDA_Assignment01.ipynb.


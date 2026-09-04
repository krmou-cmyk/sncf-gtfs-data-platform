# 🚆 SNCF GTFS Data Engineering Platform

Projet **Data Engineering end-to-end** construit avec **Databricks, PySpark, Delta Lake et Unity Catalog** à partir des données GTFS publiques de la SNCF.

L'objectif est de construire un pipeline de données complet suivant une architecture **Medallion (Bronze / Silver / Gold)**, avec nettoyage, contrôles qualité, modélisation dimensionnelle, optimisation, idempotence et orchestration.

---

## 🎯 Objectifs du projet

Ce projet met en pratique plusieurs concepts de Data Engineering :

- Ingestion de données GTFS
- Architecture Medallion : Bronze / Silver / Gold
- Transformations avec PySpark
- Stockage avec Delta Lake
- Gouvernance avec Unity Catalog
- Data Quality
- Contrôles d'intégrité PK / FK
- Modélisation dimensionnelle
- Star Schema
- Orchestration avec Databricks Jobs & Workflows
- Gestion de l'idempotence
- Optimisation Spark / Delta
- Paramétrage des environnements
- Versioning avec Git / GitHub
- Databricks Declarative Automation Bundles

---

# 📊 Source des données

Ce projet utilise le jeu de données public **Horaires SNCF**, publié sur la plateforme officielle **SNCF Open Data**.

Les données sont fournies au format **GTFS (General Transit Feed Specification)** et décrivent notamment les opérateurs, lignes, trajets, gares, horaires théoriques et dates de circulation.

### 🔗 Téléchargement

Les données peuvent être téléchargées depuis la plateforme officielle SNCF Open Data :

**➡️ [Télécharger les données GTFS SNCF](https://ressources.data.sncf.com/explore/dataset/horaires-sncf/)**

Les données sources ne sont volontairement **pas stockées dans ce repository GitHub**.

Après téléchargement et extraction du fichier GTFS, les fichiers utilisés par le pipeline sont :

| Fichier | Description |
|---|---|
| `agency.txt` | Informations sur les opérateurs de transport |
| `routes.txt` | Lignes de transport |
| `trips.txt` | Trajets associés aux lignes |
| `stops.txt` | Gares et points d'arrêt |
| `stop_times.txt` | Horaires de passage aux arrêts |
| `calendar_dates.txt` | Dates de circulation des services |
| `transfers.txt` | Règles de correspondance entre arrêts |
| `feed_info.txt` | Métadonnées du feed GTFS |

### Chargement dans Databricks

Après téléchargement, les fichiers `.txt` doivent être déposés dans un **Unity Catalog Volume** utilisé comme Landing Zone.

Format :

```text
/Volumes/<catalog>/<schema>/<volume>/
```

Exemple utilisé pendant le développement :

```text
/Volumes/workspace/sncf_bronze/landing/
```

Le flux d'entrée est donc :

```text
SNCF Open Data
       │
       ▼
    GTFS ZIP
       │
       ▼
Extraction des fichiers
       │
       ▼
Unity Catalog Volume
   Landing Zone
       │
       ▼
     Bronze
```

Cette approche permet de séparer :

- le **code**, versionné dans GitHub ;
- les **données**, récupérées depuis la source officielle et stockées dans Databricks.

---

# 🏗️ Architecture

Le pipeline suit une architecture **Medallion** :

```text
                 SNCF GTFS
                     │
                     ▼
               Landing Zone
                     │
                     ▼
              ┌────────────┐
              │   BRONZE   │
              │  Raw Data  │
              └─────┬──────┘
                    │
                    ▼
              ┌────────────┐
              │   SILVER   │
              │Cleaned Data│
              └─────┬──────┘
                    │
                    ▼
              ┌────────────┐
              │    GOLD    │
              │ Analytics  │
              └─────┬──────┘
                    │
                    ▼
           Quality & Optimization
```

---

# 🥉 Bronze Layer

La couche Bronze conserve les données GTFS proches de leur format source.

Les fichiers sont lus depuis la Landing Zone et stockés sous forme de tables **Delta Lake**.

Des métadonnées techniques sont ajoutées :

```text
_ingestion_timestamp
_source_system
_source_file
_batch_id
```

Exemple :

```text
GTFS stops.txt
      ↓
PySpark ingestion
      ↓
workspace.sncf_bronze.stops
```

Cette couche permet de conserver une représentation proche des données sources avant transformation.

---

# 🥈 Silver Layer

La couche Silver contient les données nettoyées et préparées.

Les principales transformations comprennent :

- suppression/nettoyage des valeurs incorrectes ;
- normalisation des colonnes ;
- typage des données ;
- gestion des valeurs nulles ;
- détection des doublons ;
- validation des clés logiques ;
- contrôles d'intégrité référentielle.

Les principales relations GTFS sont :

```text
AGENCY
   │
   ▼
ROUTES
   │
   ▼
TRIPS
   │
   ▼
STOP_TIMES ◄──────── STOPS


TRIPS
   │
   ▼
CALENDAR_DATES


STOPS
   │
   ▼
TRANSFERS
```

### Clés logiques principales

| Table | Clé logique |
|---|---|
| `agency` | `agency_id` |
| `routes` | `route_id` |
| `trips` | `trip_id` |
| `stops` | `stop_id` |
| `calendar_dates` | `(service_id, date)` |
| `stop_times` | `(trip_id, stop_sequence)` |
| `transfers` | `(from_stop_id, to_stop_id, transfer_type)` |

---

# 🥇 Gold Layer

La couche Gold transforme les données Silver en un **modèle dimensionnel** destiné à l'analyse.

Elle contient des dimensions et des tables de faits.

### Dimensions

```text
dim_station
dim_agency
dim_route
dim_service
dim_date
```

### Tables de faits

```text
fact_stop_times
fact_transfers
```

---

# ⭐ Star Schema

La principale table de faits est :

```text
fact_stop_times
```

Le modèle analytique est organisé comme suit :

```text
                       DIM_DATE
                           │
                           │
DIM_ROUTE ─────── FACT_STOP_TIMES ─────── DIM_STATION
    │                      │
    │                      │
DIM_AGENCY             DIM_SERVICE
```

La granularité de `fact_stop_times` est :

> **1 ligne = 1 passage planifié à un arrêt pour un trajet et une date de service.**

La clé logique correspond à :

```text
(date, trip_id, stop_sequence)
```

Les dimensions utilisent des **surrogate keys** :

```text
station_sk
agency_sk
route_sk
service_sk
date_sk
```

---

# 🔄 Orchestration

Le pipeline est orchestré avec **Databricks Jobs & Workflows**.

Le Workflow exécute les différentes couches dans l'ordre :

```text
pipeline_control
       │
       ▼
  Nouveau feed ?
     /     \
   TRUE    FALSE
    │        │
    ▼        └────────► FIN
 Bronze
    │
    ▼
 Silver
    │
    ▼
  Gold
    │
    ▼
Quality / Optimization
    │
    ▼
mark_success
```

---

# 🔁 Idempotence

Une table de contrôle est utilisée :

```text
workspace.sncf_bronze.pipeline_control
```

Le notebook `pipeline_control` récupère les informations du `feed_info.txt` et vérifie si le feed a déjà été traité.

Deux résultats sont possibles :

```text
NEW_FEED
```

ou :

```text
ALREADY_PROCESSED
```

Si le feed est nouveau :

```text
NEW_FEED
   ↓
Pipeline exécuté
   ↓
mark_success
   ↓
status = SUCCESS
```

Si le feed a déjà été traité :

```text
ALREADY_PROCESSED
       ↓
Pas de retraitement
```

Cette logique permet d'éviter le retraitement inutile du même snapshot GTFS.

---

# ✅ Data Quality

Plusieurs contrôles de qualité sont réalisés.

### Null checks

Vérification des clés importantes :

```text
station_sk
route_sk
service_sk
date_sk
```

### Duplicate checks

Vérification de l'unicité des dimensions et de la granularité des facts.

### Referential Integrity

Exemples :

```text
routes.agency_id
        ↓
agency.agency_id
```

```text
trips.route_id
        ↓
routes.route_id
```

```text
stop_times.trip_id
        ↓
trips.trip_id
```

```text
stop_times.stop_id
        ↓
stops.stop_id
```

Les contrôles d'intégrité référentielle utilisent notamment des **Left Anti Joins PySpark**.

---

# ⚡ Optimisation Spark / Delta

Le projet met en pratique plusieurs mécanismes d'optimisation.

### Broadcast Join

Les petites dimensions peuvent être broadcastées lors des jointures avec les grandes tables :

```python
broadcast(dim_station)
```

Cela permet de limiter certains shuffles.

### Adaptive Query Execution

Spark peut adapter le plan d'exécution en fonction des données réellement rencontrées pendant le traitement.

### Execution Plan

Les plans Spark peuvent être analysés avec :

```python
df.explain(mode="formatted")
```

### Delta Optimization

Les tables peuvent être optimisées avec :

```sql
OPTIMIZE workspace.sncf_gold.fact_stop_times;
```

Les statistiques peuvent être calculées avec :

```sql
ANALYZE TABLE workspace.sncf_gold.fact_stop_times
COMPUTE STATISTICS;
```

Le projet est développé et testé avec **Databricks Serverless**.

---

# ⚙️ Paramétrage

Le Job utilise plusieurs paramètres :

```text
catalog
bronze_schema
silver_schema
gold_schema
landing_path
```

Exemple :

```text
catalog        = workspace
bronze_schema  = sncf_bronze
silver_schema  = sncf_silver
gold_schema    = sncf_gold

landing_path =
/Volumes/workspace/sncf_bronze/landing
```

Cela permet de limiter les valeurs écrites en dur et facilite l'utilisation du pipeline dans différents environnements.

---

# 📁 Structure du repository

```text
sncf-gtfs-data-platform/
│
├── src/
│   ├── 00_pipeline_control
│   ├── 01_bronze_ingestion_gtfs
│   ├── 02_silver_transformations_gtfs
│   ├── 03_sncf_gold
│   ├── 04_quality_optimization
│   └── 06_mark_success
│
├── resources/
│   └── sncf_gtfs_job.yml
│
├── databricks.yml
├── README.md
└── .gitignore
```

---

# 🛠️ Technologies

| Technologie | Utilisation |
|---|---|
| Databricks | Plateforme Data |
| Apache Spark | Traitement distribué |
| PySpark | Transformations |
| Delta Lake | Stockage des tables |
| Unity Catalog | Gouvernance |
| SQL | Contrôles et analyses |
| Databricks Workflows | Orchestration |
| Git | Versioning |
| GitHub | Repository |
| Databricks Bundles | Définition et déploiement du Job |

---

# 🚀 Installation et exécution

## 1. Cloner le repository

```bash
git clone <URL_DU_REPOSITORY>
cd sncf-gtfs-data-platform
```

## 2. Télécharger les données

Télécharger le GTFS depuis la plateforme officielle SNCF :

➡️ **https://ressources.data.sncf.com/explore/dataset/horaires-sncf/**

Extraire ensuite les fichiers GTFS.

---

## 3. Créer la Landing Zone

Créer un Unity Catalog Volume et y déposer les fichiers :

```text
agency.txt
calendar_dates.txt
feed_info.txt
routes.txt
stop_times.txt
stops.txt
trips.txt
transfers.txt
```

Exemple :

```text
/Volumes/<catalog>/<schema>/landing/
```

---

## 4. Configurer les paramètres

Adapter :

```text
catalog
bronze_schema
silver_schema
gold_schema
landing_path
```

à l'environnement Databricks utilisé.

---

## 5. Valider le Bundle

```bash
databricks bundle validate -t dev
```

---

## 6. Déployer

```bash
databricks bundle deploy -t dev
```

---

## 7. Exécuter le pipeline

```bash
databricks bundle run -t dev sncf_gtfs_pipeline
```

---

# 🔮 Améliorations futures

Plusieurs évolutions peuvent être ajoutées :

- automatisation de la récupération du GTFS SNCF ;
- ingestion incrémentale avancée ;
- gestion de surrogate keys persistantes ;
- tests automatisés ;
- monitoring ;
- alerting ;
- environnements DEV / PROD ;
- CI/CD avec GitHub Actions ;
- authentification OIDC ;
- dashboard analytique basé sur les tables Gold.

---

# 👤 Auteur

**Karim Oussaidi**

Projet Data Engineering réalisé avec :

**Databricks · PySpark · Apache Spark · Delta Lake · Unity Catalog · SQL · Git/GitHub**

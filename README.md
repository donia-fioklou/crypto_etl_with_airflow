# Crypto ETL Pipeline with Apache Airflow

Pipeline ETL automatisé pour collecter, transformer et stocker les données de cryptomonnaies en temps réel.

## 📊 Architecture

```
CoinGecko API → Extract → Transform → PostgreSQL
                  ↓         ↓            ↓
              Top 20    Volatilité   crypto_prices
              cryptos   Catégories      table
```

## 🚀 Fonctionnalités

- **Extract**: Récupération des top 20 cryptos via l'API CoinGecko (gratuite, sans auth)
- **Transform**: 
  - Calcul de la volatilité sur 24h
  - Catégorisation par market cap (Large/Mid/Small Cap)
  - Nettoyage et enrichissement des données
- **Load**: Insertion dans PostgreSQL avec indexation
- **Scheduler**: Exécution automatique toutes les 5 minutes

## 🛠️ Stack Technique

- **Apache Airflow 3.1.6** (orchestration)
- **PostgreSQL** (stockage)
- **Docker Compose** (déploiement)
- **Python 3.12**
- **CoinGecko API** (source de données)

## 📋 Prérequis

- Docker Desktop installé
- 8 GB RAM minimum
- Connexion internet (pour l'API CoinGecko)

## 🔧 Installation

### 1. Cloner le projet

```bash
git clone https://github.com/donia-fioklou/crypto_etl_with_airflow.git
cd airflow-docker
```

### 2. Démarrer l'environnement

```bash
docker-compose up -d
```

Airflow sera accessible sur **http://localhost:8080**
- Username: `airflow`
- Password: `airflow`

### 3. Configurer la connexion PostgreSQL

Dans l'UI Airflow, aller dans **Admin → Connections** et créer :

- **Connection ID**: `crypto_connection`
- **Connection Type**: `Postgres`
- **Host**: `postgres`
- **Schema**: `airflow`
- **Login**: `airflow`
- **Password**: `airflow`
- **Port**: `5432`

### 4. Activer le DAG

Dans l'UI Airflow, activer le DAG `crypto_etl_pipeline` et déclencher manuellement la première exécution.

## 📁 Structure du Projet

```
airflow-docker/
├── dags/
│   └── crypto_etl_dag.py        # Pipeline ETL principal
├── logs/                         # Logs Airflow
├── plugins/                      # Plugins custom (vide)
├── config/
│   └── airflow.cfg              # Configuration Airflow
├── docker-compose.yaml          # Orchestration des services
├── requirements.txt             # Dépendances Python
└── README.md
```

## 📊 Schéma de Base de Données

**Table: `crypto_prices`**

| Colonne | Type | Description |
|---------|------|-------------|
| id | SERIAL | Clé primaire auto-incrémentée |
| symbol | VARCHAR(10) | Symbole crypto (BTC, ETH...) |
| name | VARCHAR(100) | Nom complet |
| current_price | NUMERIC(20,2) | Prix actuel en USD |
| high_24h | NUMERIC(20,2) | Plus haut sur 24h |
| low_24h | NUMERIC(20,2) | Plus bas sur 24h |
| market_cap | BIGINT | Capitalisation boursière |
| market_cap_rank | INT | Rang par market cap |
| volume_24h | BIGINT | Volume échangé sur 24h |
| price_change_24h_pct | NUMERIC(10,4) | Variation prix 24h (%) |
| volatility_24h_pct | NUMERIC(10,2) | Volatilité calculée (%) |
| category | VARCHAR(20) | Large/Mid/Small Cap |
| extracted_at | TIMESTAMP | Date d'extraction |

**Indexes:**
- `idx_crypto_symbol` sur `symbol`
- `idx_crypto_extracted_at` sur `extracted_at`

## 🔍 Requêtes Utiles

### Voir les dernières données

```sql
SELECT symbol, name, current_price, volatility_24h_pct, category, extracted_at
FROM crypto_prices
ORDER BY extracted_at DESC
LIMIT 10;
```

### Cryptos les plus volatiles

```sql
SELECT symbol, name, volatility_24h_pct, current_price
FROM crypto_prices
WHERE extracted_at > NOW() - INTERVAL '1 hour'
ORDER BY volatility_24h_pct DESC
LIMIT 5;
```

### Évolution d'une crypto

```sql
SELECT extracted_at, current_price, volatility_24h_pct
FROM crypto_prices
WHERE symbol = 'BTC'
ORDER BY extracted_at DESC
LIMIT 20;
```

## ⚙️ Configuration

### Modifier la fréquence d'exécution

Dans [crypto_etl_dag.py](dags/crypto_etl_dag.py) :

```python
schedule=timedelta(minutes=5),  # Changer ici (ex: hours=1)
```

### Nombre de cryptos à récupérer

Dans la fonction `extract_crypto_data()` :

```python
params = {
    "per_page": 20,  # Changer ici (max 250)
    ...
}
```

## 🐛 Troubleshooting

### Le DAG ne s'exécute pas

- Vérifier que la connexion `crypto_connection` existe
- Regarder les logs : `docker-compose logs -f airflow-webserver`

### Erreur API CoinGecko

- Limite de taux : 10-50 requêtes/minute (gratuit)
- Vérifier la connexion internet

### Erreur PostgreSQL

- Vérifier que le service est up : `docker-compose ps`
- Tester la connexion dans l'UI Airflow

## 📚 Ressources

- [Documentation Airflow](https://airflow.apache.org/docs/)
- [API CoinGecko](https://www.coingecko.com/en/api/documentation)
- [PostgreSQL Hooks](https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/connections/postgres.html)



"""
Crypto ETL Pipeline - Récupère et stocke les données crypto depuis CoinGecko API
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests


def extract_crypto_data(ti):
    """Récupère les top 20 cryptos depuis l'API CoinGecko."""
    print("Extraction des données crypto...")
    
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 20,
        "page": 1,
        "sparkline": False,
        "price_change_percentage": "24h"
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        print(f"{len(data)} cryptos récupérées")
        ti.xcom_push(key='raw_crypto_data', value=data)
        
    except requests.exceptions.RequestException as e:
        print(f"Erreur API: {e}")
        raise


def transform_crypto_data(ti):
    """Nettoie et enrichit les données (calcul volatilité, catégorisation)."""
    print("Transformation des données...")
    
    raw_data = ti.xcom_pull(key='raw_crypto_data', task_ids='extract_crypto_data')
    if not raw_data:
        raise ValueError("Aucune donnée reçue de l'extraction")
    
    transformed_data = []
    
    for coin in raw_data:
        current_price = coin.get('current_price') or 0
        high_24h = coin.get('high_24h') or current_price
        low_24h = coin.get('low_24h') or current_price
        market_cap = coin.get('market_cap') or 0
        
        # Calcul volatilité
        if current_price > 0:
            volatility_pct = ((high_24h - low_24h) / current_price) * 100
        else:
            volatility_pct = 0
        
        # Catégorisation par market cap
        if market_cap > 100_000_000_000:
            category = "Large Cap"
        elif market_cap > 10_000_000_000:
            category = "Mid Cap"
        else:
            category = "Small Cap"
        
        transformed_coin = {
            "symbol": coin['symbol'].upper(),
            "name": coin['name'],
            "current_price": current_price,
            "high_24h": high_24h,
            "low_24h": low_24h,
            "market_cap": market_cap,
            "market_cap_rank": coin.get('market_cap_rank') or 999,
            "volume_24h": coin.get('total_volume') or 0,
            "price_change_24h_pct": coin.get('price_change_percentage_24h') or 0,
            "volatility_24h_pct": round(volatility_pct, 2),
            "category": category,
        }
        transformed_data.append(transformed_coin)
    
    print(f"{len(transformed_data)} cryptos transformées")
    ti.xcom_push(key='transformed_crypto_data', value=transformed_data)


def load_crypto_data(ti):
    """Insert les données dans PostgreSQL."""
    print("Chargement dans PostgreSQL...")
    
    crypto_data = ti.xcom_pull(key='transformed_crypto_data', task_ids='transform_crypto_data')
    if not crypto_data:
        raise ValueError("Aucune donnée transformée à charger")
    
    postgres_hook = PostgresHook(postgres_conn_id='crypto_connection')
    
    insert_query = """
        INSERT INTO crypto_prices (
            symbol, name, current_price, high_24h, low_24h,
            market_cap, market_cap_rank, volume_24h,
            price_change_24h_pct, volatility_24h_pct, category
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    inserted_count = 0
    for coin in crypto_data:
        try:
            postgres_hook.run(
                insert_query,
                parameters=(
                    coin['symbol'],
                    coin['name'],
                    coin['current_price'],
                    coin['high_24h'],
                    coin['low_24h'],
                    coin['market_cap'],
                    coin['market_cap_rank'],
                    coin['volume_24h'],
                    coin['price_change_24h_pct'],
                    coin['volatility_24h_pct'],
                    coin['category']
                )
            )
            inserted_count += 1
        except Exception as e:
            print(f"Erreur insertion {coin['symbol']}: {e}")
    
    print(f"{inserted_count}/{len(crypto_data)} cryptos insérées")


# ============================================================
# CONFIGURATION DU DAG
# ============================================================
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 16),  # Date d'aujourd'hui
    'retries': 2,  # Réessaie 2 fois en cas d'échec
    'retry_delay': timedelta(minutes=2),  # Attend 2 min entre chaque retry
}

# Création du DAG
dag = DAG(
    'crypto_etl_pipeline',
    default_args=default_args,
    description='ETL Pipeline pour récupérer et analyser les données crypto',
    schedule=timedelta(minutes=5),  # Exécute toutes les 5 minutes
    catchup=False,  # Ne fait PAS de backfill (n'exécute pas les runs passés)
    tags=['crypto', 'etl', 'coingecko'],  # Tags pour filtrer dans l'UI
)


# ============================================================
# DÉFINITION DES TÂCHES
# ============================================================

# Tâche 1 : Extract
extract_task = PythonOperator(
    task_id='extract_crypto_data',
    python_callable=extract_crypto_data,
    dag=dag,
)

# Tâche 2 : Transform
transform_task = PythonOperator(
    task_id='transform_crypto_data',
    python_callable=transform_crypto_data,
    dag=dag,
)

# Tâche 3 : Create Table (SQL)
create_table_task = SQLExecuteQueryOperator(
    task_id='create_crypto_table',
    conn_id='crypto_connection',
    sql="""
        CREATE TABLE IF NOT EXISTS crypto_prices (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(10) NOT NULL,
            name VARCHAR(100) NOT NULL,
            current_price NUMERIC(20, 2) NOT NULL,
            high_24h NUMERIC(20, 2),
            low_24h NUMERIC(20, 2),
            market_cap BIGINT,
            market_cap_rank INT,
            volume_24h BIGINT,
            price_change_24h_pct NUMERIC(10, 4),
            volatility_24h_pct NUMERIC(10, 2),
            category VARCHAR(20),
            extracted_at TIMESTAMP DEFAULT NOW()
        );
        
        -- Crée un index pour accélérer les requêtes par symbol
        CREATE INDEX IF NOT EXISTS idx_crypto_symbol ON crypto_prices(symbol);
        CREATE INDEX IF NOT EXISTS idx_crypto_extracted_at ON crypto_prices(extracted_at);
    """,
    dag=dag,
)

# Tâche 4 : Load
load_task = PythonOperator(
    task_id='load_crypto_data',
    python_callable=load_crypto_data,
    dag=dag,
)


# ============================================================
# DÉPENDANCES (ordre d'exécution)
# ============================================================

extract_task >> transform_task >> create_table_task >> load_task


#dag - directed acyclic graph

#tasks : 1) fetch amazon data (extract) 2) clean data (transform) 3) create and store data in table on postgres (load)
#operators : Python Operator and PostgresOperator
#hooks - allows connection to postgres
#dependencies

from datetime import datetime, timedelta
from airflow import DAG
import requests
import pandas as pd
from bs4 import BeautifulSoup
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

#1) fetch amazon data (extract) 2) clean data (transform)

headers = {
    "Referer": 'https://www.amazon.com/',
    "Sec-Ch-Ua": "Not_A Brand",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "macOS",
    'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
}


def get_amazon_data_books(num_books, ti):
    # Base URL of the Amazon search results for data science books
    base_url = f"https://www.amazon.com/s?k=data+engineering+books"

    books = []
    seen_titles = set()  # To keep track of seen titles

    page = 1

    while len(books) < num_books:
        url = f"{base_url}&page={page}"
        
        # Send a request to the URL
        response = requests.get(url, headers=headers)
        
        # Check if the request was successful
        if response.status_code == 200:
            # Parse the content of the request with BeautifulSoup
            soup = BeautifulSoup(response.content, "html.parser")
            
            # Find book containers (you may need to adjust the class names based on the actual HTML structure)
            book_containers = soup.find_all("div", {"class": "s-result-item"})
            
            # Loop through the book containers and extract data
            for book in book_containers:
                # DEBUG CIBLÉ : Qu'est-ce qui est trouvé dans le conteneur ?
                all_h2 = book.find_all("h2")
                print(f"DEBUG - Nombre de h2 trouvés: {len(all_h2)}")
                if all_h2:
                    print(f"DEBUG - Classes du 1er h2: {all_h2[0].get('class')}")
                
                # TITRE : Cherche le h2, puis extrait le span enfant pour avoir le texte propre
                title_h2 = book.find("h2", {"class": "a-size-medium"})
                if title_h2:
                    print(f"DEBUG - h2 trouvé, contenu: {title_h2.text[:50]}")
                else:
                    print(f"DEBUG - h2 avec a-size-medium PAS trouvé")
                title = title_h2.find("span") if title_h2 else None
                
                # AUTEUR : Cherche dans la section métadonnées (div avec author info)
                author = None
                author_row = book.find("div", {"class": "a-row a-size-base a-color-secondary"})
                if author_row:
                    spans = author_row.find_all("span", {"class": "a-size-base"})
                    # Le 1er span contient "by", le 2ème contient l'auteur réel
                    if len(spans) > 1:
                        author = spans[1]
                
                # PRIX : Le prix entier (sans les centimes)
                price = book.find("span", {"class": "a-price-whole"})
                
                # RATING : Optionnel, tous les livres n'en ont pas
                rating = book.find("span", {"class": "a-icon-alt"})
                
                # DEBUG: Affiche ce qui est trouvé
                print(f"DEBUG - Title: {title is not None}, Author: {author is not None}, Price: {price is not None}, Rating: {rating is not None}")
                
                # On exige titre, auteur et prix. Le rating est optionnel.
                if title and author and price:
                    book_title = title.text.strip()
                    
                    # Check if title has been seen before
                    if book_title not in seen_titles:
                        seen_titles.add(book_title)
                        books.append({
                            "Title": book_title,
                            "Author": author.text.strip(),
                            "Price": price.text.strip(),
                            # Si pas de rating, on met "N/A"
                            "Rating": rating.text.strip() if rating else "N/A",
                        })
            
            # Increment the page number for the next iteration
            page += 1
        else:
            print("Failed to retrieve the page")
            break

    # Limit to the requested number of books
    books = books[:num_books]
    
    # Convert the list of dictionaries into a DataFrame
    df = pd.DataFrame(books)
    
    # Remove duplicates based on 'Title' column
    df.drop_duplicates(subset="Title", inplace=True)
    
    # Push the DataFrame to XCom
    ti.xcom_push(key='book_data', value=df.to_dict('records'))

#3) create and store data in table on postgres (load)
    
def insert_book_data_into_postgres(ti):
    book_data = ti.xcom_pull(key='book_data', task_ids='fetch_book_data')
    if not book_data:
        raise ValueError("No book data found")

    postgres_hook = PostgresHook(postgres_conn_id='books_connection')
    insert_query = """
    INSERT INTO books (title, authors, price, rating)
    VALUES (%s, %s, %s, %s)
    """
    for book in book_data:
        postgres_hook.run(insert_query, parameters=(book['Title'], book['Author'], book['Price'], book['Rating']))


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fetch_and_store_amazon_books',
    default_args=default_args,
    description='A simple DAG to fetch book data from Amazon and store it in Postgres',
    schedule=timedelta(days=1),
)

#operators : Python Operator and PostgresOperator
#hooks - allows connection to postgres


fetch_book_data_task = PythonOperator(
    task_id='fetch_book_data',
    python_callable=get_amazon_data_books,
    op_args=[5],  # Number of books to fetch
    dag=dag,
)

create_table_task = SQLExecuteQueryOperator(
    task_id='create_table',
    conn_id='books_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS books (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        authors TEXT,
        price TEXT,
        rating TEXT
    );
    """,
    dag=dag,
)

insert_book_data_task = PythonOperator(
    task_id='insert_book_data',
    python_callable=insert_book_data_into_postgres,
    dag=dag,
)

#dependencies

fetch_book_data_task >> create_table_task >> insert_book_data_task

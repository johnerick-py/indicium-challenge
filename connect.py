import psycopg2

conn = psycopg2.connect(
    host="localhost", database="northwind", user="postgres", password="123"
)
cur = conn.cursor()
conn.commit()

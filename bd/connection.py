import psycopg2
from config import *

try:
    connection = psycopg2.connect(
        user=USER,
        password=PASSWORD,
        host="127.0.0.1",
        port=PORT,
        database = DBNAME
    )

    with connection.cursor() as cursor:
        cursor.execute("""CREATE TABLE IF NOT EXISTS companys(
        id SERIAL PRIMARY KEY NOT NULL,
        inn VARCHAR(100) NOT NULL,
        kpp VARCHAR(100) NOT NULL,
        name VARCHAR(300) NOT NULL,
        full_name VARCHAR(300) NOT NULL,
        okved VARCHAR(100) NOT NULL,
        adress VARCHAR(300) NOT NULL
        )
        """)

        connection.commit()
except Exception as e:
    print(f"{e} Error while work with mysql")
# finally:
#     if connection:
#         connection.close()


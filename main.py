import psycopg2


def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
		    CREATE TABLE IF NOT EXISTS clients(
		    id SERIAL PRIMARY KEY,
		    firstname VARCHAR(20),
		    lastname VARCHAR(20),
		    email VARCHAR(30)  UNIQUE);
		    """)
        cur.execute("""
		    CREATE TABLE IF NOT EXISTS phones(
		    id SERIAL PRIMARY KEY,
		    client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
		    number VARCHAR(20) NOT NULL UNIQUE);
		    """)
    conn.commit()


def delete_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
		DROP TABLE IF EXISTS clients, phones CASCADE;
		""")
    conn.commit()


def add_client(conn, first_name, last_name, email, number=None):
    with conn.cursor() as cur:
        cur.execute("""
		INSERT INTO clients(firstname, lastname, email)
		VALUES(%s, %s, %s)
		RETURNING id, firstname, lastname;
		""", (first_name, last_name, email))
        new_client = cur.fetchone()
    if number is not None:
        cur.execute("""
                INSERT INTO phones(client_id, number)
                VALUES(%s, %s) RETURNING client_id;
                """, (new_client[0], number))
        cur.fetchone()
    print(f'Добавили клиента {new_client}')



def add_phone(conn, client_id, number):
    with conn.cursor() as cur:
        cur.execute("""
		INSERT INTO phones(number, client_id)
		VALUES (%s, %s)
		""", (number, client_id))
    conn.commit()


def change_client(conn, client_id, first_name=None, last_name=None, email=None):
    if first_name is not None:
        with conn.cursor() as cur:
            cur.execute("""
			UPDATE clients SET firstname=%s
			WHERE id = %s
			""", (first_name, client_id))
    if last_name is None:
        with conn.cursor() as cur:
            cur.execute("""
			UPDATE clients SET lastname = %s WHERE id = %s
			""", (last_name, client_id))
    if email is None:
        with conn.cursor() as cur:
            cur.execute("""
			UPDATE clients SET email = %s WHERE id = %s
			""", (email, client_id))
            cur.execute("""
			SELECT * FROM clients;
			""")
    conn.commit()


def delete_phone(conn, client_id, number):
    with conn.cursor() as cur:
        cur.execute("""
		DELETE FROM phones
		WHERE client_id = %s and number = %s
		RETURNING *;
		""", (client_id, number))
    conn.commit()


def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""
		DELETE FROM clients
		WHERE id = %s;
		""", (client_id,))
    conn.commit()



# def find_client(conn, first_name='%', last_name='%', email='%', number='%'):
#     with conn.cursor() as curs:
#         find_client = (f"""
#              SELECT email,
#                     firsrname,
#                     lastname,
#                     CASE
#                         WHEN ARRAY_AGG() = '{{Null}}' THEN '{{}}'
#                         ESLE ARRAY_AGG(number)
#                     END phones p
#              FROM clients c
#              LEFT JOIN phones p ON p.client_id = c.id
#              WHERE firstname ILIKE %s AND lastname ILIKE %s AND email ILAKE %s AND number ILIKE %s
#              GROUP BY email, firstname, lastname
#              """)
#         curs.execute("""
#             SELECT * FROM clients c
#             FULL JOIN phones p ON p.client_id = c.id
#             WHERE firstname=%s, lastname=%s, email=%s, number=%s;""", (first_name, last_name, email, number))
#         return curs.fetchall()


def find_client(cur, first_name=None, last_name=None, email=None, number=None):
    with conn.cursor() as cur:
        if number is not None:
            cur.execute("""
                   SELECT c.id, c.firstname, c.lastname, c.email, p.number FROM clients c
                   LEFT JOIN phones p ON c.id = p.client_id
                   WHERE c.firstname LIKE %s AND c.lastname LIKE %s
                   AND c.email LIKE %s
                   """, (first_name, last_name, email))
        else:
            cur.execute("""
                   SELECT c.id, c.firstname, c.lastname, c.email, p.number FROM clients c
                   LEFT JOIN phones p ON c.id = p.client_id
                   WHERE c.firstname LIKE %s AND c.lastname LIKE %s
                   AND c.email LIKE %s AND p.number like %s
                   """, (first_name, last_name, email, number))
        return cur.fetchall()


if __name__ == '__main__':
    with psycopg2.connect(database="test2", user="test", password="test") as conn:
        # 1. Удаление таблиц перед запуском
        delete_db(conn)
        print("БД удалена")
        #  Cоздание таблиц
        create_db(conn)
        print("БД создана")
        # 2. Добавляем 5 клиентов
        print("Добавлен клиент id: ",
            add_client(conn, "Игорь", "Поляков", "234as@gmail.com"))
        print("Добавлен клиент id: ",
            add_client(conn, "Сергей", "Королев", "uuuyy12@mail.ru"))
        print("Добавлен клиент id: ",
            add_client(conn, "Александр", "Гайдук", "666@yandex.ru"))
        print("Добавлен клиент id: ",
            add_client(conn, "Владимир", "Бутнев", "999aaa@mail.ru"))
        print("Добавлена клиент id: ",
            add_client(conn, "Николай", "Левковец", "467bb@outlook.com"))
        print("Данные в таблицах")
        # print (get_phone(conn, 1, 77777777777))
        print ("Телефон добавлен клиенту id: ",
              add_phone(conn, 1, 11111111111))
        print("Телефон добавлен клиенту id: ",
              add_phone(conn, 2, 22222222222))
        print("Телефон добавлен клиенту id: ",
              add_phone(conn, 3, 3333333333333))
        print("Телефон добавлен клиенту id: ",
              add_phone(conn, 4, 44444444444))
        print("Телефон добавлен клиенту id: ",
              add_phone(conn, 5, 555555555555))
        print("Данные в таблицах")
    #     # 4. Изменим данные клиента(имя, фамилию и email)
    print("Изменены данные клиента id: ",
              change_client(conn, 1, first_name='Семен'),
              change_client(conn, 2, last_name='Иванин'),
              change_client(conn, 3, email='789sss@gmail.com'))
        # 5. Удаляем клиенту номер телефона
    print("Телефон удалён c номером: ",
              delete_phone(conn, '3', '71111155522'))
    print("Данные в таблицах")
        # 6. Удалим клиента номер 5
    print("Клиент удалён с id: ",
              delete_client(conn, 5))
        # 7. Найдём клиента по его данным (имени, фамилии, email и телефону)
    print("Найденный клиент по имени: ",
              find_client(conn, first_name='Сергей'))
    print("Найденный клиент по фамилии:",
              find_client(conn, last_name='Королев'))
    print("Найденный клиент по email:",
              find_client(conn, email="uuuyy12@mail.ru"))
    print("Найденный клиент по телефону:",
              find_client(conn, number='79797979711'))

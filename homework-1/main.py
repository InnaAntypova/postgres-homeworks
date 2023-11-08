"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv
import psycopg2


def write_customers_data():
    """ Функция для записи customers_data.csv в БД"""
    try:
        with open('../homework-1/north_data/customers_data.csv', 'r', encoding='utf-8') as file:
            # encoding нужен для корректного отображения всех букв
            data = list(csv.reader(file))

            for i in data[1:]:  # в data[0] лежат названия колонок
                try:
                    with psycopg2.connect(host='localhost', database='north', user='postgres', password='12345') as conn:
                        with conn.cursor() as cur:
                            cur.execute("INSERT INTO customers_data VALUES (%s, %s, %s)", i)

                except TypeError:
                    print("Ошибка записи в таблицу customers_data")
                finally:
                    conn.close()
    except FileNotFoundError:
        print("Файл не найден")


def write_employees_data():
    """ Функция для записи employees_data.csv в БД"""
    try:
        with open('../homework-1/north_data/employees_data.csv', 'r', encoding='utf-8') as file:
            # encoding нужен для корректного отображения всех букв
            data = list(csv.reader(file))

            for i in data[1:]:  # в data[0] лежат названия колонок
                try:
                    with psycopg2.connect(host='localhost', database='north', user='postgres',
                                          password='12345') as conn:
                        with conn.cursor() as cur:
                            cur.execute("INSERT INTO employees_data VALUES (%s, %s, %s, %s, %s, %s)", i)

                except TypeError:
                    print("Ошибка записи в таблицу customers_data")
                finally:
                    conn.close()
    except FileNotFoundError:
        print("Файл не найден")


def write_orders_data():
    """ Функция для записи orders_data.csv в БД"""
    try:
        with open('../homework-1/north_data/orders_data.csv', 'r', encoding='utf-8') as file:
            # encoding нужен для корректного отображения всех букв
            data = list(csv.reader(file))

            for i in data[1:]:  # в data[0] лежат названия колонок
                try:
                    with psycopg2.connect(host='localhost', database='north', user='postgres',
                                          password='12345') as conn:
                        with conn.cursor() as cur:
                            cur.execute("INSERT INTO orders_data VALUES (%s, %s, %s, %s, %s)", i)

                except TypeError:
                    print("Ошибка записи в таблицу customers_data")
                finally:
                    conn.close()
    except FileNotFoundError:
        print("Файл не найден")


if __name__ == '__main__':
    write_customers_data()
    write_employees_data()
    write_orders_data()

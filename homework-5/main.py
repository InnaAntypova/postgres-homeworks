import json

import psycopg2

from config import config


def main():
    script_file = 'fill_db.sql'
    json_file = 'suppliers.json'
    db_name = 'my_new_db'

    params = config()
    conn = None

    create_database(params, db_name)
    print(f"БД {db_name} успешно создана")

    params.update({'dbname': db_name})
    try:
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                execute_sql_script(cur, script_file)
                print(f"БД {db_name} успешно заполнена")

                create_suppliers_table(cur)
                print("Таблица suppliers успешно создана")

                suppliers = get_suppliers_data(json_file)
                insert_suppliers_data(cur, suppliers)
                print("Данные в suppliers успешно добавлены")

                add_foreign_keys(cur, json_file)
                print(f"FOREIGN KEY успешно добавлены")

    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def create_database(params, db_name) -> None:
    """Создает новую базу данных."""
    conn = psycopg2.connect(**params)
    cursor = conn.cursor()
    conn.autocommit = True
    # удалим базу, если была создана ранее
    cursor.execute(f'DROP DATABASE IF EXISTS {db_name}')
    cursor.execute(f'CREATE DATABASE {db_name}')
    cursor.close()
    conn.close()


def execute_sql_script(cur, script_file) -> None:
    """Выполняет скрипт из файла для заполнения БД данными."""
    with open(script_file, 'r', encoding='utf-8') as file:
        data = file.read()
    # передаем данные из sql файла
        cur.execute(data)


def create_suppliers_table(cur) -> None:
    """Создает таблицу suppliers."""
    cur.execute("CREATE TABLE suppliers (supplier_id int ,"
                                        "company_name varchar(100), "
                                        "contact varchar(100), "
                                        "address varchar(100), "
                                        "phone varchar(30), "
                                        "fax varchar(30), "
                                        "homepage varchar)")


def get_suppliers_data(json_file: str) -> list[dict]:
    """Извлекает данные о поставщиках из JSON-файла и возвращает список словарей с соответствующей информацией."""
    with open(json_file, 'r', encoding='utf-8') as file:
        data = json.load(file)
        return data


def insert_suppliers_data(cur, suppliers: list[dict]) -> None:
    """Добавляет данные из suppliers в таблицу suppliers."""
    count = 0
    for i in suppliers:
        company_name = i['company_name']
        contact = i['contact']
        address = i['address']
        phone = i['phone']
        fax = i['fax']
        homepage = i['homepage']
        #products = i['products']
        count += 1
        supplier_id = count
        cur.execute("INSERT INTO suppliers VALUES (%s, %s, %s, %s, %s, %s, %s)", (supplier_id, company_name, contact,
                                                                                  address, phone, fax, homepage))



def add_foreign_keys(cur, json_file) -> None:
    """Добавляет foreign key со ссылкой на supplier_id в таблицу products."""
    cur.execute("ALTER TABLE products ADD COLUMN supplier_id int;")

    cur.execute("SELECT product_id, product_name FROM products")
    products = cur.fetchall()
    #print(products)

    with open(json_file, 'r', encoding='utf-8') as file:
        data = json.load(file)
    #print(data)

    for item in data:
        #print(item)
        for product in products:
            #print(product)
            if product[1] in item['products']:
                product_id = product[0]
                company_name = item['company_name']
                #print(company_name)
                cur.execute("SELECT supplier_id FROM suppliers WHERE company_name = %s", (company_name,))
                supplier = cur.fetchall()
                #print(supplier)
                cur.execute("UPDATE products SET supplier_id = %s WHERE product_id = %s", (supplier[0], product_id))

    cur.execute("ALTER TABLE suppliers ADD CONSTRAINT pk_suppliers PRIMARY KEY (supplier_id);"
                "ALTER TABLE products ADD CONSTRAINT fk_products_supplier_id FOREIGN KEY (supplier_id) "
                "REFERENCES suppliers (supplier_id);")

if __name__ == '__main__':
    main()

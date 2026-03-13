from pathlib import Path
import psycopg2


def get_connection():
    return psycopg2.connect(
        dbname="retail_project",
        user="anna"
    )


def load_sales_report(records):
    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute("DELETE FROM sales_report;")

    insert_query = """
        INSERT INTO sales_report (
            order_id,
            order_date,
            customer_id,
            customer_name,
            city,
            product_id,
            product_name,
            category,
            quantity,
            price,
            total_amount
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    for record in records:
        cursor.execute(
            insert_query,
            (
                record["order_id"],
                record["order_date"],
                record["customer_id"],
                record["customer_name"],
                record["city"],
                record["product_id"],
                record["product_name"],
                record["category"],
                record["quantity"],
                record["price"],
                record["total_amount"]
            )
        )

    connection.commit()
    cursor.close()
    connection.close()


def load_raw_customers(customers):
    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute("DELETE FROM raw_customers;")

    insert_query = """
        INSERT INTO raw_customers (
            customer_id,
            customer_name,
            city,
            signup_date
        )
        VALUES (%s, %s, %s, %s);
    """

    for customer in customers:
        cursor.execute(
            insert_query,
            (
                customer.get("customer_id"),
                customer.get("customer_name"),
                customer.get("city"),
                customer.get("signup_date")
            )
        )

    connection.commit()
    cursor.close()
    connection.close()


def load_raw_orders(orders):
    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute("DELETE FROM raw_orders;")

    insert_query = """
        INSERT INTO raw_orders (
            order_id,
            customer_id,
            product_id,
            quantity,
            order_date
        )
        VALUES (%s, %s, %s, %s, %s);
    """

    for order in orders:
        cursor.execute(
            insert_query,
            (
                order.get("order_id"),
                order.get("customer_id"),
                order.get("product_id"),
                order.get("quantity"),
                order.get("order_date")
            )
        )

    connection.commit()
    cursor.close()
    connection.close()


def load_raw_products(products):
    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute("DELETE FROM raw_products;")

    insert_query = """
        INSERT INTO raw_products (
            product_id,
            product_name,
            category,
            price
        )
        VALUES (%s, %s, %s, %s);
    """

    for product in products:
        cursor.execute(
            insert_query,
            (
                product.get("product_id"),
                product.get("product_name"),
                product.get("category"),
                product.get("price")
            )
        )

    connection.commit()
    cursor.close()
    connection.close()


def run_sql_file(file_path):
    connection = get_connection()
    cursor = connection.cursor()

    sql_path = Path(file_path)
    sql_content = sql_path.read_text(encoding="utf-8")

    cursor.execute(sql_content)

    connection.commit()
    cursor.close()
    connection.close()
import os
import pandas as pd
from products_load import ProductLoader
from categories_load import CategoriesLoader
from customers_load import CustomersLoader
from employees_load import EmployeesLoader
from inventory_load import InventoriesLoader
from order_items_load import OrderItemsLoader
from orders_load import OrdersLoader
from stores_load import StoresLoader

from snowflake_connection import connect_to_snowflake

conn = connect_to_snowflake.get_connection()

try:
    ProductLoader(conn).validate_and_load_products_file()
    CategoriesLoader(conn).validate_and_load_categories_file()
    CustomersLoader(conn).validate_and_load_customers_file()
    EmployeesLoader(conn).validate_and_load_employees_file()
    InventoriesLoader(conn).validate_and_load_inventories_file()
    OrderItemsLoader(conn).validate_and_load_orderitems_file()
    OrdersLoader(conn).validate_and_load_orders_file()
    StoresLoader(conn).validate_and_load_stores_file()
    
except Exception as e:
    print(e)

conn.close()
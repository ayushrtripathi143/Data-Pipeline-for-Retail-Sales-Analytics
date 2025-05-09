create or replace database RETAIL_SALES_DB;
create or replace schema RETAIL_SALES_DB.RETAIL_TARGET;
CREATE OR REPLACE SCHEMA RETAIL_SALES_DB.RETAIL_LANDING;

CREATE OR REPLACE TABLE RETAIL_LANDING.LND_PRODUCTS (
    "product_id" STRING,
    "product_name" STRING,
    "category" STRING,
    "price" FLOAT,
    "quantity" INT,
    "ingest_ts" TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RETAIL_LANDING.LND_CUSTOMERS (
    "customer_id" STRING,
    "first_name" STRING,
    "last_name" STRING,
    "email" STRING,
    "phone" STRING,
    "address" STRING,
    "ingest_ts" TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RETAIL_LANDING.LND_ORDERS (
    "order_id" STRING,
    "customer_id" STRING,
    "order_date" DATE,
    "store_id" STRING,
    "employee_id" STRING,
    "ingest_ts" TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RETAIL_LANDING.LND_ORDER_ITEMS (
    "order_item_id" STRING,
    "order_id" STRING,
    "product_id" STRING,
    "quantity" INT,
    "price" FLOAT,
    "ingest_ts" TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RETAIL_LANDING.LND_CATEGORIES (
    "category_id" STRING,
    "category_name" STRING,
    "ingest_ts" TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RETAIL_LANDING.LND_STORES (
    "store_id" STRING,
    "store_name" STRING,
    "location" STRING,
    "ingest_ts" TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RETAIL_LANDING.LND_EMPLOYEES (
    "employee_id" STRING,
    "first_name" STRING,
    "last_name" STRING,
    "title" STRING,
    "store_id" STRING",
    "ingest_ts" TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RETAIL_LANDING.LND_INVENTORY (
    "inventory_id" STRING,
    "product_id" STRING,
    "store_id" STRING,
    "stock_quantity" INT,
    "last_updated" DATE,
    "ingest_ts" TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);


-- ===================================================
-- CREATE FINAL TABLES
-- ===================================================

-- 1. DIM_STORES
CREATE OR REPLACE TABLE DIM_STORES (
    store_id STRING,
    store_name STRING,
    location STRING,
    added_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- 2. DIM_EMPLOYEES
CREATE OR REPLACE TABLE DIM_EMPLOYEES (
    employee_id STRING,
    first_name STRING,
    last_name STRING,
    title STRING,
    store_id STRING,
    added_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- 3. DIM_CUSTOMERS
CREATE OR REPLACE TABLE DIM_CUSTOMERS (
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone STRING,
    address STRING,
    added_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- 4. DIM_CATEGORIES
CREATE OR REPLACE TABLE DIM_CATEGORIES (
    category_id STRING,
    category_name STRING,
    added_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- 5. DIM_PRODUCTS
CREATE OR REPLACE TABLE DIM_PRODUCTS (
    product_id STRING,
    product_name STRING,
    category STRING,
    price FLOAT,
    quantity INT,
    added_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- 6. DIM_ORDERS
CREATE OR REPLACE TABLE DIM_ORDERS (
    order_id STRING,
    customer_id STRING,
    order_date DATE,
    store_id STRING,
    employee_id STRING,
    added_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- 7. DIM_ORDER_ITEMS
CREATE OR REPLACE TABLE DIM_ORDER_ITEMS (
    order_item_id STRING,
    order_id STRING,
    product_id STRING,
    quantity INT,
    price FLOAT,
    added_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- 8. DIM_INVENTORY
CREATE OR REPLACE TABLE DIM_INVENTORY (
    inventory_id STRING,
    product_id STRING,
    store_id STRING,
    stock_quantity INT,
    last_updated DATE,
    added_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE PROCEDURE RETAIL_TARGET.LOAD_DIM_STORES()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO RETAIL_TARGET.DIM_STORES tgt
    USING (
        SELECT DISTINCT 
            TRIM(store_id) AS store_id,
            INITCAP(TRIM(store_name)) AS store_name,
            INITCAP(TRIM(location)) AS location
        FROM RETAIL_LANDING.LND_STORES
    ) src
    ON tgt.store_id = src.store_id
    WHEN NOT MATCHED THEN
        INSERT (store_id, store_name, location)
        VALUES (src.store_id, src.store_name, src.location);

    RETURN 'Stores Load Complete';
END;
$$;

CREATE OR REPLACE PROCEDURE RETAIL_TARGET.LOAD_DIM_EMPLOYEES()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO RETAIL_TARGET.DIM_EMPLOYEES tgt
    USING (
        SELECT DISTINCT
            TRIM(employee_id) AS employee_id,
            INITCAP(TRIM(first_name)) AS first_name,
            INITCAP(TRIM(last_name)) AS last_name,
            INITCAP(TRIM(title)) AS title,
            TRIM(store_id) AS store_id
        FROM RETAIL_LANDING.LND_EMPLOYEES
    ) src
    ON tgt.employee_id = src.employee_id
    WHEN NOT MATCHED THEN
        INSERT (employee_id, first_name, last_name, title, store_id)
        VALUES (src.employee_id, src.first_name, src.last_name, src.title, src.store_id);

    RETURN 'Employees Load Complete';
END;
$$;

CREATE OR REPLACE PROCEDURE RETAIL_TARGET.LOAD_DIM_CUSTOMERS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO RETAIL_TARGET.DIM_CUSTOMERS tgt
    USING (
        SELECT DISTINCT
            TRIM(customer_id) AS customer_id,
            INITCAP(TRIM(first_name)) AS first_name,
            INITCAP(TRIM(last_name)) AS last_name,
            LOWER(TRIM(email)) AS email,
            TRIM(phone) AS phone,
            INITCAP(TRIM(address)) AS address
        FROM RETAIL_LANDING.LND_CUSTOMERS
    ) src
    ON tgt.customer_id = src.customer_id
    WHEN NOT MATCHED THEN
        INSERT (customer_id, first_name, last_name, email, phone, address)
        VALUES (src.customer_id, src.first_name, src.last_name, src.email, src.phone, src.address);

    RETURN 'Customers Load Complete';
END;
$$;

CREATE OR REPLACE PROCEDURE RETAIL_TARGET.LOAD_DIM_CATEGORIES()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO RETAIL_TARGET.DIM_CATEGORIES tgt
    USING (
        SELECT DISTINCT
            TRIM(category_id) AS category_id,
            INITCAP(TRIM(category_name)) AS category_name
        FROM RETAIL_LANDING.LND_CATEGORIES
    ) src
    ON tgt.category_id = src.category_id
    WHEN NOT MATCHED THEN
        INSERT (category_id, category_name)
        VALUES (src.category_id, src.category_name);

    RETURN 'Categories Load Complete';
END;
$$;

CREATE OR REPLACE PROCEDURE RETAIL_TARGET.LOAD_DIM_PRODUCTS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO RETAIL_TARGET.DIM_PRODUCTS tgt
    USING (
        SELECT DISTINCT
            TRIM(product_id) AS product_id,
            INITCAP(TRIM(product_name)) AS product_name,
            INITCAP(TRIM(category)) AS category,
            price,
            quantity
        FROM RETAIL_LANDING.LND_PRODUCTS
    ) src
    ON tgt.product_id = src.product_id
    WHEN NOT MATCHED THEN
        INSERT (product_id, product_name, category, price, quantity)
        VALUES (src.product_id, src.product_name, src.category, src.price, src.quantity);

    RETURN 'Products Load Complete';
END;
$$;

CREATE OR REPLACE PROCEDURE RETAIL_TARGET.LOAD_DIM_ORDERS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO RETAIL_TARGET.DIM_ORDERS tgt
    USING (
        SELECT DISTINCT
            TRIM(order_id) AS order_id,
            TRIM(customer_id) AS customer_id,
            order_date,
            TRIM(store_id) AS store_id,
            TRIM(employee_id) AS employee_id
        FROM RETAIL_LANDING.LND_ORDERS
    ) src
    ON tgt.order_id = src.order_id
    WHEN NOT MATCHED THEN
        INSERT (order_id, customer_id, order_date, store_id, employee_id)
        VALUES (src.order_id, src.customer_id, src.order_date, src.store_id, src.employee_id);

    RETURN 'Orders Load Complete';
END;
$$;

CREATE OR REPLACE PROCEDURE RETAIL_TARGET.LOAD_DIM_ORDER_ITEMS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO RETAIL_TARGET.DIM_ORDER_ITEMS tgt
    USING (
        SELECT DISTINCT
            TRIM(order_item_id) AS order_item_id,
            TRIM(order_id) AS order_id,
            TRIM(product_id) AS product_id,
            quantity,
            price
        FROM RETAIL_LANDING.LND_ORDER_ITEMS
    ) src
    ON tgt.order_item_id = src.order_item_id
    WHEN NOT MATCHED THEN
        INSERT (order_item_id, order_id, product_id, quantity, price)
        VALUES (src.order_item_id, src.order_id, src.product_id, src.quantity, src.price);

    RETURN 'Order Items Load Complete';
END;
$$;

CREATE OR REPLACE PROCEDURE RETAIL_TARGET.LOAD_DIM_INVENTORY()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO RETAIL_TARGET.DIM_INVENTORY tgt
    USING (
        SELECT DISTINCT
            TRIM(inventory_id) AS inventory_id,
            TRIM(product_id) AS product_id,
            TRIM(store_id) AS store_id,
            stock_quantity,
            last_updated
        FROM RETAIL_LANDING.LND_INVENTORY
    ) src
    ON tgt.inventory_id = src.inventory_id
    WHEN NOT MATCHED THEN
        INSERT (inventory_id, product_id, store_id, stock_quantity, last_updated)
        VALUES (src.inventory_id, src.product_id, src.store_id, src.stock_quantity, src.last_updated);

    RETURN 'Inventory Load Complete';
END;
$$;

-- Create a master task to orchestrate the loading of all dimension data

CREATE OR REPLACE TASK RETAIL_TASK.LOAD_ALL_DIM_DATA_TASK
  WAREHOUSE = COMPUTE_VW
  SCHEDULE = 'USING CRON 0 0 * * * UTC'  -- Example schedule: Every day at midnight UTC
  COMMENT = 'Master task to load all DIM tables'
AS
BEGIN
    -- Step 1: Load stores data
    CALL RETAIL_TARGET.LOAD_DIM_STORES();

    -- Step 2: Load employees data
    CALL RETAIL_TARGET.LOAD_DIM_EMPLOYEES();

    -- Step 3: Load customers data
    CALL RETAIL_TARGET.LOAD_DIM_CUSTOMERS();

    -- Step 4: Load categories data
    CALL RETAIL_TARGET.LOAD_DIM_CATEGORIES();

    -- Step 5: Load products data
    CALL RETAIL_TARGET.LOAD_DIM_PRODUCTS();

    -- Step 6: Load orders data
    CALL RETAIL_TARGET.LOAD_DIM_ORDERS();

    -- Step 7: Load order items data
    CALL RETAIL_TARGET.LOAD_DIM_ORDER_ITEMS();

    -- Step 8: Load inventory data
    CALL RETAIL_TARGET.LOAD_DIM_INVENTORY();

    RETURN 'All DIM Data Loads Completed';
END;

execute task LOAD_ALL_DIM_DATA_TASK;

CREATE OR REPLACE TASK LOAD_SALES_ANALYTICS_TASK
  WAREHOUSE = COMPUTE_VW
  --SCHEDULE = 'USING CRON 0 * * * * UTC'  -- every hour
AS
INSERT INTO SALES_ANALYTICS
select 
o.order_id as order_id,
o.order_date as order_date,
to_char(o.order_date, 'MON') as month_ordered,
to_char(o.order_date, 'YYYY') as yyyy_ordered,
day(o.order_date) as day_order,
TO_CHAR(order_date, 'Dy') AS day_of_week,
--c.customer_id as customer_id,
c.first_name||' '||c.last_name as customer_name,
c.email as email,
c.phone as phone,
c.address as address,
substr(address, -8, 2) as cust_area_code,
al.STATE_NAME as state_name,
nvl(al.latitude, 0) as latitude,
nvl(al.longitude, 0) as longitude,
--p.product_id,
p.product_name as product_name,
p.category as category,
p.price as price,
oi.quantity as quantity,
--s.store_id,
s.store_name as store_name,
s.location as store_location,
--e.employee_id,
e.first_name||' '||e.last_name as employee_name,
e.title as employee_title,
oi.quantity * oi.price as ORDER_ITEM_TOTAL
from
(((((dim_orders o inner join dim_customers c on o.customer_id=c.customer_id )
inner join dim_order_items oi on o.order_id = oi.order_id)
inner join dim_products p on oi.product_id = p.product_id)
inner join dim_stores s on o.store_id = s.store_id)
inner join dim_employees e on o.employee_id = e.employee_id)
left outer join StateCoordinates al on upper(cust_area_code) = al.state_code
WHERE NOT EXISTS (
    SELECT 1
    FROM RETAIL_TARGET.SALES_ANALYTICS sa
    WHERE sa.order_id = o.order_id
);

execute task LOAD_SALES_ANALYTICS_TASK;

create or replace view v_SALES_ANALYTICS
as
select *
from sales_analytics;

create or replace TABLE RETAIL_SALES_DB.RETAIL_TARGET.STATECOORDINATES (
	STATE_CODE VARCHAR(5),
	STATE_NAME VARCHAR(50),
	LATITUDE NUMBER(9,6),
	LONGITUDE NUMBER(9,6)
);



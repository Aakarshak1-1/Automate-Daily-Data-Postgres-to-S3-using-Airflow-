## Steps to Import CSV Data into PostgreSQL Running in Docker

### 1. Log Into PostgreSQL Inside Docker

```bash
docker exec -it container_id bash
psql -U airflow
```

### 2. Create a Database
```
CREATE DATABASE crm;
```
After creating the database, connect to it:
```
\c my_database
```

### 3. Create a Table
Now create a table that matches the structure of your CSV file. For example, if the CSV file contains the following columns: order_id, date, product_name, and quantity, run:
```bash
CREATE TABLE orders (
    order_id VARCHAR PRIMARY KEY,
    date DATE,
    product_name VARCHAR(100),
    quantity INT
);
```

### 4. Copy the CSV File into the Docker Container
```bash
docker cp /path/to/your/orders.csv postgres_container:/tmp/orders.csv
```

### 5. Create Staging Table to avoid error that occurs due to the present of duplicates 
```bash
DROP TABLE IF EXISTS orders_staging;

CREATE TABLE orders_staging (
    order_id VARCHAR,
    date DATE,
    product_name VARCHAR(100),
    quantity INT
);
```

### 6. Import data into the staging table
```bash
COPY orders_staging (order_id, date, product_name, quantity)
FROM '/tmp/Orders.csv'
DELIMITER ','
CSV HEADER;
```

### 7. Insert into orders table
```bash
INSERT INTO orders (order_id, date, product_name, quantity)
SELECT DISTINCT ON (order_id) order_id, date, product_name, quantity
FROM orders_staging
ORDER BY order_id, date DESC
ON CONFLICT (order_id) DO UPDATE
SET date = EXCLUDED.date,
    product_name = EXCLUDED.product_name,
    quantity = EXCLUDED.quantity;
```

### 8. Drop Staging Table
```
DROP TABLE orders_staging;
```

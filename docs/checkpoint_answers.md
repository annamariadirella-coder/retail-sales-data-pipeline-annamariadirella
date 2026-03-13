# Checkpoint Answers

## 1. What is the main difference between ETL and ELT in this mini-project?
In ETL, the data is cleaned in Python before loading it into PostgreSQL. In ELT, the raw data is loaded first and cleaned later inside PostgreSQL with SQL.

## 2. Why does the ETL version transform data in Python before loading it into PostgreSQL?
Because in ETL the data is prepared before loading, so PostgreSQL gets a clean final table ready for reporting.

## 3. Why does the ELT version load raw data into PostgreSQL first before transforming it?
Because in ELT the raw data is kept first, and the cleaning and transformation happen later in the database with SQL.

## 4. What are the three raw source files in this project, and what does each one represent in the business scenario?
`customers.csv` is customer data, `orders.json` is order data, and `products.xml` is product catalog data.

## 5. Why is this project useful as a portfolio project for future job interviews?
Because it shows that I can work with different file formats, use PostgreSQL, build ETL and ELT pipelines, and organize a project clearly.
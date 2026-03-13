# Project Notes

This project uses three raw source files:
- customers.csv
- orders.json
- products.xml

I built two versions of the pipeline:
- ETL: transform in Python before loading into PostgreSQL
- ELT: load raw data first, then transform inside PostgreSQL with SQL

Final tables:
- sales_report
- sales_report_elt
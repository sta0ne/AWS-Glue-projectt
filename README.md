# AWS Glue ETL
![](https://github.com/sta0ne/AWS-Glue-projectt/blob/main/Images/Screenshot%202026-03-09%20184932.png)

## 📌 Project Overview
An automated ETL pipeline that processes raw sales data from S3, performs schema validation, calculates line-item revenue, and stores the output in a partitioned Parquet format.

## ⚙️ Tech Stack
- **AWS Glue Studio:** Visual ETL & PySpark
- **Amazon S3:** Data Lake (Raw & Processed)
- **Amazon Athena:** SQL Analytics
- **Glue Data Catalog:** Metadata Management

## 🔄 ETL Logic
1. **Source:** Ingests CSV files from `s3://cdcprojectt/source/`.
2. **Transform:** - Casts `price` to Decimal and `orderdate` to Date.
   - Calculates `revenue = quantity * price`.
   - Partitions data by `Year` and `Month`.
3. **Target:** Writes to `s3://cdcprojectt/target/` in Parquet format.

## 🚀 How to Run
1. Upload `sample_data.csv` to your S3 bucket.
2. Create an AWS Glue job using the provided `glue_script.py`.
3. Enable **Job Bookmarks** in the job settings.
4. Run the job and query the results in Athena.


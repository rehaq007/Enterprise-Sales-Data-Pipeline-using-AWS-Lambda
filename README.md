# 🚀 Enterprise Sales Data Pipeline using AWS Lambda

This project automates the process of ingesting sales data from Amazon S3, validating and transforming it, storing it in a MySQL RDS database, and sending notifications — all powered by AWS Lambda.

---

## 📦 What this project does:

1. Accepts **CSV or JSON files** with sales data uploaded to an S3 bucket.
2. **Validates** the data for required columns, correct data types, date formats, and duplicate UUIDs.
3. Moves invalid files to a **quarantine** folder for review.
4. Converts valid data into **Parquet format** and stores it in a timestamped folder in S3.
5. Inserts raw and cleaned data into **RDS MySQL tables**:
   - `sales` (raw data log)
   - `sales_tgt` (cleaned, deduplicated data)
   - `sales_summary` (aggregated summary by country)
6. Deletes the original **raw file** and removes it from the `raw/` folder.
7. Sends an **SNS notification** about success or failure.

---

## 🛠️ Step-by-Step Setup Guide

> Follow these steps to build and deploy your own serverless data pipeline.

---

### 1. Prepare Your S3 Bucket

- Create an S3 bucket (e.g., `dehlive-sales-<your-account-id>-us-east-1`)
- Inside the bucket, create a folder structure like:
  ```
  raw/
  quarantine/
  processed/
  ```
- Upload a sample file to the `raw/` folder (JSON or CSV)

---

### 2. Prepare Your RDS MySQL Database

- Launch a MySQL-compatible RDS database
- Create a database (e.g., `salesdb`)
- Connect the database with the client. You can use dbeaver. You can use the credentials from aws secret manager from below step to connect to your MySQL RDS.

**Please note that if you have not created the table and its schema it will create automatically.**

Use the **sample data provided in this repo**. Based upon your requirement, you can edit the code and create the schema based on your source file.

---

### 3. Create a Secrets Manager Entry

- Go to **AWS Secrets Manager**
- Create a secret with the dbname as "salesdb". You would need the below info to connect your client with the MySQL DB RDS
  ```json
  {
    "username": "your_db_user",
    "password": "your_db_password",
    "host": "your_rds_endpoint",
    "dbname": "salesdb"
  }
  ```
- Name your secret something like: `dev/database-1/salesdb`

---

### 4. Create an SNS Topic

- Go to **Amazon SNS** and create a topic named `dehtopic`
- Add your email or SMS subscription to receive notifications

---

### 5. Deploy the Lambda Function

- Go to **AWS Lambda** and create a new function (choose Python 3.10 runtime)
- Set up the **S3 trigger** for the `raw/` folder
- Add the following **IAM permissions** to the Lambda function role:
  - S3 (read/write)
  - RDS (network & connection permissions)
  - Secrets Manager (read secret)
  - SNS (publish)

---

### 6. Upload the Lambda Code

- Open the `lambda_function.py` file from this repository
- Copy the entire code and paste it into the **Lambda inline editor**
- Click **Deploy** to save the changes

> No zip upload required. Just copy the code from the repo and paste it into Lambda.

#### Add Layers to the Lambda Function -
- Scroll down, you will find the option to add Layers. Click on 'Add Layers'.
- You need to add below 3 Layers using ARN:
  
 1. **arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python310:23**
 2. **arn:aws:lambda:us-east-1:030798167757:layer:Aws_sql_alchemy_dehlive:4**
 3. **arn:aws:lambda:us-east-1:030798167757:layer:Pymysql_dehlive:1**

#### Add 'Environment Variables' from Configuration as below -
Key - dbname and Value - salesdb_dev

---

### 7. Test the Pipeline

- Upload a valid or invalid file into the `raw/` folder in your S3 bucket
- Check **CloudWatch logs** for processing info
- Check your S3 folders (`processed/`, `quarantine/`)
- Verify updates to your RDS tables (`sales`, `sales_tgt`, `sales_summary`)
- Confirm you received the **SNS email notification**

---

## 💡 Real-World Scenario

This pipeline is designed for teams who want to:

- **Automate daily ingestion** of sales data from vendors
- **Clean and validate** that data automatically
- **Store raw and cleaned data** separately for compliance and analytics
- **Receive alerts** when files fail validation or succeed

---

## 🔄 Reprocessing the Same File?

- Re-uploading the same file **won't create duplicates** in `sales_tgt`
- Duplicate `uuid` values are dropped
- `sales_summary` is regenerated based on the cleaned data

---

## 👨‍💻 Author

**Rehan Qureshi**  
[LinkedIn](#) | [Portfolio](#) *(https://www.linkedin.com/in/rehan-qureshi-4a1078302/)*

---

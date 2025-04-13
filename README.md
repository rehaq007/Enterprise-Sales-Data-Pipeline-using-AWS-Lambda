# 🚀 Enterprise Sales Data Pipeline using AWS Lambda

This project automates the ingestion, validation, transformation, storage, and notification of enterprise sales data using a completely serverless architecture on AWS.

---

## 📦 Project Overview

This pipeline:

- Accepts **CSV or JSON** sales data uploaded to S3
- **Validates** the data (schema, types, duplicates)
- Moves invalid files to a **quarantine** folder
- Converts valid files to **Parquet** format
- Stores **raw and clean** data in RDS MySQL
- Generates a **sales summary**
- delete the **raw folder**
- Sends **SNS notifications** on success or failure

---

## 🛠️ Step-by-Step Setup Guide

### 1. 📁 Create and Configure Your S3 Bucket

1. Go to **Amazon S3** and create a new bucket  
   Example: `dehlive-sales-<your-account-id>-us-east-1`
2. Inside the bucket, create the following folders:
   ```
   raw/
   quarantine/
   processed/
   ```

---

### 2. 🧪 Set Up RDS MySQL

1. Launch a MySQL-compatible RDS instance in **Amazon RDS**
2. Create a database named `salesdb`
3. Create the following three tables:

#### ✅ `sales` (raw data log)

```sql
CREATE TABLE IF NOT EXISTS sales (
    uuid VARCHAR(100) PRIMARY KEY,
    country VARCHAR(100),
    item_type VARCHAR(100),
    sales_channel VARCHAR(100),
    order_priority VARCHAR(10),
    order_date DATE,
    order_id BIGINT,
    ship_date DATE,
    units_sold INT,
    unit_price DECIMAL(10, 2),
    unit_cost DECIMAL(10, 2),
    total_revenue DECIMAL(15, 2),
    total_cost DECIMAL(15, 2),
    total_profit DECIMAL(15, 2),
    file_name VARCHAR(255),
    load_timestamp TIMESTAMP
);
```

#### ✅ `sales_tgt` (validated, deduplicated clean data)

```sql
CREATE TABLE IF NOT EXISTS sales_tgt LIKE sales;
```

#### ✅ `sales_summary` (aggregated total sales by country)

```sql
CREATE TABLE IF NOT EXISTS sales_summary (
    country VARCHAR(100),
    total_units_sold INT,
    total_revenue DECIMAL(15, 2),
    total_profit DECIMAL(15, 2),
    summary_date TIMESTAMP
);

##### Please note that if you have not created the table and its schema it will create automatically. Use the sample data I have provided in the repo. Based upon your requirement you can edit the code and create the schema according to your requirement/source file.
```

---

### 3. 🔐 Set Up AWS Secrets Manager

1. Open **AWS Secrets Manager**
2. Click **"Store a new secret"**
3. Choose **Other type of secret**
4. Add the following key-value pairs:

```json
{
  "username": "your_db_user",
  "password": "your_db_password",
  "host": "your_rds_endpoint",
  "dbname": "salesdb"
}
```

5. Name the secret:  
   `dev/database-1/salesdb`

---

### 4. 📨 Set Up SNS for Notifications

1. Open **Amazon SNS**
2. Create a topic: `dehtopic`
3. Add a subscription (email/SMS) to receive alerts:
   - Type: `Email`
   - Endpoint: your email address
4. Confirm the subscription from your inbox

---

### 5. 🧑‍💻 Create and Configure the Lambda Function

1. Go to **AWS Lambda**, click **Create function**
2. Choose:
   - Runtime: `Python 3.10`
   - Permissions: Create or use an existing IAM role with:
     - S3 Full Access
     - RDS Access
     - Secrets Manager Read
     - SNS Publish

3. Set **Trigger**:
   - Source: **S3**
   - Event: `PUT` on `raw/` folder

---

### 6. 🐍 Upload Lambda Code

#### Option 1: Manually upload ZIP

1. Zip your `lambda_function.py` along with all necessary libraries (e.g., `pandas`, `sqlalchemy`)
2. Upload the ZIP under **Code > Upload from > .zip file**

#### Option 2: Use Lambda Layers

1. Create a Lambda Layer with packages like:
   - `pandas`
   - `pyarrow`
   - `sqlalchemy`
2. Attach this layer to your Lambda function

---

### 7. ✅ Test Your Pipeline

1. Upload a valid `.csv` or `.json` to `raw/`
2. Check:
   - ✅ `processed/` for the Parquet file
   - ❌ `quarantine/` if validation fails
   - 📊 RDS tables for inserts
   - 🔔 SNS alert in your email
   - 📜 CloudWatch for detailed logs

---

## 🧠 How the Schema is Created Automatically

If you're using `pandas.to_sql()` with SQLAlchemy, the Lambda code will:

1. Connect to the database using credentials from Secrets Manager
2. Use `to_sql(..., if_exists='append')` or `if_exists='replace'`
3. SQLAlchemy + pandas will automatically:
   - Create the table if it doesn't exist
   - Map Python/Pandas types to SQL types
   - Insert the data row-by-row or in batch

> Note: You must ensure the data types match (e.g., dates, decimals, UUID strings) to avoid insert errors.

---

## 🔄 Handling Repeated File Uploads

- ✅ UUID is the primary key in `sales` and `sales_tgt`
- ✅ If a file is uploaded again, it will **not** insert duplicate records
- ✅ The summary table is recalculated, not duplicated

---

## 💡 Use Case

Ideal for teams that want to:

- Automatically ingest and validate daily sales files
- Store both raw and clean data
- Use MySQL for analytics/reporting
- Get email alerts when something goes wrong

---


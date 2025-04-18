"""
Enterprise Sales Data Pipeline - Lambda Function

Author: Rehan Qureshi
Date: April 15, 2025
Version: 5.0.4

Description:
-------------
This AWS Lambda function handles an enterprise data pipeline with:
1. Multi-format data ingestion (CSV/JSON)
2. Data validation and quarantine
3. Parquet conversion for data lake (saved under a timestamped folder)
4. RDS MySQL loading (with sales_summary aggregated by country)
5. SNS notifications

AWS Services Used:
------------------
- S3 (Data Lake)
- RDS MySQL (Data Warehouse)
- SNS (Notifications)
- AWS Lambda (Compute)
- Secrets Manager (Credentials)
"""

import pandas as pd
import boto3
import json
import os
import io
import pymysql
from sqlalchemy import create_engine
from datetime import datetime
import logging

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
secrets_client = boto3.client('secretsmanager')

def get_aws_account_id():
    """Retrieve AWS account ID using STS."""
    sts = boto3.client('sts')
    account_id = sts.get_caller_identity()['Account']
    logger.info(f"AWS Account ID: {account_id}")
    return account_id

def get_timestamp_folder():
    """Generate a timestamp string for folder naming."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    logger.info(f"Generated timestamp folder: {timestamp}")
    return timestamp

def read_data_from_s3(bucket, key):
    """Read CSV/JSON files from S3."""
    try:
        logger.info(f"Reading file from s3://{bucket}/{key}")
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        file_content = obj['Body'].read()
        if key.endswith('.csv'):
            df = pd.read_csv(io.BytesIO(file_content))
        elif key.endswith('.json'):
            try:

                df = pd.read_json(io.BytesIO(file_content))
            except ValueError:
                df = pd.read_json(io.BytesIO(file_content), lines=True)

        else:
            raise ValueError("Unsupported file format")
        logger.info("File read successfully.")
        return df
    except Exception as e:
        logger.error(f"Error reading file from S3: {str(e)}")
        raise RuntimeError(f"Error reading file from S3: {str(e)}")

def validate_data(df):
    """Validate incoming data"""
    required_columns = [
        'uuid', 'Country', 'ItemType', 'SalesChannel', 'OrderPriority',
        'OrderDate', 'Region', 'ShipDate', 'UnitsSold', 'UnitPrice',
        'UnitCost', 'TotalRevenue', 'TotalCost', 'TotalProfit'
    ]
    errors = []
    
    # Check for required columns
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        errors.append(f"Missing columns: {', '.join(missing_cols)}")
    
    # Check numeric fields only if the column exists
    numeric_cols = ['UnitsSold', 'UnitPrice', 'UnitCost', 'TotalRevenue', 'TotalCost', 'TotalProfit']
    for col in numeric_cols:
        if col in df.columns:
            if not pd.api.types.is_numeric_dtype(df[col]):
                errors.append(f"Non-numeric values in {col}")

    # Check date formats only if the column exists
    date_cols = ['OrderDate', 'ShipDate']
    for col in date_cols:
        if col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col], format='%m/%d/%Y')
            except Exception:
                errors.append(f"Invalid date format in {col}")
    
    # Check UUID uniqueness only if the column exists
    if 'uuid' in df.columns:
        if df['uuid'].duplicated().any():
            errors.append("Duplicate UUIDs found")
    
    if errors:
        logger.warning(f"Validation errors: {errors}")
    else:
        logger.info("Data validation passed successfully.")
    
    return errors

def move_to_quarantine(bucket, key, reason):
    """Move invalid files to a quarantine folder under a timestamp sub-folder."""
    try:
        timestamp = get_timestamp_folder()
        base_filename = os.path.basename(key)
        quarantine_key = f"quarantine/{timestamp}/{base_filename}"
        logger.info(f"Moving file to quarantine: s3://{bucket}/{quarantine_key}")
        copy_source = {'Bucket': bucket, 'Key': key}
        s3_client.copy_object(
            CopySource=copy_source,
            Bucket=bucket,
            Key=quarantine_key
        )
        s3_client.delete_object(Bucket=bucket, Key=key)
        logger.info("File successfully moved to quarantine.")
    except Exception as e:
        logger.error(f"Quarantine operation failed: {str(e)}")

def write_parquet_to_s3(df, bucket, key):
    """Convert DataFrame to Parquet and upload to S3 under a timestamped folder."""
    try:
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        timestamp = get_timestamp_folder()
        base_filename = os.path.splitext(os.path.basename(key))[0]  # Remove extension
        target_key = f"processed/{timestamp}/{base_filename}.parquet"
        logger.info(f"Writing Parquet file to s3://{bucket}/{target_key}")
        s3_client.put_object(
            Bucket=bucket,
            Key=target_key,
            Body=parquet_buffer.getvalue()
        )
        logger.info("Parquet file written successfully.")
    except Exception as e:
        logger.error(f"Parquet conversion failed: {str(e)}")
        raise RuntimeError(f"Parquet conversion failed: {str(e)}")

def update_rds_tables(df, credentials):
    """Update MySQL RDS tables with new schema and aggregate summary by Country."""
    try:
        engine = create_engine(
            f"mysql+pymysql://{credentials['username']}:{credentials['password']}"
            f"@{credentials['host']}/{credentials['dbname']}"
        )
        # Convert date columns to string format
        df['OrderDate'] = df['OrderDate'].dt.strftime('%Y-%m-%d')
        df['ShipDate'] = df['ShipDate'].dt.strftime('%Y-%m-%d')

        logger.info("Inserting raw data into sales table.")
        df.to_sql('sales', engine, if_exists='append', index=False)

        logger.info("Updating sales_tgt table.")
        try:
            existing_sales_tgt = pd.read_sql_table('sales_tgt', con=engine)
        except Exception:
            existing_sales_tgt = pd.DataFrame(columns=df.columns)
        merged_df = pd.concat([existing_sales_tgt, df], ignore_index=True)
        merged_df.drop_duplicates(subset=['uuid'], keep='last', inplace=True)
        merged_df.to_sql('sales_tgt', engine, if_exists='replace', index=False)

        logger.info("Updating sales_summary table with aggregation by Country.")
        summary_df = df.groupby('Country').agg(
            max_units_sold=('UnitsSold', 'max'),
            average_total_revenue=('TotalRevenue', 'mean'),
            average_total_cost=('TotalCost', 'mean'),
            average_total_profit=('TotalProfit', 'mean')
        ).reset_index()
        summary_df.to_sql('sales_summary', engine, if_exists='replace', index=False)
        logger.info("RDS tables updated successfully.")
    except Exception as e:
        logger.error(f"RDS update failed: {str(e)}")
        raise RuntimeError(f"RDS update failed: {str(e)}")

def delete_raw_file(bucket, key):
    """Delete the original raw file after processing"""
    try:
        s3_client.delete_object(Bucket=bucket, Key=key)
        logger.info(f"[CLEANUP] Deleted raw file: s3://{bucket}/{key}")
    except Exception as e:
        logger.error(f"[ERROR] Failed to delete raw file: {e}")

def lambda_handler(event, context):
    """Main Lambda function."""
    try:
        record = event['Records'][0]
        raw_bucket = record['s3']['bucket']['name']
        raw_key = record['s3']['object']['key']
        logger.info(f"[START] Processing file: s3://{raw_bucket}/{raw_key}")
    except Exception:
        logger.error("[ERROR] Malformed event structure.")
        raise ValueError("Malformed event structure")
    
    region = boto3.Session().region_name
    account_id = get_aws_account_id()

    try:
        secrets = secrets_client.get_secret_value(SecretId='dev/database-1/salesdb')
        db_credentials = json.loads(secrets['SecretString'])
        logger.info("[INFO] Successfully retrieved DB credentials from Secrets Manager.")
    except Exception:
        logger.error("[ERROR] Failed to retrieve database credentials.")
        raise RuntimeError("Failed to retrieve database credentials")

    try:
        # Step 1: Read data from S3
        raw_df = read_data_from_s3(raw_bucket, raw_key)
        logger.info("[STEP 1] Data read from S3.")

        # Step 2: Validate data
        validation_errors = validate_data(raw_df)
        if validation_errors:
            logger.error(f"[VALIDATION FAILED] {validation_errors} - Moving file to quarantine.")
            move_to_quarantine(raw_bucket, raw_key, "; ".join(validation_errors))
            return {"status": "failed", "reason": validation_errors}
        logger.info("[STEP 2] Data validation passed.")

        # Step 3: Convert data to Parquet and write to S3 under a timestamp folder
        write_parquet_to_s3(raw_df, raw_bucket, raw_key)
        logger.info("[STEP 3] Parquet file created and uploaded to S3.")

        # Step 4: Update RDS tables with processed data and aggregated summary by Country
        update_rds_tables(raw_df, db_credentials)
        logger.info("[STEP 4] RDS tables updated successfully.")

        # Step 5: Delete the raw file
        delete_raw_file(raw_bucket, raw_key)

        # Step 6: Send success notification via SNS
        sns_client.publish(
            TopicArn=f"arn:aws:sns:{region}:{account_id}:dehtopic",
            Subject="Data Processing Successful",
            Message=f"Processed file: {raw_key}"
        )
        logger.info("[END] Success notification sent via SNS.")

        return {"status": "success"}
    except Exception as e:
        logger.error(f"[ERROR] Processing failed for {raw_key}: {str(e)}")
        sns_client.publish(
            TopicArn=f"arn:aws:sns:{region}:{account_id}:dehtopic",
            Subject="Data Processing Failed",
            Message=f"Error processing {raw_key}: {str(e)}"
        )
        return {"status": "failed", "error": str(e)}

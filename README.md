# Movie Data ETL Pipeline

This project is an ETL (Extract, Transform, Load) pipeline for processing movie data using AWS Glue, Amazon S3, and Amazon EventBridge. It automates data ingestion, quality evaluation, transformation, and loading into Amazon Redshift, ensuring data quality and providing mechanisms for error handling and notifications.

## Table of Contents
- [Project Overview](#project-overview)
- [Components](#components)
- [Workflow](#workflow)
- [Setup Instructions](#setup-instructions)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## Project Overview
This ETL pipeline processes movie data by:
- Automates Data Processing: The pipeline automates the entire data processing workflow, reducing manual intervention and ensuring consistent data delivery.
- Data Quality Checks: Glue jobs within the pipeline incorporate data quality checks to identify and handle potential issues in the movie data, like missing values or invalid entries.
- Schema Discovery & Management: AWS Glue crawlers discover and manage the schema of movie data in S3 and Redshift, ensuring the pipeline works with the latest schema information.
- Efficient Workflow Orchestration: AWS Step Functions orchestrates the workflow between different stages of the data pipeline (data extraction, transformation, loading) for efficient execution.
- Proactive Monitoring & Notifications: AWS SNS topics provide notifications about the success or failure of the Glue job, enabling proactive monitoring and maintenance of the data pipeline.

![Architecture Diagram](https://github.com/jignesh-kachhad/Movie-Data-Analysis-With-Data-Quality-Check/blob/main/Architecture.png).

## Components
### Amazon S3
- `movies-gds1` bucket with the following folders:
  - `bad_records/`
  - `historical_DQ_outcome/`
  - `input_data/`
  - `rule_outcome_from_etl/`

### AWS Glue Crawlers
- `crawl_movies_data_in_s3`
- `crawl_movies_data_in_redshift`

### AWS Glue Jobs
- Glue job script that:
  1. Reads data from S3.
  2. Evaluates data quality.
  3. Transforms data format.
  4. Writes evaluation outcomes and failed records to S3.
  5. Loads transformed data into Glue Data Catalog.

### Amazon EventBridge
- Rule: `movies-data-pipeline`

### Step Functions
- Orchestrates the workflow:
  1. Starts the S3 crawler.
  2. Monitors crawler state.
  3. Starts the Glue job.
  4. Monitors Glue job state and sends SNS notifications.

## Workflow
1. **Data Crawling**:
   - Crawlers scan S3 and Redshift, updating the Glue Data Catalog.
2. **Data Quality Evaluation**:
   - Glue job reads raw data from S3 and applies quality rules.
   - Splits data into groups based on evaluation.
3. **Data Transformation**:
   - Transforms data format using schema mapping.
4. **Data Loading**:
   - Writes evaluation outcomes and failed records to S3.
   - Loads transformed data into Glue Data Catalog.
5. **Orchestration**:
   - Step Function orchestrates the workflow and handles notifications.

## Setup Instructions
1. **AWS Setup**:
   - Create an S3 bucket named `movies-gds1` with the required folders.
   - Configure AWS Glue Crawlers and Jobs.
   - Set up EventBridge rules and Step Functions.

2. **Clone the Repository**:
   ```bash
   git clone https://github.com/yourusername/movie-data-etl-pipeline.git
   cd movie-data-etl-pipeline
   ```

3. **AWS Configuration**:
   - Update the AWS Glue job script with your S3 bucket and table names.
   - Configure IAM roles and permissions for AWS Glue, S3, EventBridge, and Step Functions.

4. **Deploy the Pipeline**:
   - Deploy the Step Functions state machine.
   - Schedule the EventBridge rule.

## Usage
1. **Start the Pipeline**:
   - Upload data to the `input_data` folder in the S3 bucket.
   - The pipeline will automatically trigger and process the data.

2. **Monitor the Pipeline**:
   - Use AWS Step Functions console to monitor the state machine.
   - Check S3 for data quality outcomes and transformed data.
   - Review SNS notifications for job status.

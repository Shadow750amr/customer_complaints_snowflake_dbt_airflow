# Consumer complaints against commercial businesses. ELT using python, airflow, Amazon S3, dbt and snowflake.

This repository aims to analyze complaints against commercial businesses in México.

The data was acquired using the national open data platform, hosted by the mexican government.

This project was designed to be a demonstration of the wide use of cloud frameworks and tools where every piece (the tool itself) has its own challenges in design and implementation. This ELT pipeline follows (at least) the following state of the arts tools and data engineering tendencies, framework and platforms:

Extraction: Python.
Transformation: DBT
Warehousing: Snowflake
Storage: Amazon S3
Orchestration: Airflow (astronomer-cosmos).



# S3 integration with snowflake

To create the storage integration with snowflake:

1. Create a new policy in AWS with the following permissions:

{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "Statement1",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": "your arn bucket"
        }
    ]
}

2. Create a role in AWS to which the policie will be attached to.

3. Create the storage integration in snowflake with the following settings:

CREATE OR REPLACE STORAGE INTEGRATION your_integration_name
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'your_arn_role'
  STORAGE_ALLOWED_LOCATIONS = ( 'your-bucket-name') ---- structure: s3://bucket-name/

4. Once the integration has been created, execute the DESC INTEGRATION 'your_integration_name' to visualize the integration settings and from which you are going to need:
- STORAGE_AWS_IAM_USER_ARN
- STORAGE_AWS_EXTERNAL_ID

5. Change the trust relationships of your role with the following settings:

{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "your-snowflake-iam-user-arn"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "your-aws-external-id"
                }
            }
        }
    ]
}


6. Finally, create the external stage using your integration storage and:


CREATE OR REPLACE STAGE your_external_stage_name
  STORAGE_INTEGRATION = my-integration-name
  URL = 'your-s3-bucket/' 
  FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1);

7. A good practice is to confirm the stage has been succesfully linked with LIST @your_external_stage_name;




Hot to use it:

1. Donwload the repo using clone.
2. Download the dependencies using dbt deps
3. Create your profiles.yml at home/user/.dbt/profiles.yml usign the following structure:

4. You are good to go.













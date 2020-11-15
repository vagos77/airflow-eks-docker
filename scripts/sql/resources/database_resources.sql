-- Create JSON format parser
CREATE FILE FORMAT IF NOT EXISTS AE_JSON_FORMAT TYPE = 'JSON'
COMPRESSION = 'AUTO' ENABLE_OCTAL = FALSE ALLOW_DUPLICATE = FALSE
STRIP_OUTER_ARRAY = TRUE STRIP_NULL_VALUES = FALSE IGNORE_UTF8_ERRORS = FALSE;

-- Create CSV format parser
CREATE OR REPLACE FILE FORMAT DATA_VAULT.AT_CSV_FORMAT TYPE = 'CSV'
FIELD_DELIMITER = ',' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 NULL_IF = "NULL"
DATE_FORMAT = "DD/MM/YYYY" ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE;

-- TODO copy files from AE S3
-- TODO storage integration
-- FIXME to be fixed with the updated AE folder structure and naming convention

-- Create stages
-- AE Property data
CREATE STAGE IF NOT EXISTS ae_property_data
  storage_integration = voyanta_glue
  url = 's3://voyanta-glue/snowflake/json/ae/'
  file_format = AE_JSON_FORMAT;

-- AE Chart of accounts
CREATE STAGE IF NOT EXISTS ae_coa_data
  storage_integration = voyanta_glue
  url = 's3://voyanta-glue/snowflake/json/generic/coa/'
  file_format = AE_JSON_FORMAT;

-- Line item types
CREATE STAGE IF NOT EXISTS ae_lit_data
  storage_integration = voyanta_glue
  url = 's3://voyanta-glue/snowflake/json/generic/lit/'
  file_format = AE_JSON_FORMAT;

-- Taliance
CREATE STAGE IF NOT EXISTS at_csv_data
  storage_integration = voyanta_glue
  url = 's3://voyanta-glue/snowflake/csv/taliance'
  file_format = AT_CSV_FORMAT;

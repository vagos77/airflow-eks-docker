-- Create JSON format parser
CREATE FILE FORMAT IF NOT EXISTS AE_JSON_FORMAT TYPE = 'JSON'
COMPRESSION = 'AUTO' ENABLE_OCTAL = FALSE ALLOW_DUPLICATE = FALSE
STRIP_OUTER_ARRAY = TRUE STRIP_NULL_VALUES = FALSE IGNORE_UTF8_ERRORS = FALSE;

-- Create CSV format parser
CREATE OR REPLACE FILE FORMAT DATA_VAULT.AT_CSV_FORMAT TYPE = 'CSV'
FIELD_DELIMITER = ',' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 NULL_IF = "NULL"
DATE_FORMAT = "DD/MM/YYYY" ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE;

-- Create stage
-- FIXME to be fixed with the updated AE folder structure and naming convention
CREATE STAGE IF NOT EXISTS ae_property_data
  storage_integration = voyanta_glue
  url = 's3://voyanta-glue/snowflake/json/ae/'
  file_format = AE_JSON_FORMAT;

-- Create stage
-- FIXME to be moved in proper S3 AE folder
CREATE STAGE IF NOT EXISTS ae_coa_data
  storage_integration = voyanta_glue
  url = 's3://voyanta-glue/snowflake/json/generic/coa/'
  file_format = AE_JSON_FORMAT;

-- Create stages
-- TODO restore correct JSON format (instead of reusing AE's one) because of UTF-8 errors when importing. AE's encoding is probably different from Taliance's
CREATE STAGE IF NOT EXISTS at_json_data
  storage_integration = voyanta_glue
  url = 's3://voyanta-glue/snowflake/json/taliance/'
  file_format = AE_JSON_FORMAT;

CREATE STAGE IF NOT EXISTS at_csv_data
  storage_integration = voyanta_glue
  url = 's3://voyanta-glue/snowflake/csv/taliance'
  file_format = AT_CSV_FORMAT;

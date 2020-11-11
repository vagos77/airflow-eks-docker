/* *********************************************************************************** */
/* *** VARIABLES ********************************************************************* */
/* *********************************************************************************** */

ALTER SESSION SET TIMEZONE = 'UTC';
SET LoadDTS = CURRENT_TIMESTAMP;

-- Temporary variables required where source files are not currently providing data
-- Change these as required for testing, e.g. incrementing version number
-- FIXME
SET S3folder = 'V6.5';

/* *********************************************************************************** */
/* *** CREATE TEMPORARY TABLES ******************************************************* */
/* *********************************************************************************** */

CREATE OR REPLACE TRANSIENT TABLE transient_at_comment (
    SCENARIO_ID integer
  , ENTITY_ID integer
  , COMMENT_TYPE_ID integer
  , COMMENT_LABEL string
  , LoadDTS timestamp_ltz
  , StageRecSrc string(100)
);

CREATE OR REPLACE TRANSIENT TABLE transient_at_comment_type (
    SCENARIO_ID integer
  , COMMENT_TYPE_ID integer
  , COMMENT_TYPE_CODE string(50)
  , COMMENT_TYPE_LABEL string
  , LoadDTS timestamp_ltz
  , StageRecSrc string(100)
);

CREATE OR REPLACE TRANSIENT TABLE transient_at_data_set (
    SCENARIO_ID integer
  , DATA_SET_ID integer
  , DATA_SET_CODE string(50)
  , DATA_SET_LABEL string
  , DATA_SET_OPERATION_MODEL integer
  , LoadDTS timestamp_ltz
  , StageRecSrc string(100)
);

CREATE OR REPLACE TRANSIENT TABLE transient_at_detail_data_set (
    SCENARIO_ID integer
  , DATA_SET_ID integer
  , LINE_ITEM_ID integer
  , NORDER integer
  , LoadDTS timestamp_ltz
  , StageRecSrc string(100)
);

CREATE OR REPLACE TRANSIENT TABLE transient_at_dimension (
    SCENARIO_ID integer
  , DIMENSION_ID integer
  , DIMENSION_CODE string(50)
  , DIMENSION_LABEL string
  , LoadDTS timestamp_ltz
  , StageRecSrc string(100)
);

CREATE OR REPLACE TRANSIENT TABLE transient_at_entity (
    SCENARIO_ID integer
  , ENTITY_ID integer
  , ENTITY_CODE string(50)
  , ENTITY_LABEL string
  , ENTITY_TYPE_ID integer
  , AE_EXTERNAL_ID string(255)
  , BEGIN_DATE date
  , END_DATE date
  , LoadDTS timestamp_ltz
  , StageRecSrc string(100)
);

CREATE OR REPLACE TRANSIENT TABLE transient_at_entity_type (
    SCENARIO_ID integer
  , ENTITY_TYPE_ID integer
  , ENTITY_TYPE_CODE string(50)
  , ENTITY_TYPE_LABEL string
  , LoadDTS timestamp_ltz
  , StageRecSrc string(100)
);

CREATE OR REPLACE TRANSIENT TABLE transient_at_fact (
    SCENARIO_ID integer
  , ENTITY_ID integer
  , LINE_ITEM_ID integer
  , DATE date
  , PERIODICITY string(1)
  , VALUE string
  , DIMENSION_ID integer
  , LoadDTS timestamp_ltz
  , StageRecSrc string(100)
);

CREATE OR REPLACE TRANSIENT TABLE transient_at_line_item (
    SCENARIO_ID integer
  , LINE_ITEM_ID integer
  , LINE_ITEM_CODE string(50)
  , LINE_ITEM_LABEL string
  , LINE_ITEM_CATEGORY_CODE string(255)
  , LINE_ITEM_TYPE string(50)
  , LINE_ITEM_CHARACTERISTIC string(5)
  , LINE_ITEM_STOCK_FLOW string(5)
  , LoadDTS timestamp_ltz
  , StageRecSrc string(100)
);

CREATE OR REPLACE TRANSIENT TABLE transient_at_relationship (
    SCENARIO_ID integer
  , RELATIONSHIP_TYPE_ID integer
  , PARENT_ENTITY_ID integer
  , CHILD_ENTITY_ID integer
  , START_DATE date
  , END_DATE date
  , PATH string
  , CALCULATION string
  , RATIO decimal(13,10)
  , DIRECT_RELATIONSHIP boolean
  , LoadDTS timestamp_ltz
  , StageRecSrc string(100)
);

CREATE OR REPLACE TRANSIENT TABLE transient_at_relationship_type (
    SCENARIO_ID integer
  , RELATIONSHIP_TYPE_CODE_DUMMY string(50)
  , RELATIONSHIP_TYPE_ID integer
  , RELATIONSHIP_TYPE_CODE string(50)
  , RELATIONSHIP_TYPE_LABEL string
  , LoadDTS timestamp_ltz
  , StageRecSrc string(100)
);

CREATE OR REPLACE TRANSIENT TABLE transient_at_scenario (
    SCENARIO_ID integer
  , SCENARIO_CODE string(50)
  , LoadDTS timestamp_ltz
  , StageRecSrc string(100)
);

/* *********************************************************************************** */
/* *** LOAD FROM S3 ****************************************************************** */
/* *********************************************************************************** */
-- TODO define naming convention for CSV files

SET LoadPattern = '.*/taliance/' || $S3folder || '/COMMENTS.csv';
COPY INTO transient_at_comment
FROM @at_csv_data
pattern = $LoadPattern
on_error = 'continue'
force = true;

SET LoadPattern = '.*/taliance/' || $S3folder || '/COMMENT_TYPE.csv';
COPY INTO transient_at_comment_type
FROM @at_csv_data
pattern = $LoadPattern
on_error = 'continue'
force = true;

SET LoadPattern = '.*/taliance/' || $S3folder || '/DATA_SETS.csv';
COPY INTO transient_at_data_set
FROM @at_csv_data
pattern = $LoadPattern
on_error = 'continue'
force = true;

SET LoadPattern = '.*/taliance/' || $S3folder || '/DETAIL_DATA_SET.csv';
COPY INTO transient_at_detail_data_set
FROM @at_csv_data
pattern = $LoadPattern
on_error = 'continue'
force = true;

SET LoadPattern = '.*/taliance/' || $S3folder || '/DIMENSION.csv';
COPY INTO transient_at_dimension
FROM @at_csv_data
pattern = $LoadPattern
on_error = 'continue'
force = true;

SET LoadPattern = '.*/taliance/' || $S3folder || '/ENTITIES.csv';
COPY INTO transient_at_entity
FROM @at_csv_data
pattern = $LoadPattern
on_error = 'continue'
force = true;

SET LoadPattern = '.*/taliance/' || $S3folder || '/ENTITY_TYPE.csv';
COPY INTO transient_at_entity_type
FROM @at_csv_data
pattern = $LoadPattern
on_error = 'continue'
force = true;

SET LoadPattern = '.*/taliance/' || $S3folder || '/[Ff][Aa][Cc][Tt].*[.]csv';
COPY INTO transient_at_fact
FROM @at_csv_data
pattern = $LoadPattern
file_format = (TYPE = CSV, FIELD_DELIMITER = ',', FIELD_OPTIONALLY_ENCLOSED_BY = '"', SKIP_HEADER = 1, NULL_IF = "NULL",  date_format = "YYYY-MM", error_on_column_count_mismatch=false)
on_error = 'continue'
force = true;

SET LoadPattern = '.*/taliance/' || $S3folder || '/LINE_ITEMS.csv';
COPY INTO transient_at_line_item
FROM @at_csv_data
pattern = $LoadPattern
on_error = 'continue'
force = true;

SET LoadPattern = '.*/taliance/' || $S3folder || '/RELATIONSHIPS.csv';
COPY INTO transient_at_relationship
FROM @at_csv_data
pattern = $LoadPattern
on_error = 'continue'
force = true;

SET LoadPattern = '.*/taliance/' || $S3folder || '/RELATIONSHIP_TYPE.csv';
COPY INTO transient_at_relationship_type
FROM @at_csv_data
pattern = $LoadPattern
on_error = 'continue'
force = true;

SET LoadPattern = '.*/taliance/' || $S3folder || '/SCENARIO .csv';
COPY INTO transient_at_scenario
FROM @at_csv_data
pattern = $LoadPattern
on_error = 'continue'
force = true;

-- update metadata upon load
SET Src = 'AT_CSV_EXPORT ' || $S3folder;

UPDATE transient_at_comment           SET LoadDTS = $LoadDTS, StageRecSrc = $Src WHERE LoadDTS IS NULL AND StageRecSrc IS NULL;
UPDATE transient_at_comment_type      SET LoadDTS = $LoadDTS, StageRecSrc = $Src WHERE LoadDTS IS NULL AND StageRecSrc IS NULL;
UPDATE transient_at_data_set          SET LoadDTS = $LoadDTS, StageRecSrc = $Src WHERE LoadDTS IS NULL AND StageRecSrc IS NULL;
UPDATE transient_at_detail_data_set   SET LoadDTS = $LoadDTS, StageRecSrc = $Src WHERE LoadDTS IS NULL AND StageRecSrc IS NULL;
UPDATE transient_at_dimension         SET LoadDTS = $LoadDTS, StageRecSrc = $Src WHERE LoadDTS IS NULL AND StageRecSrc IS NULL;
UPDATE transient_at_entity            SET LoadDTS = $LoadDTS, StageRecSrc = $Src WHERE LoadDTS IS NULL AND StageRecSrc IS NULL;
UPDATE transient_at_entity_type       SET LoadDTS = $LoadDTS, StageRecSrc = $Src WHERE LoadDTS IS NULL AND StageRecSrc IS NULL;
UPDATE transient_at_fact              SET LoadDTS = $LoadDTS, StageRecSrc = $Src WHERE LoadDTS IS NULL AND StageRecSrc IS NULL;
UPDATE transient_at_line_item         SET LoadDTS = $LoadDTS, StageRecSrc = $Src WHERE LoadDTS IS NULL AND StageRecSrc IS NULL;
UPDATE transient_at_relationship      SET LoadDTS = $LoadDTS, StageRecSrc = $Src WHERE LoadDTS IS NULL AND StageRecSrc IS NULL;
UPDATE transient_at_relationship_type SET LoadDTS = $LoadDTS, StageRecSrc = $Src WHERE LoadDTS IS NULL AND StageRecSrc IS NULL;
UPDATE transient_at_scenario          SET LoadDTS = $LoadDTS, StageRecSrc = $Src WHERE LoadDTS IS NULL AND StageRecSrc IS NULL;

-- set fact date at last day of the month
UPDATE transient_at_fact SET DATE = last_day(DATE);

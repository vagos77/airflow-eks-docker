/* *********************************************************************************** */
/* *** Argus Enterprise ************************************************************** */
/* *********************************************************************************** */

-- Create raw JSON table
CREATE TABLE IF NOT EXISTS ae_property_data_raw (
  SRC VARIANT);

-- Create raw JSON table
CREATE TABLE IF NOT EXISTS ae_coa_data_raw (
  SRC VARIANT);

-- Create raw JSON table
CREATE TABLE IF NOT EXISTS ae_lit_data_raw (
  SRC VARIANT);

/* *********************************************************************************** */
/* *** HUBs ************************************************************************** */
/* *********************************************************************************** */

CREATE TABLE IF NOT EXISTS HUB_AE_PORTFOLIO (
  MD5_HUB_AE_PORTFOLIO string(32)
  , PORTFOLIO_NAME string
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS HUB_AE_SCENARIO (
  MD5_HUB_AE_SCENARIO string(32)
  , SCENARIO_ID integer
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS HUB_AE_PROPERTY (
  MD5_HUB_AE_PROPERTY string(32)
  , PROPERTY_ID integer
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS HUB_EXTERNAL_PROPERTY (
  MD5_HUB_EXTERNAL_PROPERTY string(32),
  EXTERNAL_PROPERTY_ID string(255),
  LDTS timestamp_ltz,
  RSRC string(100)
);

CREATE TABLE IF NOT EXISTS HUB_AE_REVEX (
    MD5_HUB_AE_REVEX string(32)
  , PROPERTY_ID integer
  , REVEX_ID integer
  , LDTS timestamp_ltz
  , RSRC string(100)
);
  
CREATE TABLE IF NOT EXISTS HUB_AE_LEASE (
    MD5_HUB_AE_LEASE string(32)
  , PROPERTY_ID integer
  , LEASE_ID integer
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS HUB_AE_LOAN (
    MD5_HUB_AE_LOAN string(32)
  , PROPERTY_ID integer
  , LOAN_ID integer
  , LDTS timestamp_ltz
  , RSRC string(100) 
);

CREATE TABLE IF NOT EXISTS HUB_AE_LINE_ITEM_TYPE (
    MD5_HUB_AE_LINE_ITEM_TYPE string(32)
  , LINE_ITEM_TYPE_NAME string(255)
  , LDTS timestamp_ltz
  , RSRC string(100)
);

/* *********************************************************************************** */
/* *** SATs ************************************************************************** */
/* *********************************************************************************** */

CREATE TABLE IF NOT EXISTS SAT_AE_PORTFOLIO_DETAILS (
    MD5_HUB_AE_PORTFOLIO string(32)
  , HASH_DIFF string(32)
  , PORTFOLIO_NAME string(255)
  , PORTFOLIO_DESCRIPTION string
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS SAT_AE_SCENARIO_DETAILS (
    MD5_HUB_AE_SCENARIO string(32)
  , HASH_DIFF string(32)
  , SCENARIO_NAME string(255)
  , SCENARIO_DESCRIPTION string
  , SCENARIO_CURRENCY string(3)
  , SCENARIO_AREA_MEASURE integer -- TODO needs decoding
  , SCENARIO_ARCHIVED boolean
  , IS_BASE_SCENARIO boolean
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS SAT_AE_PROPERTY_VERSION (
    MD5_HUB_AE_PROPERTY string(32)
  , HASH_DIFF string(32)
  , PROPERTY_VERSION integer
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS SAT_AE_PROPERTY_DETAILS (
    MD5_HUB_AE_PROPERTY string(32)
  , HASH_DIFF string(32)
  , PROPERTY_NAME string(255)
  , VALUATION_DATE date
  , RESALE_DATE date
  , PROPERTY_TYPE integer
  , PROPERTY_TYPE_ENUM string(255)
  , LOCAL_CURRENCY string(3)
  , PROPERTY_AREA_MEASURE integer
  , PROPERTY_AREA_MEASURE_ENUM string(255)
  , PROPERTY_ARCHIVED boolean
  , PROPERTY_DESCRIPTION string
  , PROPERTY_COMMENTS string
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS SAT_AE_METADATA_PROPERTY (
    MD5_HUB_AE_PROPERTY string(32)
  , HASH_DIFF string(32)
  , PROPERTY_LAST_MODIFIED_BY string(255)
  , PROPERTY_LAST_MODIFIED_DATE timestamp_ltz
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS SAT_AE_PROPERTY_LOCATION (
    MD5_HUB_AE_PROPERTY string(32)
  , HASH_DIFF string(32)
  , ADDRESS string
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS SAT_AE_PROPERTY_CASHFLOW (
    MD5_HUB_AE_PROPERTY string(32)
  , HASH_DIFF string(32)
  , CURRENCY_BASIS integer -- 0 for property currency, 1 for scenario currency
  , IS_ASSURED_RESULTSET integer -- 0 default calculation, 1 calculation for assured income
  , LINE_ITEM_TYPE_ID integer
  , LINE_ITEM_TYPE_NAME string(255)
  , ACCOUNT_CODE string(255)
  , VALUATION_DATE_VALUE double
  , RESALE_VALUE double
  , UNIT_OF_MEASURE integer -- TODO needs decoding, not yet implemented
  , AMOUNT double
  , DATE date
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS SAT_AE_PROPERTY_KPI (
    MD5_HUB_AE_PROPERTY string(32)
  , HASH_DIFF string(32)
  , CURRENCY_BASIS integer -- 0 for property currency, 1 for scenario currency
  , IS_ASSURED_RESULTSET integer -- 0 default calculation, 1 calculation for assured income
  , RESULTSET_ID integer
  , RESULTSET string(255)
  , LINE_ITEM_TYPE_ID integer
  , LINE_ITEM_TYPE_NAME string(255)
  , ACCOUNT_CODE string(255)
  , VALUATION_DATE_VALUE double
  , UNIT_OF_MEASURE integer -- TODO needs decoding, not yet implemented
  , AMOUNT double
  , DATE date
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS SAT_AE_PROPERTY_PAYMENT (
    MD5_HUB_AE_PROPERTY string(32)
  , HASH_DIFF string(32)
  , CURRENCY_BASIS integer -- 0 for property currency, 1 for scenario currency
  , IS_ASSURED_RESULTSET integer -- 0 default calculation, 1 calculation for assured income
  , RESULTSET_ID integer
  , RESULTSET string(255)
  , LINE_ITEM_TYPE_ID integer
  , LINE_ITEM_TYPE_NAME string(255)
  , ACCOUNT_CODE string(255)
  , VALUATION_YEAR_TOTAL double
  , RESALE_YEAR_TOTAL double
  , AMOUNT double
  , DATE date
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS SAT_AE_REVEX_DETAILS (
    MD5_HUB_AE_REVEX string(32)
  , HASH_DIFF string(32)
  , REVEX_NAME string(255)
  , REVEX_TYPE_ID integer
  , REVEX_TYPE_ENUM string(255)
  , ACCOUNT_CODE string(255)
  , SORT_ORDER integer
  , BASIS integer -- TODO needs decoding
  , START_DATE date
  , UNIT_OF_MEASURE integer -- TODO needs decoding, not yet implemented
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS SAT_AE_REVEX_CASHFLOW (
    MD5_HUB_AE_REVEX string(32)
  , HASH_DIFF string(32)
  , CURRENCY_BASIS integer -- 0 for property currency, 1 for scenario currency
  , IS_ASSURED_RESULTSET integer -- 0 default calculation, 1 calculation for assured income
  , REVEX_TYPE_ID integer
  , REVEX_TYPE_ENUM string(255)
  , LINE_ITEM_TYPE_ID integer
  , LINE_ITEM_TYPE_NAME string(255)
  , UNIT_OF_MEASURE integer -- TODO needs decoding, not yet implemented
  , INITIAL_ANNUAL_AMOUNT double
  , AMOUNT double
  , DATE date
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS SAT_AE_REVEX_PAYMENT (
    MD5_HUB_AE_REVEX string(32)
  , HASH_DIFF string(32)
  , CURRENCY_BASIS integer -- 0 for property currency, 1 for scenario currency
  , IS_ASSURED_RESULTSET integer -- 0 default calculation, 1 calculation for assured income
  , RESULTSET_ID integer
  , RESULTSET string(255)
  , REVEX_TYPE_ID integer
  , REVEX_TYPE_ENUM string(255)
  , LINE_ITEM_TYPE_ID integer
  , LINE_ITEM_TYPE_NAME string(255)
  , AMOUNT double
  , DATE date
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS SAT_AE_REVEX_VERSION (
    MD5_HUB_AE_REVEX string(32)
  , HASH_DIFF string(32)
  , PROPERTY_VERSION integer
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS SAT_AE_LEASE_VERSION (
    MD5_HUB_AE_LEASE string(32)
  , HASH_DIFF string(32)
  , PROPERTY_VERSION integer
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS SAT_AE_LEASE_DETAILS (
    MD5_HUB_AE_LEASE string(32)
  , HASH_DIFF string(32)
  , IS_ASSURED_RESULTSET integer -- 0 default calculation, 1 calculation for assured income
  , LEASE_AGGREGATION_KEY integer -- ID of the record on the rent roll
  , IS_BASE_LEASE boolean -- True if the lease is on the rent roll, False if is generated by the calc for future leases
  , TENANT_NAME string(255)
  , SUITE string(255)
  , TENURE string(255)
  , LEASE_TYPE integer
  , LEASE_TYPE_ENUM string(255)
  , CUSTOM_LEASE_TYPE string(255)
  , LEASE_BEGIN date
  , LEASE_EXPIRY date
  , EXPIRY_TYPE integer
  , EXPIRY_TYPE_ENUM string(255)
  , EARLIEST_BREAK date
  , REMAINING_TERM_DAYS integer
  , LEASE_STATUS string(255)
  , LEASE_PERIOD_DAYS integer
  , MARKET_LEASE_PROFILE_NAME string(255)
  , RECOVERY_STRUCTURE_NAME string(255)
  , CPI_TYPE integer -- TODO needs decoding
  , CPI_TIMING string(255)
  , CPI_RATE_OR_INDEX string(255)
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS SAT_AE_LEASE_PAYMENT (
    MD5_HUB_AE_LEASE string(32)
  , HASH_DIFF string(32)
  , CURRENCY_BASIS integer -- 0 for property currency, 1 for scenario currency
  , IS_ASSURED_RESULTSET integer -- 0 default calculation, 1 calculation for assured income
  , LINE_ITEM_TYPE_ID integer
  , LINE_ITEM_TYPE_NAME string(255)
  , AMOUNT double
  , DATE date
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS SAT_AE_LEASE_CASHFLOW (
    MD5_HUB_AE_LEASE string(32)
  , HASH_DIFF string(32)
  , CURRENCY_BASIS integer -- 0 for property currency, 1 for scenario currency
  , IS_ASSURED_RESULTSET integer -- 0 default calculation, 1 calculation for assured income
  , LINE_ITEM_TYPE_ID integer
  , LINE_ITEM_TYPE_NAME string(255)
  , LEASE_BEGIN_VALUE double
  , VALUATION_DATE_VALUE double
  , LEASE_EXPIRY_VALUE double
  , RESALE_VALUE double
  , UNIT_OF_MEASURE integer -- TODO needs decoding, not yet implemented
  , AMOUNT double
  , DATE date
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS SAT_AE_LOAN_DETAILS (
    MD5_HUB_AE_LOAN string(32)
  , HASH_DIFF string(32)
  , LOAN_NAME string (255)
  , LOAN_TYPE integer
  , LOAN_TYPE_ENUM string (255)
  , SENIORITY integer -- TODO needs decoding
  , HOW_INPUT integer
  , HOW_INPUT_ENUM string (255)
  , LOAN_DATE date
  , LOAN_END date
  , LDTS timestamp_ltz
  , RSRC string(100)  
);

CREATE TABLE IF NOT EXISTS SAT_AE_LOAN_PAYMENT (
    MD5_HUB_AE_LOAN string(32)
  , HASH_DIFF string(32)
  , CURRENCY_BASIS integer
  , IS_ASSURED_RESULTSET integer
  , RESULTSET_ID integer
  , RESULTSET string(255)
  , LINE_ITEM_TYPE integer
  , LINE_ITEM_NAME string(255)
  , ACCOUNT_CODE string(255)
  , AMOUNT double
  , DATE date
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS SAT_AE_LOAN_VERSION (
    MD5_HUB_AE_LOAN string(32)
  , HASH_DIFF string(32)
  , PROPERTY_VERSION integer
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS SAT_AE_LINE_ITEM_TYPE_DETAILS (
    MD5_HUB_AE_LINE_ITEM_TYPE string(32)
  , HASH_DIFF string(32)
  , LINE_ITEM_TYPE_ID integer
  , LINE_ITEM_TYPE_PARENT_ID integer
  , CLASS_TYPE string(255)
  , ACCOUNT_RELATION_TYPE string(255)
  , SORT_ORDER integer
  , REPORT_DESCRIPTION string(255)
  , REPORT_GROUP string(255)
  , SHOW_ENTITIES boolean
  , IS_HEADER integer -- TODO needs decoding
  , COST_CODE_TYPE string(255)
  , LINEITEM_DEPTH integer
  , UNIT_OF_MEASURE_TYPE string(255)
  , LINEITEM_AGGREGATION_TYPE integer -- TODO needs decoding
  , RESULT_STORAGE_LOCATION string(255)
  , SECONDARY_RESULT_STORAGE_LOCATION string(255)
  , THIRD_RESULT_STORAGE_LOCATION string(255)
  , ACCOUNT_CODE string(255)
  , LDTS timestamp_ltz
  , RSRC string(100)
);

/* *********************************************************************************** */
/* *** LINKs ************************************************************************* */
/* *********************************************************************************** */

CREATE TABLE IF NOT EXISTS LINK_AE_SCENARIO_PORTFOLIO (
    MD5_LINK_AE_SCENARIO_PORTFOLIO string(32)
  , MD5_HUB_AE_SCENARIO string(32)
  , MD5_HUB_AE_PORTFOLIO string(32)
  , SCENARIO_ID integer
  , PORTFOLIO_NAME string(255)
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS LINK_AE_PROPERTY_SCENARIO (
    MD5_LINK_AE_PROPERTY_SCENARIO string(32)
  , MD5_HUB_AE_PROPERTY string(32)
  , MD5_HUB_AE_SCENARIO string(32)
  , PROPERTY_ID integer
  , SCENARIO_ID integer
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS LINK_AE_PROPERTY_EXT_PROPERTY (
    MD5_LINK_AE_PROPERTY_EXT_PROPERTY string(32)
  , MD5_HUB_AE_PROPERTY string(32)
  , MD5_HUB_EXTERNAL_PROPERTY string(32)
  , PROPERTY_ID integer
  , EXTERNAL_PROPERTY_ID string(255)
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS LINK_AE_REVEX_PROPERTY (
    MD5_LINK_AE_REVEX_PROPERTY string(32)
  , MD5_HUB_AE_REVEX string(32)
  , MD5_HUB_AE_PROPERTY string(32)
  , MD5_HUB_AE_PARENT_PROPERTY string(32)
  , REVEX_ID integer
  , PROPERTY_ID integer
  , PARENT_ID integer DEFAULT -1
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS LINK_AE_LEASE_PROPERTY (
    MD5_LINK_AE_LEASE_PROPERTY string(32)
  , MD5_HUB_AE_LEASE string(32)
  , MD5_HUB_AE_PROPERTY string(32)
  , MD5_HUB_AE_BASELEASE string(32)
  , MD5_HUB_AE_PRIORLEASE string(32)
  , MD5_HUB_AE_NEXTLEASE string(32)
  , LEASE_ID integer
  , PROPERTY_ID integer
  , BASELEASE_ID integer DEFAULT -1
  , PRIORLEASE_ID integer DEFAULT -1
  , NEXTLEASE_ID integer DEFAULT -1
  , LDTS timestamp_ltz
  , RSRC string(100)
); -- TODO requires creation of dummy lease with ID -1

CREATE TABLE IF NOT EXISTS LINK_EXT_PROPERTY_EXT_LEASE (
    MD5_LINK_EXT_PROPERTY_EXT_LEASE string(32)
  , MD5_HUB_EXTERNAL_PROPERTY string(32)
  , MD5_HUB_EXTERNAL_LEASE string(32)
  , EXTERNAL_PROPERTY_ID string(255)
  , EXTERNAL_LEASE_ID string(255)
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS LINK_AE_LEASE_EXT_LEASE (
    MD5_LINK_AE_LEASE_EXT_LEASE string(32)
  , MD5_HUB_AE_LEASE string(32)
  , MD5_HUB_EXTERNAL_LEASE string(32)
  , LEASE_ID integer
  , PROPERTY_ID integer
  , EXTERNAL_LEASE_ID string(255)
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS LINK_AE_LOAN_PROPERTY (
    MD5_LINK_AE_LOAN_PROPERTY string (32)
  , MD5_HUB_AE_LOAN string(32)
  , MD5_HUB_AE_PROPERTY string(32)
  , LOAN_ID integer
  , PROPERTY_ID integer
  , LDTS timestamp_ltz
  , RSRC string(100)  
);

/* *********************************************************************************** */
/* *** PITs ************************************************************************** */
/* *********************************************************************************** */

CREATE TABLE IF NOT EXISTS PIT_AE_PROPERTY (
    MD5_HUB_AE_PROPERTY string(32)
  , PROPERTY_VERSION integer
  , LDTS_SAT_AE_METADATA_PROPERTY timestamp_ltz
  , LDTS_SAT_AE_PROPERTY_CASHFLOW timestamp_ltz
  , LDTS_SAT_AE_PROPERTY_DETAILS timestamp_ltz
  , LDTS_SAT_AE_PROPERTY_KPI timestamp_ltz
  , LDTS_SAT_AE_PROPERTY_LOCATION timestamp_ltz
  , LDTS_SAT_AE_PROPERTY_PAYMENT timestamp_ltz
  , LDTS timestamp_ltz  
);

CREATE TABLE IF NOT EXISTS PIT_AE_LEASE (
    MD5_HUB_AE_LEASE string(32)
  , PROPERTY_VERSION integer
  , LDTS_SAT_AE_LEASE_CASHFLOW timestamp_ltz
  , LDTS_SAT_AE_LEASE_DETAILS timestamp_ltz
  , LDTS_SAT_AE_LEASE_PAYMENT timestamp_ltz 
  , LDTS timestamp_ltz  
);

CREATE TABLE IF NOT EXISTS PIT_AE_LOAN (
    MD5_HUB_AE_LOAN string(32)
  , PROPERTY_VERSION integer
  , LDTS_SAT_AE_LOAN_DETAILS timestamp_ltz
  , LDTS_SAT_AE_LOAN_PAYMENT timestamp_ltz 
  , LDTS timestamp_ltz  
);

CREATE TABLE IF NOT EXISTS PIT_AE_REVEX (
    MD5_HUB_AE_REVEX string(32)
  , PROPERTY_VERSION integer
  , LDTS_SAT_AE_REVEX_CASHFLOW timestamp_ltz
  , LDTS_SAT_AE_REVEX_DETAILS timestamp_ltz
  , LDTS_SAT_AE_REVEX_PAYMENT timestamp_ltz 
  , LDTS timestamp_ltz  
);

/* *********************************************************************************** */
/* *** Argus Taliance **************************************************************** */
/* *********************************************************************************** */

/* *********************************************************************************** */
/* *** HUBs ************************************************************************** */
/* *********************************************************************************** */

CREATE TABLE IF NOT EXISTS HUB_AT_COMMENT_TYPE (
    MD5_HUB_AT_COMMENT_TYPE string(32)
  , SCENARIO_ID integer
  , COMMENT_TYPE_ID integer
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS HUB_AT_DATA_SET (
    MD5_HUB_AT_DATA_SET string(32)
  , SCENARIO_ID integer
  , DATA_SET_ID integer
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS HUB_AT_DIMENSION (
    MD5_HUB_AT_DIMENSION string(32)
  , SCENARIO_ID integer
  , DIMENSION_ID integer
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS HUB_AT_ENTITY (
    MD5_HUB_AT_ENTITY string(32)
  , SCENARIO_ID integer
  , ENTITY_ID integer
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS HUB_AT_ENTITY_TYPE (
    MD5_HUB_AT_ENTITY_TYPE string(32)
  , SCENARIO_ID integer
  , ENTITY_TYPE_ID integer
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS HUB_AT_LINE_ITEM (
    MD5_HUB_AT_LINE_ITEM string(32)
  , SCENARIO_ID integer
  , LINE_ITEM_ID integer
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS HUB_AT_RELATIONSHIP_TYPE (
    MD5_HUB_AT_RELATIONSHIP_TYPE string(32)
  , SCENARIO_ID integer
  , RELATIONSHIP_TYPE_ID integer
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS HUB_AT_SCENARIO (
    MD5_HUB_AT_SCENARIO string(32)
  , SCENARIO_ID integer
  , LDTS timestamp_ltz
  , RSRC string(100)
);


/* *********************************************************************************** */
/* *** SATs ************************************************************************** */
/* *********************************************************************************** */

CREATE TABLE IF NOT EXISTS SAT_AT_COMMENT_DETAILS (
    MD5_LINK_AT_COMMENT_ENTITY string(32)
  , HASH_DIFF string(32)
  , COMMENT_LABEL string
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS SAT_AT_COMMENT_TYPE_DETAILS (
    MD5_HUB_AT_COMMENT_TYPE string(32)
  , HASH_DIFF string(32)
  , COMMENT_TYPE_CODE string(50)
  , COMMENT_TYPE_LABEL string
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS SAT_AT_DATA_SET_DETAILS (
    MD5_HUB_AT_DATA_SET string(32)
  , HASH_DIFF string(32)
  , DATA_SET_CODE string(50)
  , DATA_SET_LABEL string
  , DATA_SET_OPERATION_MODEL integer
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS SAT_AT_DIMENSION_DETAILS (
    MD5_HUB_AT_DIMENSION string(32)
  , HASH_DIFF string(32)
  , DIMENSION_CODE string(50)
  , DIMENSION_LABEL string
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS SAT_AT_ENTITY_DETAILS (
    MD5_HUB_AT_ENTITY string(32)
  , HASH_DIFF string(32)
  , ENTITY_CODE string(50)
  , ENTITY_LABEL string
  , BEGIN_DATE date
  , END_DATE date
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS SAT_AT_ENTITY_TYPE_DETAILS (
    MD5_HUB_AT_ENTITY_TYPE string(32)
  , HASH_DIFF string(32)
  , ENTITY_TYPE_CODE string(50)
  , ENTITY_TYPE_LABEL string
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS SAT_AT_LINE_ITEM_DETAILS (
    MD5_HUB_AT_LINE_ITEM string(32)
  , HASH_DIFF string(32)
  , LINE_ITEM_CODE string(50)
  , LINE_ITEM_LABEL string
  , LINE_ITEM_CATEGORY_CODE string
  , LINE_ITEM_TYPE string(50)
  , LINE_ITEM_CHARACTERISTIC boolean
  , LINE_ITEM_STOCK_FLOW string(5)
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS SAT_AT_RELATIONSHIP_DETAILS (
    MD5_LINK_AT_RELATIONSHIP string(32)
  , HASH_DIFF string(32)
  , BEGIN_DATE date
  , END_DATE date
  , PATH string
  , CALCULATION string
  , RATIO decimal(13,10)
  , DIRECT_RELATIONSHIP boolean
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS SAT_AT_RELATIONSHIP_TYPE_DETAILS (
    MD5_HUB_AT_RELATIONSHIP_TYPE string(32)
  , HASH_DIFF string(32)
  , RELATIONSHIP_TYPE_CODE string(50)
  , RELATIONSHIP_TYPE_LABEL string
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS SAT_AT_SCENARIO_DETAILS (
    MD5_HUB_AT_SCENARIO string(32)
  , HASH_DIFF string(32)
  , SCENARIO_CODE string(50)
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS SAT_AT_FACT_DETAILS (
    MD5_LINK_AT_FACT string(32)
  , HASH_DIFF string(32)
  , PERIODICITY string(1)
  , FACT_TYPE string(50)
  , VALUE string
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS SAT_AT_DATA_SET_LINE_ITEM_DETAILS (
    MD5_LINK_AT_DATA_SET_LINE_ITEM string(32)
  , HASH_DIFF string(32)
  , NORDER integer
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS SAT_AT_METADATA_ENTITY (
    MD5_HUB_AT_ENTITY string(32)
  , HASH_DIFF string(32)
  , AE_CODE string(50)
  , AE_SCENARIO_ID integer
  , AE_PROPERTY_VERSION integer
  , AT_CURRENCY string(3)
  , ENTITY_LAST_MODIFIED_BY string(255)
  , ENTITY_LAST_MODIFIED_DATE timestamp_ltz
  , LDTS timestamp_ltz
  , RSRC string(100)
);

/* *********************************************************************************** */
/* *** LINKs ************************************************************************* */
/* *********************************************************************************** */

CREATE TABLE IF NOT EXISTS LINK_AT_COMMENT_ENTITY (
    MD5_LINK_AT_COMMENT_ENTITY string(32)
  , MD5_HUB_AT_ENTITY string(32)
  , MD5_HUB_AT_COMMENT_TYPE string(32)
  , SCENARIO_ID integer
  , ENTITY_ID integer
  , COMMENT_TYPE_ID integer
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS LINK_AT_DATA_SET_LINE_ITEM (
    MD5_LINK_AT_DATA_SET_LINE_ITEM string(32)
  , MD5_HUB_AT_DATA_SET string(32)
  , MD5_HUB_AT_LINE_ITEM string(32)
  , SCENARIO_ID integer
  , DATA_SET_ID integer
  , LINE_ITEM_ID integer
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS LINK_AT_ENTITY_EXT_PROPERTY (
    MD5_LINK_AT_ENTITY_EXT_PROPERTY string(32)
  , MD5_HUB_AT_ENTITY string(32)
  , MD5_HUB_EXTERNAL_PROPERTY string(32)
  , SCENARIO_ID integer
  , ENTITY_ID integer
  , EXTERNAL_PROPERTY_ID string(255)
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS LINK_AT_ENTITY_ENTITY_TYPE (
    MD5_LINK_AT_ENTITY_ENTITY_TYPE string(32)
  , MD5_HUB_AT_ENTITY string(32)
  , MD5_HUB_AT_ENTITY_TYPE string(32)
  , SCENARIO_ID integer
  , ENTITY_ID integer
  , ENTITY_TYPE_ID integer
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS LINK_AT_FACT (
    MD5_LINK_AT_FACT string(32)
  , MD5_HUB_AT_SCENARIO string(32)
  , MD5_HUB_AT_ENTITY string(32)
  , MD5_HUB_AT_LINE_ITEM string(32)
  , MD5_HUB_AT_DATE string(32)
  , MD5_HUB_AT_DIMENSION string(32)
  , SCENARIO_ID integer
  , ENTITY_ID integer
  , LINE_ITEM_ID integer
  , DIMENSION_ID integer
  , DATE date
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS LINK_AT_RELATIONSHIP (
    MD5_LINK_AT_RELATIONSHIP string(32)
  , MD5_HUB_AT_SCENARIO string(32)
  , MD5_HUB_AT_RELATIONSHIP_TYPE string(32)
  , MD5_HUB_AT_PARENT_ENTITY string(32)
  , MD5_HUB_AT_CHILD_ENTITY string(32)
  , SCENARIO_ID integer
  , RELATIONSHIP_TYPE_ID integer
  , PARENT_ENTITY_ID integer
  , CHILD_ENTITY_ID integer
  , LDTS timestamp_ltz
  , RSRC string(100)
);

/* *********************************************************************************** */
/* *** Cross-system ****************************************************************** */
/* *********************************************************************************** */

/* *********************************************************************************** */
/* *** HUBs ************************************************************************** */
/* *********************************************************************************** */

CREATE TABLE IF NOT EXISTS HUB_EXTERNAL_PROPERTY (
    MD5_HUB_EXTERNAL_PROPERTY string(32)
  , EXTERNAL_PROPERTY_ID string(255)
  , LDTS timestamp_ltz
  , RSRC string(100)
);

CREATE TABLE IF NOT EXISTS HUB_EXTERNAL_LEASE (
    MD5_HUB_EXTERNAL_LEASE string(32)
  , EXTERNAL_LEASE_ID string(255)
  , LDTS timestamp_ltz
  , RSRC string(100)
);

/* *********************************************************************************** */
/* *** SATs ************************************************************************** */
/* *********************************************************************************** */

CREATE TABLE IF NOT EXISTS SAT_PLATFORM_SCENARIO_DETAILS (
    MD5_LINK_PLATFORM_SCENARIO string(32)
  , HASH_DIFF string(32)
  , PLATFORM_SCENARIO_NAME string(520)
  , AT_SCENARIO_NAME string(255)
  , AE_SCENARIO_NAME string(255)
  , LDTS timestamp_ltz
  , RSRC string(100)
);

/* *********************************************************************************** */
/* *** LINKs ************************************************************************* */
/* *********************************************************************************** */

CREATE TABLE IF NOT EXISTS LINK_PLATFORM_SCENARIO (
    MD5_LINK_PLATFORM_SCENARIO string(32)
  , MD5_HUB_AT_SCENARIO string(32)
  , MD5_HUB_AE_SCENARIO string(32)
  , AT_SCENARIO_ID integer
  , AE_SCENARIO_ID integer
  , LDTS timestamp_ltz
  , RSRC string(100)
);

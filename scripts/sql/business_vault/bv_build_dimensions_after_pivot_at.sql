/* *********************************************************************************** */
/* *** ARGUS Taliance **************************************************************** */
/* *********************************************************************************** */

/* create AT entity dimension including all the pivoted characteristics */
CREATE OR REPLACE TABLE BUSINESS_VAULT.DIMENSION_ENTITY_AT AS
SELECT 
    e.MD5_HUB_AT_ENTITY AS PK_DIMENSION_ENTITY_AT
  , e.ENTITY
  , e.ENTITY_TYPE
  , e.BEGIN_DATE
  , e.END_DATE
  , m.AT_CURRENCY AS LOCAL_CURRENCY
  , p.*
FROM DATA_VAULT.TRANSIENT_AT_DIMENSION_ENTITY e
LEFT JOIN DATA_VAULT.SAT_AT_METADATA_ENTITY m
  ON e.MD5_HUB_AT_ENTITY = m.MD5_HUB_AT_ENTITY
LEFT JOIN DATA_VAULT.TRANSIENT_AT_DIMENSION_CHARACTERISTIC_PIVOTED p
  ON e.MD5_HUB_AT_ENTITY = p.ENTITY_AT_KEY
;

/* drop columns not to be shown to customers */
ALTER TABLE BUSINESS_VAULT.DIMENSION_ENTITY_AT DROP COLUMN ENTITY_AT_KEY;

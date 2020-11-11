/* *********************************************************************************** */
/* *** VARIABLES ********************************************************************* */
/* *********************************************************************************** */

ALTER SESSION SET TIMEZONE = 'UTC';
SET LoadDTS = CURRENT_TIMESTAMP;

-- Temporary variables required where source files are not currently providing data
-- Change these AS required for testing
SET AEPropertyLastModifiedBy = 'user@example.com';
SET AEPropertyLastModifiedDate = $LoadDTS;

/* *********************************************************************************** */
/* *** POPULATE HUBS AND THEIR SATELLITES ******************************************** */
/* *********************************************************************************** */

INSERT ALL
  -- HUB_AT_COMMENT_TYPE
  WHEN (SELECT COUNT(*) FROM HUB_AT_COMMENT_TYPE WHERE MD5_HUB_AT_COMMENT_TYPE = MD5_AT_CommentTypeId) = 0
  THEN
    INTO HUB_AT_COMMENT_TYPE (MD5_HUB_AT_COMMENT_TYPE, SCENARIO_ID, COMMENT_TYPE_ID, LDTS, RSRC)
      VALUES (MD5_AT_CommentTypeId, SCENARIO_ID, COMMENT_TYPE_ID, LoadDTS, StageRecSrc)
SELECT DISTINCT
    MD5(UPPER(SCENARIO_ID || '^' || COMMENT_TYPE_ID)) AS MD5_AT_CommentTypeId
  , SCENARIO_ID
  , COMMENT_TYPE_ID
  , LoadDTS
  , StageRecSrc
FROM transient_at_comment_type
WHERE SCENARIO_ID IS NOT NULL AND COMMENT_TYPE_ID IS NOT NULL
;

INSERT ALL
  -- SAT_AT_COMMENT_TYPE_DETAILS
  WHEN (SELECT COUNT(*) FROM SAT_AT_COMMENT_TYPE_DETAILS SD WHERE SD.MD5_HUB_AT_COMMENT_TYPE = MD5_AT_CommentTypeId AND SD.HASH_DIFF = MD5_CommentTypeDetailsHashDiff) = 0
  THEN
    INTO SAT_AT_COMMENT_TYPE_DETAILS (MD5_HUB_AT_COMMENT_TYPE, HASH_DIFF, COMMENT_TYPE_CODE, COMMENT_TYPE_LABEL, LDTS, RSRC)
    VALUES (MD5_AT_CommentTypeId, MD5_CommentTypeDetailsHashDiff, COMMENT_TYPE_CODE, COMMENT_TYPE_LABEL, LoadDTS, StageRecSrc)
SELECT DISTINCT
    MD5(UPPER(SCENARIO_ID || '^' || COMMENT_TYPE_ID)) AS MD5_AT_CommentTypeId
  , MD5(UPPER(COALESCE(TO_VARCHAR(COMMENT_TYPE_CODE), '')
    || '^' || COALESCE(TO_VARCHAR(COMMENT_TYPE_LABEL), '')
    )) AS MD5_CommentTypeDetailsHashDiff
  , COMMENT_TYPE_CODE
  , COMMENT_TYPE_LABEL
  , LoadDTS
  , StageRecSrc
FROM transient_at_comment_type
WHERE COMMENT_TYPE_ID IS NOT NULL
;

INSERT ALL
  -- HUB_AT_DATA_SET
  WHEN (SELECT COUNT(*) FROM HUB_AT_DATA_SET WHERE MD5_HUB_AT_DATA_SET = MD5_AT_DataSetId) = 0
  THEN
    INTO HUB_AT_DATA_SET (MD5_HUB_AT_DATA_SET, SCENARIO_ID, DATA_SET_ID, LDTS, RSRC)
      VALUES (MD5_AT_DataSetId, SCENARIO_ID, DATA_SET_ID, LoadDTS, StageRecSrc)
SELECT DISTINCT
    MD5(UPPER(SCENARIO_ID || '^' || DATA_SET_ID)) AS MD5_AT_DataSetId
  , SCENARIO_ID
  , DATA_SET_ID
  , LoadDTS
  , StageRecSrc
FROM transient_at_data_set
WHERE SCENARIO_ID IS NOT NULL AND DATA_SET_ID IS NOT NULL
;

INSERT ALL
  -- SAT_AT_DATA_SET_DETAILS
  WHEN (SELECT COUNT(*) FROM SAT_AT_DATA_SET_DETAILS SD WHERE SD.MD5_HUB_AT_DATA_SET = MD5_AT_DataSetId AND SD.HASH_DIFF = MD5_DataSetDetailsHashDiff) = 0
  THEN
    INTO SAT_AT_DATA_SET_DETAILS (MD5_HUB_AT_DATA_SET, HASH_DIFF, DATA_SET_CODE, DATA_SET_LABEL, DATA_SET_OPERATION_MODEL, LDTS, RSRC)
    VALUES (MD5_AT_DataSetId, MD5_DataSetDetailsHashDiff, DATA_SET_CODE, DATA_SET_LABEL, DATA_SET_OPERATION_MODEL, LoadDTS, StageRecSrc)
SELECT DISTINCT
    MD5(UPPER(SCENARIO_ID || '^' || DATA_SET_ID)) AS MD5_AT_DataSetId
  , MD5(UPPER(COALESCE(TO_VARCHAR(DATA_SET_CODE), '')
    || '^' || COALESCE(TO_VARCHAR(DATA_SET_LABEL), '')
    || '^' || COALESCE(TO_VARCHAR(DATA_SET_OPERATION_MODEL), '')
    )) AS MD5_DataSetDetailsHashDiff
  , DATA_SET_CODE
  , DATA_SET_LABEL
  , DATA_SET_OPERATION_MODEL
  , LoadDTS
  , StageRecSrc
FROM transient_at_data_set
WHERE SCENARIO_ID IS NOT NULL AND DATA_SET_ID IS NOT NULL
;

INSERT ALL
  -- HUB_AT_DIMENSION
  WHEN (SELECT COUNT(*) FROM HUB_AT_DIMENSION WHERE MD5_HUB_AT_DIMENSION = MD5_AT_DimensionId) = 0
  THEN
    INTO HUB_AT_DIMENSION (MD5_HUB_AT_DIMENSION, SCENARIO_ID, DIMENSION_ID, LDTS, RSRC)
    VALUES (MD5_AT_DimensionId, SCENARIO_ID, DIMENSION_ID, LoadDTS, StageRecSrc)
SELECT DISTINCT
    MD5(UPPER(SCENARIO_ID || '^' || DIMENSION_ID)) AS MD5_AT_DimensionId
  , SCENARIO_ID
  , DIMENSION_ID
  , MD5(UPPER(COALESCE(TO_VARCHAR(DIMENSION_CODE), '')
     || '^' || COALESCE(TO_VARCHAR(DIMENSION_LABEL), '')
    )) AS MD5_DimensionDetailsHashDiff
  , LoadDTS
  , StageRecSrc
FROM transient_at_dimension
WHERE SCENARIO_ID IS NOT NULL AND DIMENSION_ID IS NOT NULL
;

INSERT ALL
  -- SAT_AT_DIMENSION_DETAILS
  WHEN (SELECT COUNT(*) FROM SAT_AT_DIMENSION_DETAILS SD WHERE SD.MD5_HUB_AT_DIMENSION = MD5_AT_DimensionId AND SD.HASH_DIFF = MD5_DimensionDetailsHashDiff) = 0
  THEN
    INTO SAT_AT_DIMENSION_DETAILS (MD5_HUB_AT_DIMENSION, HASH_DIFF, DIMENSION_CODE, DIMENSION_LABEL, LDTS, RSRC)
    VALUES (MD5_AT_DimensionId, MD5_DimensionDetailsHashDiff, DIMENSION_CODE, DIMENSION_LABEL, LoadDTS, StageRecSrc)
SELECT DISTINCT
    MD5(UPPER(SCENARIO_ID || '^' || DIMENSION_ID)) AS MD5_AT_DimensionId
  , MD5(UPPER(COALESCE(TO_VARCHAR(DIMENSION_CODE), '')
     || '^' || COALESCE(TO_VARCHAR(DIMENSION_LABEL), '')
    )) AS MD5_DimensionDetailsHashDiff
  , DIMENSION_CODE
  , DIMENSION_LABEL
  , LoadDTS
  , StageRecSrc
FROM transient_at_dimension
WHERE SCENARIO_ID IS NOT NULL AND DIMENSION_ID IS NOT NULL
;

INSERT ALL
  -- HUB_AT_ENTITY
  WHEN (SELECT COUNT(*) FROM HUB_AT_ENTITY WHERE MD5_HUB_AT_ENTITY = MD5_AT_EntityId) = 0
  THEN
    INTO HUB_AT_ENTITY (MD5_HUB_AT_ENTITY, SCENARIO_ID, ENTITY_ID, LDTS, RSRC)
      VALUES (MD5_AT_EntityId, SCENARIO_ID, ENTITY_ID, LoadDTS, StageRecSrc)
SELECT DISTINCT
    MD5(UPPER(SCENARIO_ID || '^' || ENTITY_ID)) AS MD5_AT_EntityId
  , SCENARIO_ID
  , ENTITY_ID
  , LoadDTS
  , StageRecSrc
FROM transient_at_entity
WHERE SCENARIO_ID IS NOT NULL AND ENTITY_ID IS NOT NULL
;

INSERT ALL
  -- SAT_AT_ENTITY_DETAILS
  WHEN (SELECT COUNT(*) FROM SAT_AT_ENTITY_DETAILS WHERE MD5_HUB_AT_ENTITY = MD5_AT_EntityId AND HASH_DIFF = MD5_EntityDetailsHashDiff) = 0
  THEN
    INTO SAT_AT_ENTITY_DETAILS (MD5_HUB_AT_ENTITY
                              , HASH_DIFF
                              , ENTITY_CODE
                              , ENTITY_LABEL
                              , BEGIN_DATE
                              , END_DATE
                              , LDTS
                              , RSRC)
    VALUES (MD5_AT_EntityId
          , MD5_EntityDetailsHashDiff
          , ENTITY_CODE
          , ENTITY_LABEL
          , BEGIN_DATE
          , END_DATE
          , LoadDTS
          , StageRecSrc)
SELECT DISTINCT
    MD5(UPPER(SCENARIO_ID || '^' || ENTITY_ID)) AS MD5_AT_EntityId
  , MD5(UPPER(COALESCE(TO_VARCHAR(ENTITY_CODE), '')
    || '^' || COALESCE(TO_VARCHAR(ENTITY_LABEL), '')
    || '^' || COALESCE(TO_VARCHAR(BEGIN_DATE), '')
    || '^' || COALESCE(TO_VARCHAR(END_DATE), '')
    )) AS MD5_EntityDetailsHashDiff
  , ENTITY_CODE
  , ENTITY_LABEL
  , BEGIN_DATE
  , END_DATE
  , LoadDTS
  , StageRecSrc
FROM transient_at_entity
WHERE SCENARIO_ID IS NOT NULL AND ENTITY_ID IS NOT NULL
;

INSERT ALL
  -- HUB_AT_ENTITY_TYPE
  WHEN (SELECT COUNT(*) FROM HUB_AT_ENTITY_TYPE WHERE MD5_HUB_AT_ENTITY_TYPE = MD5_AT_EntityTypeId) = 0
  THEN
    INTO HUB_AT_ENTITY_TYPE (MD5_HUB_AT_ENTITY_TYPE, SCENARIO_ID, ENTITY_TYPE_ID, LDTS, RSRC)
      VALUES (MD5_AT_EntityTypeId, SCENARIO_ID, ENTITY_TYPE_ID, LoadDTS, StageRecSrc)
SELECT DISTINCT
    MD5(UPPER(SCENARIO_ID || '^' || ENTITY_TYPE_ID)) AS MD5_AT_EntityTypeId
  , SCENARIO_ID
  , ENTITY_TYPE_ID
  , LoadDTS
  , StageRecSrc
FROM transient_at_entity_type
WHERE SCENARIO_ID IS NOT NULL AND ENTITY_TYPE_ID IS NOT NULL
;

INSERT ALL
  -- SAT_AT_ENTITY_TYPE_DETAILS
  WHEN (SELECT COUNT(*) FROM SAT_AT_ENTITY_TYPE_DETAILS SD WHERE SD.MD5_HUB_AT_ENTITY_TYPE = MD5_AT_EntityTypeId AND SD.HASH_DIFF = MD5_EntityTypeDetailsHashDiff) = 0
  THEN
    INTO SAT_AT_ENTITY_TYPE_DETAILS (MD5_HUB_AT_ENTITY_TYPE, HASH_DIFF, ENTITY_TYPE_CODE, ENTITY_TYPE_LABEL, LDTS, RSRC)
    VALUES (MD5_AT_EntityTypeId, MD5_EntityTypeDetailsHashDiff, ENTITY_TYPE_CODE, ENTITY_TYPE_LABEL, LoadDTS, StageRecSrc)
SELECT DISTINCT
    MD5(UPPER(SCENARIO_ID || '^' || ENTITY_TYPE_ID)) AS MD5_AT_EntityTypeId
  , MD5(UPPER(COALESCE(TO_VARCHAR(ENTITY_TYPE_CODE), '')
    || '^' || COALESCE(TO_VARCHAR(ENTITY_TYPE_LABEL), '')
    )) AS MD5_EntityTypeDetailsHashDiff
  , ENTITY_TYPE_CODE
  , ENTITY_TYPE_LABEL
  , LoadDTS
  , StageRecSrc
FROM transient_at_entity_type
WHERE SCENARIO_ID IS NOT NULL AND ENTITY_TYPE_ID IS NOT NULL
;

INSERT ALL
  -- HUB_AT_LINE_ITEM
  WHEN (SELECT COUNT(*) FROM HUB_AT_LINE_ITEM WHERE MD5_HUB_AT_LINE_ITEM = MD5_AT_LineItemId) = 0
  THEN
    INTO HUB_AT_LINE_ITEM (MD5_HUB_AT_LINE_ITEM, SCENARIO_ID, LINE_ITEM_ID, LDTS, RSRC)
      VALUES (MD5_AT_LineItemId, SCENARIO_ID, LINE_ITEM_ID, LoadDTS, StageRecSrc)
SELECT DISTINCT
    MD5(UPPER(SCENARIO_ID || '^' || LINE_ITEM_ID)) AS MD5_AT_LineItemId
  , SCENARIO_ID
  , LINE_ITEM_ID
  , LoadDTS
  , StageRecSrc
FROM transient_at_line_item
WHERE SCENARIO_ID IS NOT NULL AND LINE_ITEM_ID IS NOT NULL
;

INSERT ALL
  -- SAT_AT_LINE_ITEM_DETAILS
  WHEN (SELECT COUNT(*) FROM SAT_AT_LINE_ITEM_DETAILS SD WHERE SD.MD5_HUB_AT_LINE_ITEM = MD5_AT_LineItemId AND SD.HASH_DIFF = MD5_LineItemDetailsHashDiff) = 0
  THEN
    INTO SAT_AT_LINE_ITEM_DETAILS (
        MD5_HUB_AT_LINE_ITEM
      , HASH_DIFF
      , LINE_ITEM_CODE
      , LINE_ITEM_LABEL
      , LINE_ITEM_CATEGORY_CODE
      , LINE_ITEM_TYPE
      , LINE_ITEM_CHARACTERISTIC
      , LINE_ITEM_STOCK_FLOW
      , LDTS
      , RSRC)
    VALUES (
        MD5_AT_LineItemId
      , MD5_LineItemDetailsHashDiff
      , LINE_ITEM_CODE
      , LINE_ITEM_LABEL
      , LINE_ITEM_CATEGORY_CODE
      , LINE_ITEM_TYPE
      , LINE_ITEM_CHARACTERISTIC
      , LINE_ITEM_STOCK_FLOW
      , LoadDTS
      , StageRecSrc)
SELECT DISTINCT
    MD5(UPPER(SCENARIO_ID || '^' || LINE_ITEM_ID)) AS MD5_AT_LineItemId
  , MD5(UPPER(COALESCE(TO_VARCHAR(LINE_ITEM_CODE), '')
    || '^' || COALESCE(TO_VARCHAR(LINE_ITEM_LABEL), '')
    || '^' || COALESCE(TO_VARCHAR(LINE_ITEM_CATEGORY_CODE), '')
    || '^' || COALESCE(TO_VARCHAR(LINE_ITEM_TYPE), '')
    || '^' || COALESCE(TO_VARCHAR(LINE_ITEM_CHARACTERISTIC), '')
    || '^' || COALESCE(TO_VARCHAR(LINE_ITEM_STOCK_FLOW), '')
    )) AS MD5_LineItemDetailsHashDiff
  , LINE_ITEM_CODE
  , LINE_ITEM_LABEL
  , LINE_ITEM_CATEGORY_CODE
  , LINE_ITEM_TYPE
  , LINE_ITEM_CHARACTERISTIC
  , LINE_ITEM_STOCK_FLOW
  , LoadDTS
  , StageRecSrc
FROM transient_at_line_item
WHERE SCENARIO_ID IS NOT NULL AND LINE_ITEM_ID IS NOT NULL
;

INSERT ALL
  -- HUB_AT_RELATIONSHIP_TYPE
  WHEN (SELECT COUNT(*) FROM HUB_AT_RELATIONSHIP_TYPE WHERE MD5_HUB_AT_RELATIONSHIP_TYPE = MD5_AT_RelationshipTypeId) = 0
  THEN
    INTO HUB_AT_RELATIONSHIP_TYPE (MD5_HUB_AT_RELATIONSHIP_TYPE, SCENARIO_ID, RELATIONSHIP_TYPE_ID, LDTS, RSRC)
      VALUES (MD5_AT_RelationshipTypeId, SCENARIO_ID, RELATIONSHIP_TYPE_ID, LoadDTS, StageRecSrc)
SELECT DISTINCT
    MD5(UPPER(SCENARIO_ID || '^' || RELATIONSHIP_TYPE_ID)) AS MD5_AT_RelationshipTypeId
  , SCENARIO_ID
  , RELATIONSHIP_TYPE_ID
  , LoadDTS
  , StageRecSrc
FROM transient_at_relationship_type
WHERE SCENARIO_ID IS NOT NULL AND RELATIONSHIP_TYPE_ID IS NOT NULL
;

INSERT ALL
  -- SAT_AT_RELATIONSHIP_TYPE_DETAILS
  WHEN (SELECT COUNT(*) FROM SAT_AT_RELATIONSHIP_TYPE_DETAILS SD WHERE SD.MD5_HUB_AT_RELATIONSHIP_TYPE = MD5_AT_RelationshipTypeId AND SD.HASH_DIFF = MD5_RelationshipTypeDetailsHashDiff) = 0
  THEN
    INTO SAT_AT_RELATIONSHIP_TYPE_DETAILS (MD5_HUB_AT_RELATIONSHIP_TYPE, HASH_DIFF, RELATIONSHIP_TYPE_CODE, RELATIONSHIP_TYPE_LABEL, LDTS, RSRC)
    VALUES (MD5_AT_RelationshipTypeId, MD5_RelationshipTypeDetailsHashDiff, RELATIONSHIP_TYPE_CODE, RELATIONSHIP_TYPE_LABEL, LoadDTS, StageRecSrc)
SELECT DISTINCT
    MD5(UPPER(SCENARIO_ID || '^' || RELATIONSHIP_TYPE_ID)) AS MD5_AT_RelationshipTypeId
  , MD5(UPPER(COALESCE(TO_VARCHAR(RELATIONSHIP_TYPE_CODE), '')
    || '^' || COALESCE(TO_VARCHAR(RELATIONSHIP_TYPE_LABEL), '')
  )) AS MD5_RelationshipTypeDetailsHashDiff
  , RELATIONSHIP_TYPE_CODE
  , RELATIONSHIP_TYPE_LABEL
  , LoadDTS
  , StageRecSrc
FROM transient_at_relationship_type
WHERE SCENARIO_ID IS NOT NULL AND RELATIONSHIP_TYPE_ID IS NOT NULL
;

INSERT ALL
  -- HUB_AT_SCENARIO
  WHEN (SELECT COUNT(*) FROM HUB_AT_SCENARIO WHERE MD5_HUB_AT_SCENARIO = MD5_AT_ScenarioId) = 0
  THEN
    INTO HUB_AT_SCENARIO (MD5_HUB_AT_SCENARIO, SCENARIO_ID, LDTS, RSRC)
      VALUES (MD5_AT_ScenarioId, SCENARIO_ID, LoadDTS, StageRecSrc)
SELECT DISTINCT
    MD5(UPPER(SCENARIO_ID)) AS MD5_AT_ScenarioId
  , SCENARIO_ID
  , LoadDTS
  , StageRecSrc
FROM transient_at_scenario
WHERE SCENARIO_ID IS NOT NULL
;

INSERT ALL
  -- SAT_AT_SCENARIO_DETAILS
  WHEN (SELECT COUNT(*) FROM SAT_AT_SCENARIO_DETAILS SD WHERE SD.MD5_HUB_AT_SCENARIO = MD5_AT_ScenarioId AND SD.HASH_DIFF = MD5_ScenarioDetailsHashDiff) = 0
  THEN
    INTO SAT_AT_SCENARIO_DETAILS (MD5_HUB_AT_SCENARIO, HASH_DIFF, SCENARIO_CODE, LDTS, RSRC)
    VALUES (MD5_AT_ScenarioId, MD5_ScenarioDetailsHashDiff, SCENARIO_CODE, LoadDTS, StageRecSrc)
SELECT DISTINCT
    MD5(UPPER(SCENARIO_ID)) AS MD5_AT_ScenarioId
  , MD5(UPPER(COALESCE(TO_VARCHAR(SCENARIO_CODE), ''))) AS MD5_ScenarioDetailsHashDiff
  , SCENARIO_CODE
  , LoadDTS
  , StageRecSrc
FROM transient_at_scenario
WHERE SCENARIO_ID IS NOT NULL
;

/* *********************************************************************************** */
/* *** POPULATE EXTERNAL HUBS AND THEIR LINKS **************************************** */
/* *********************************************************************************** */

-- Load entity data that have external ID populated
INSERT ALL
  -- HUB_EXTERNAL_PROPERTY
  WHEN (SELECT COUNT(*) FROM HUB_EXTERNAL_PROPERTY HEP WHERE HEP.MD5_HUB_EXTERNAL_PROPERTY = MD5_ExternalPropertyId) = 0
  THEN
    INTO HUB_EXTERNAL_PROPERTY (MD5_HUB_EXTERNAL_PROPERTY, EXTERNAL_PROPERTY_ID, LDTS, RSRC)
    VALUES (MD5_ExternalPropertyId, AE_EXTERNAL_ID, LoadDTS, StageRecSrc)
SELECT DISTINCT
    MD5(UPPER(AE_EXTERNAL_ID)) AS MD5_ExternalPropertyId
  , AE_EXTERNAL_ID
  , LoadDTS
  , StageRecSrc
FROM transient_at_entity
WHERE AE_EXTERNAL_ID IS NOT NULL
;

INSERT ALL
  -- LINK_AT_ENTITY_EXT_PROPERTY
  WHEN (SELECT COUNT(*) FROM LINK_AT_ENTITY_EXT_PROPERTY LEEP
       WHERE LEEP.MD5_LINK_AT_ENTITY_EXT_PROPERTY = MD5_AT_LinkEntityExtProperty) = 0
  THEN
    INTO LINK_AT_ENTITY_EXT_PROPERTY (MD5_LINK_AT_ENTITY_EXT_PROPERTY, MD5_HUB_AT_ENTITY, MD5_HUB_EXTERNAL_PROPERTY, SCENARIO_ID, ENTITY_ID, EXTERNAL_PROPERTY_ID, LDTS, RSRC)
    VALUES (MD5_AT_LinkEntityExtProperty, MD5_AT_EntityId, MD5_ExternalPropertyId, SCENARIO_ID, ENTITY_ID, AE_EXTERNAL_ID, LoadDTS, StageRecSrc)
SELECT DISTINCT
    MD5(UPPER(SCENARIO_ID || '^' || ENTITY_ID || '^' || AE_EXTERNAL_ID)) AS MD5_AT_LinkEntityExtProperty
  , MD5(UPPER(SCENARIO_ID || '^' || ENTITY_ID)) AS MD5_AT_EntityId
  , MD5(UPPER(AE_EXTERNAL_ID)) AS MD5_ExternalPropertyId
  , SCENARIO_ID
  , ENTITY_ID
  , AE_EXTERNAL_ID
  , LoadDTS
  , StageRecSrc
FROM transient_at_entity
WHERE SCENARIO_ID IS NOT NULL 
  AND ENTITY_ID IS NOT NULL
  AND AE_EXTERNAL_ID IS NOT NULL
;

/* *********************************************************************************** */
/* *** POPULATE LINKS AND THEIR SATELLITES ******************************************* */
/* *********************************************************************************** */

INSERT ALL
  -- LINK_AT_COMMENT_ENTITY
  WHEN (SELECT COUNT(*) FROM LINK_AT_COMMENT_ENTITY WHERE MD5_LINK_AT_COMMENT_ENTITY = MD5_AT_LinkCommentEntityScenario) = 0
  THEN
    INTO LINK_AT_COMMENT_ENTITY (MD5_LINK_AT_COMMENT_ENTITY
                                        , MD5_HUB_AT_ENTITY
                                        , MD5_HUB_AT_COMMENT_TYPE
                                        , SCENARIO_ID
                                        , ENTITY_ID
                                        , COMMENT_TYPE_ID
                                        , LDTS
                                        , RSRC)
    VALUES (MD5_AT_LinkCommentEntityScenario
          , MD5_AT_EntityId
          , MD5_AT_CommentTypeId
          , SCENARIO_ID
          , ENTITY_ID
          , COMMENT_TYPE_ID
          , LoadDTS
          , StageRecSrc)
SELECT DISTINCT
    MD5(UPPER(SCENARIO_ID || '^' || ENTITY_ID || '^' || COMMENT_TYPE_ID)) AS MD5_AT_LinkCommentEntityScenario
  , MD5(UPPER(SCENARIO_ID || '^' || ENTITY_ID)) AS MD5_AT_EntityId
  , MD5(UPPER(COMMENT_TYPE_ID)) AS MD5_AT_CommentTypeId
  , SCENARIO_ID
  , ENTITY_ID
  , COMMENT_TYPE_ID
  , LoadDTS
  , StageRecSrc
FROM transient_at_comment
WHERE SCENARIO_ID IS NOT NULL
  AND ENTITY_ID IS NOT NULL
  AND COMMENT_TYPE_ID IS NOT NULL
;

INSERT ALL
  -- SAT_AT_COMMENT_DETAILS
  WHEN (SELECT COUNT(*) FROM SAT_AT_COMMENT_DETAILS WHERE MD5_LINK_AT_COMMENT_ENTITY = MD5_AT_LinkCommentEntityScenario AND HASH_DIFF = MD5_CommentDetailsHashDiff) = 0
  THEN
    INTO SAT_AT_COMMENT_DETAILS (MD5_LINK_AT_COMMENT_ENTITY
                               , HASH_DIFF
                               , COMMENT_LABEL
                               , LDTS
                               , RSRC)
    VALUES (MD5_AT_LinkCommentEntityScenario
          , MD5_CommentDetailsHashDiff
          , COMMENT_LABEL
          , LoadDTS
          , StageRecSrc)
SELECT DISTINCT
    MD5(UPPER(SCENARIO_ID || '^' || ENTITY_ID || '^' || COMMENT_TYPE_ID)) AS MD5_AT_LinkCommentEntityScenario
  , MD5(UPPER(COALESCE(TO_VARCHAR(COMMENT_LABEL), ''))) AS MD5_CommentDetailsHashDiff
  , COMMENT_LABEL
  , LoadDTS
  , StageRecSrc
FROM transient_at_comment
WHERE SCENARIO_ID IS NOT NULL
  AND ENTITY_ID IS NOT NULL
  AND COMMENT_TYPE_ID IS NOT NULL
;

INSERT ALL
  -- LINK_AT_DATA_SET_LINE_ITEM
  WHEN (SELECT COUNT(*) FROM LINK_AT_DATA_SET_LINE_ITEM WHERE MD5_LINK_AT_DATA_SET_LINE_ITEM = MD5_AT_LinkDataSetLineItem) = 0
  THEN
    INTO LINK_AT_DATA_SET_LINE_ITEM (
        MD5_LINK_AT_DATA_SET_LINE_ITEM
      , MD5_HUB_AT_DATA_SET
      , MD5_HUB_AT_LINE_ITEM
      , DATA_SET_ID
      , LINE_ITEM_ID
      , LDTS
      , RSRC)
    VALUES (
        MD5_AT_LinkDataSetLineItem
      , MD5_AT_DataSetId
      , MD5_AT_LineItemId
      , DATA_SET_ID
      , LINE_ITEM_ID
      , LoadDTS
      , StageRecSrc)
SELECT DISTINCT
    MD5(UPPER(SCENARIO_ID || '^' || DATA_SET_ID || '^' || LINE_ITEM_ID)) AS MD5_AT_LinkDataSetLineItem
  , MD5(UPPER(SCENARIO_ID || '^' || DATA_SET_ID)) AS MD5_AT_DataSetId
  , MD5(UPPER(SCENARIO_ID || '^' || LINE_ITEM_ID)) AS MD5_AT_LineItemId
  , SCENARIO_ID
  , DATA_SET_ID
  , LINE_ITEM_ID
  , LoadDTS
  , StageRecSrc
FROM transient_at_detail_data_set
WHERE SCENARIO_ID IS NOT NULL
  AND DATA_SET_ID IS NOT NULL
  AND LINE_ITEM_ID IS NOT NULL
;

INSERT ALL
  -- SAT_AT_DATA_SET_LINE_ITEM_DETAILS
  WHEN (SELECT COUNT(*) FROM SAT_AT_DATA_SET_LINE_ITEM_DETAILS SD WHERE SD.MD5_LINK_AT_DATA_SET_LINE_ITEM = MD5_AT_LinkDataSetLineItem AND SD.HASH_DIFF = MD5_DataSetLineItemDetailsHashDiff) = 0
  THEN
    INTO SAT_AT_DATA_SET_LINE_ITEM_DETAILS (
        MD5_LINK_AT_DATA_SET_LINE_ITEM
      , HASH_DIFF
      , NORDER
      , LDTS
      , RSRC)
    VALUES (
        MD5_AT_LinkDataSetLineItem
      , MD5_DataSetLineItemDetailsHashDiff
      , NORDER
      , LoadDTS
      , StageRecSrc)
SELECT DISTINCT
    MD5(UPPER(SCENARIO_ID || '^' || DATA_SET_ID || '^' || LINE_ITEM_ID)) AS MD5_AT_LinkDataSetLineItem
  , MD5(UPPER(COALESCE(TO_VARCHAR(NORDER), ''))) AS MD5_DataSetLineItemDetailsHashDiff
  , NORDER
  , LoadDTS
  , StageRecSrc
FROM transient_at_detail_data_set
WHERE SCENARIO_ID IS NOT NULL
  AND DATA_SET_ID IS NOT NULL
  AND LINE_ITEM_ID IS NOT NULL
;

INSERT ALL
  -- LINK_AT_ENTITY_ENTITY_TYPE
  WHEN (SELECT COUNT(*) FROM LINK_AT_ENTITY_ENTITY_TYPE WHERE MD5_LINK_AT_ENTITY_ENTITY_TYPE = MD5_AT_LinkEntityEntityType) = 0
  THEN
    INTO LINK_AT_ENTITY_ENTITY_TYPE (
        MD5_LINK_AT_ENTITY_ENTITY_TYPE
      , MD5_HUB_AT_ENTITY
      , MD5_HUB_AT_ENTITY_TYPE
      , SCENARIO_ID
      , ENTITY_ID
      , ENTITY_TYPE_ID
      , LDTS
      , RSRC)
    VALUES (
        MD5_AT_LinkEntityEntityType
      , MD5_AT_EntityId
      , MD5_AT_EntityTypeId
      , SCENARIO_ID
      , ENTITY_ID
      , ENTITY_TYPE_ID
      , LoadDTS
      , StageRecSrc)
SELECT DISTINCT
    MD5(UPPER(SCENARIO_ID || '^' || ENTITY_ID || '^' || ENTITY_TYPE_ID)) AS MD5_AT_LinkEntityEntityType
  , MD5(UPPER(SCENARIO_ID || '^' || ENTITY_ID)) AS MD5_AT_EntityId
  , MD5(UPPER(SCENARIO_ID || '^' || ENTITY_TYPE_ID)) AS MD5_AT_EntityTypeId
  , SCENARIO_ID
  , ENTITY_ID
  , ENTITY_TYPE_ID
  , LoadDTS
  , StageRecSrc
FROM transient_at_entity
WHERE SCENARIO_ID IS NOT NULL
  AND ENTITY_ID IS NOT NULL
  AND ENTITY_TYPE_ID IS NOT NULL
;

INSERT ALL
  -- LINK_AT_RELATIONSHIP
  WHEN (SELECT COUNT(*) FROM LINK_AT_RELATIONSHIP WHERE MD5_LINK_AT_RELATIONSHIP = MD5_AT_LinkRelationship) = 0
  THEN
    INTO LINK_AT_RELATIONSHIP (
        MD5_LINK_AT_RELATIONSHIP
      , MD5_HUB_AT_SCENARIO
      , MD5_HUB_AT_RELATIONSHIP_TYPE
      , MD5_HUB_AT_PARENT_ENTITY
      , MD5_HUB_AT_CHILD_ENTITY
      , SCENARIO_ID
      , RELATIONSHIP_TYPE_ID
      , PARENT_ENTITY_ID
      , CHILD_ENTITY_ID
      , LDTS
      , RSRC)
    VALUES (
        MD5_AT_LinkRelationship
      , MD5_AT_ScenarioId
      , MD5_AT_RelationshipTypeId
      , MD5_AT_ParentEntityId
      , MD5_AT_ChildEntityId
      , SCENARIO_ID
      , RELATIONSHIP_TYPE_ID
      , PARENT_ENTITY_ID
      , CHILD_ENTITY_ID
      , LoadDTS
      , StageRecSrc)
SELECT DISTINCT
    MD5(UPPER(SCENARIO_ID || '^' || RELATIONSHIP_TYPE_ID || '^' || PARENT_ENTITY_ID || '^' || CHILD_ENTITY_ID)) AS MD5_AT_LinkRelationship
  , MD5(UPPER(SCENARIO_ID)) AS MD5_AT_ScenarioId
  , MD5(UPPER(SCENARIO_ID || '^' || RELATIONSHIP_TYPE_ID)) AS MD5_AT_RelationshipTypeId
  , MD5(UPPER(SCENARIO_ID || '^' || PARENT_ENTITY_ID)) AS MD5_AT_ParentEntityId
  , MD5(UPPER(SCENARIO_ID || '^' || CHILD_ENTITY_ID)) AS MD5_AT_ChildEntityId
  , SCENARIO_ID
  , RELATIONSHIP_TYPE_ID
  , PARENT_ENTITY_ID
  , CHILD_ENTITY_ID
  , LoadDTS
  , StageRecSrc
FROM transient_at_relationship
WHERE SCENARIO_ID IS NOT NULL
  AND RELATIONSHIP_TYPE_ID IS NOT NULL
  AND PARENT_ENTITY_ID IS NOT NULL
  AND CHILD_ENTITY_ID IS NOT NULL
;

INSERT ALL
  -- SAT_AT_RELATIONSHIP_DETAILS
  WHEN (SELECT COUNT(*) FROM SAT_AT_RELATIONSHIP_DETAILS SD WHERE SD.MD5_LINK_AT_RELATIONSHIP = MD5_AT_LinkRelationship AND SD.HASH_DIFF = MD5_RelationshipDetailsHashDiff) = 0
  THEN
    INTO SAT_AT_RELATIONSHIP_DETAILS (
        MD5_LINK_AT_RELATIONSHIP
      , HASH_DIFF
      , BEGIN_DATE
      , END_DATE
      , PATH
      , CALCULATION
      , RATIO
      , DIRECT_RELATIONSHIP
      , LDTS
      , RSRC)
    VALUES (
        MD5_AT_LinkRelationship
      , MD5_RelationshipDetailsHashDiff
      , START_DATE
      , END_DATE
      , PATH
      , CALCULATION
      , RATIO
      , DIRECT_RELATIONSHIP
      , LoadDTS
      , StageRecSrc)
SELECT DISTINCT
    MD5(UPPER(SCENARIO_ID || '^' || RELATIONSHIP_TYPE_ID || '^' || PARENT_ENTITY_ID || '^' || CHILD_ENTITY_ID)) AS MD5_AT_LinkRelationship
  , MD5(UPPER(COALESCE(TO_VARCHAR(START_DATE), '')
    || '^' || COALESCE(TO_VARCHAR(END_DATE), '')
    || '^' || COALESCE(TO_VARCHAR(PATH), '')
    || '^' || COALESCE(TO_VARCHAR(CALCULATION), '')
    || '^' || COALESCE(TO_VARCHAR(RATIO), '')
    || '^' || COALESCE(TO_VARCHAR(DIRECT_RELATIONSHIP), '')
    )) AS MD5_RelationshipDetailsHashDiff
  , START_DATE
  , END_DATE
  , PATH
  , CALCULATION
  , RATIO
  , DIRECT_RELATIONSHIP
  , LoadDTS
  , StageRecSrc
FROM transient_at_relationship
WHERE SCENARIO_ID IS NOT NULL
  AND RELATIONSHIP_TYPE_ID IS NOT NULL
  AND PARENT_ENTITY_ID IS NOT NULL
  AND CHILD_ENTITY_ID IS NOT NULL
;

INSERT ALL
  -- LINK_AT_FACT
  WHEN (SELECT COUNT(*) FROM LINK_AT_FACT WHERE MD5_LINK_AT_FACT = MD5_AT_LinkFact) = 0
  THEN
    INTO LINK_AT_FACT (
        MD5_LINK_AT_FACT
      , MD5_HUB_AT_SCENARIO
      , MD5_HUB_AT_ENTITY
      , MD5_HUB_AT_LINE_ITEM
      , MD5_HUB_AT_DATE
      , MD5_HUB_AT_DIMENSION
      , SCENARIO_ID
      , ENTITY_ID
      , LINE_ITEM_ID
	  , DATE
      , DIMENSION_ID
      , LDTS
      , RSRC)
    VALUES (
        MD5_AT_LinkFact
      , MD5_AT_ScenarioId
      , MD5_AT_EntityId
      , MD5_AT_LineItemId
      , MD5_AT_DateId
      , MD5_AT_DimensionId
      , SCENARIO_ID
      , ENTITY_ID
      , LINE_ITEM_ID
	  , DATE
      , DIMENSION_ID
      , LoadDTS
      , StageRecSrc)
SELECT DISTINCT
    MD5(UPPER(SCENARIO_ID
    || '^' || ENTITY_ID
    || '^' || LINE_ITEM_ID
    || '^' || COALESCE(TO_VARCHAR(DATE), '')
    || '^' || COALESCE(TO_VARCHAR(DIMENSION_ID), '')
    )) AS MD5_AT_LinkFact
  , MD5(UPPER(SCENARIO_ID)) AS MD5_AT_ScenarioId
  , MD5(UPPER(SCENARIO_ID || '^' || ENTITY_ID)) AS MD5_AT_EntityId
  , MD5(UPPER(SCENARIO_ID || '^' || LINE_ITEM_ID)) AS MD5_AT_LineItemId
  , MD5(UPPER(COALESCE(TO_VARCHAR(DATE), ''))) AS MD5_AT_DateId
  , MD5(UPPER(SCENARIO_ID || '^' || COALESCE(TO_VARCHAR(DIMENSION_ID), ''))) AS MD5_AT_DimensionId
  , SCENARIO_ID
  , ENTITY_ID
  , LINE_ITEM_ID
  , DATE
  , DIMENSION_ID
  , LoadDTS
  , StageRecSrc
FROM transient_at_fact
WHERE SCENARIO_ID IS NOT NULL
  AND ENTITY_ID IS NOT NULL
  AND LINE_ITEM_ID IS NOT NULL
;

INSERT ALL
  -- SAT_AT_FACT_DETAILS
  WHEN (SELECT COUNT(*) FROM SAT_AT_FACT_DETAILS SD WHERE SD.MD5_LINK_AT_FACT = MD5_AT_LinkFact AND SD.HASH_DIFF = MD5_FactDetailsHashDiff) = 0
  THEN
    INTO SAT_AT_FACT_DETAILS (
        MD5_LINK_AT_FACT
      , HASH_DIFF
      , PERIODICITY
      , FACT_TYPE
      , VALUE
      , LDTS
      , RSRC)
    VALUES (
        MD5_AT_LinkFact
      , MD5_FactDetailsHashDiff
      , PERIODICITY
      , FACT_TYPE
      , VALUE
      , LoadDTS
      , StageRecSrc)
SELECT DISTINCT
    MD5(UPPER(f.SCENARIO_ID
    || '^' || f.ENTITY_ID
    || '^' || f.LINE_ITEM_ID
    || '^' || COALESCE(TO_VARCHAR(f.DATE), '')
    || '^' || COALESCE(TO_VARCHAR(f.DIMENSION_ID), '')
    )) AS MD5_AT_LinkFact
  , MD5(UPPER(COALESCE(TO_VARCHAR(f.PERIODICITY), '')
    || '^' || COALESCE(TO_VARCHAR(f.VALUE), '')
    )) AS MD5_FactDetailsHashDiff
  , f.PERIODICITY
  , l.LINE_ITEM_TYPE AS FACT_TYPE
  , f.VALUE
  , f.LoadDTS
  , f.StageRecSrc
FROM transient_at_fact f
  INNER JOIN transient_at_line_item l ON f.LINE_ITEM_ID = l.LINE_ITEM_ID
WHERE f.SCENARIO_ID IS NOT NULL
  AND f.ENTITY_ID IS NOT NULL
  AND f.LINE_ITEM_ID IS NOT NULL
;

/* *********************************************************************************** */
/* *** METADATA ********************************************************************** */
/* *********************************************************************************** */

INSERT ALL
  -- Load AT entity data
  -- SAT_AT_METADATA_ENTITY
  WHEN (SELECT COUNT(*) FROM SAT_AT_METADATA_ENTITY WHERE MD5_HUB_AT_ENTITY = MD5_AT_EntityId AND HASH_DIFF = EntityMetadataHashDiff) = 0
  THEN
    INTO SAT_AT_METADATA_ENTITY (
        MD5_HUB_AT_ENTITY
      , HASH_DIFF
      , AE_CODE
      , AE_SCENARIO_ID
      , AE_PROPERTY_VERSION
      , AT_CURRENCY
      , ENTITY_LAST_MODIFIED_BY
      , ENTITY_LAST_MODIFIED_DATE
      , LDTS
      , RSRC)
    VALUES (
        MD5_AT_EntityId
      , EntityMetadataHashDiff
      , AE_EXTERNAL_ID
      , AEScenarioID
      , AEPropertyVersion
      , ATCurrency
      , AEPropertyLastModifiedBy
      , AEPropertyLastModifiedDate
      , LoadDTS
      , StageRecSrc)
    SELECT
        MD5_AT_EntityId
      , EntityMetadataHashDiff
      , AE_EXTERNAL_ID
      , AEPropertyLastModifiedBy
      , AEPropertyLastModifiedDate
      , LoadDTS
      , StageRecSrc
      , CASE
            WHEN "'Ch_AE_Scenario_ID'" LIKE 'SC%' THEN TRY_TO_NUMBER(RIGHT("'Ch_AE_Scenario_ID'", 5))
            ELSE TRY_TO_NUMBER("'Ch_AE_Scenario_ID'")
        END AS AEScenarioID -- FIXME: Horrible hack while the scenario ID is not an integer  
      , TRY_TO_NUMBER("'Ch_AE_Last_Changed_Version'") AS AEPropertyVersion
      , "'Ch_CurrencyCode'" AS ATCurrency
    FROM
      (
        SELECT DISTINCT
            MD5(UPPER(e.SCENARIO_ID || '^' || e.ENTITY_ID)) AS MD5_AT_EntityId
          , MD5(UPPER(COALESCE(TO_VARCHAR(e.AE_EXTERNAL_ID), '')
            || '^' || COALESCE(TO_VARCHAR($AEPropertyLastModifiedBy), '')
            || '^' || COALESCE(TO_VARCHAR($AEPropertyLastModifiedDate), '')
            )) AS EntityMetadataHashDiff
          , e.AE_EXTERNAL_ID
          , $AEPropertyLastModifiedBy AS AEPropertyLastModifiedBy
          , $AEPropertyLastModifiedDate AS AEPropertyLastModifiedDate
          , e.LoadDTS
          , e.StageRecSRC
          , LINE_ITEM_CODE
          , VALUE
        FROM
            transient_at_entity e
            INNER JOIN transient_at_fact f ON e.ENTITY_ID = f.ENTITY_ID
            INNER JOIN transient_at_line_item l ON f.LINE_ITEM_ID = l.LINE_ITEM_ID
        WHERE e.SCENARIO_ID IS NOT NULL
          AND e.ENTITY_ID IS NOT NULL
      ) AS a
    PIVOT
        (MAX(VALUE) FOR LINE_ITEM_CODE IN ('Ch_AE_Last_Changed_Version', 'Ch_AE_Scenario_ID', 'Ch_CurrencyCode'))
;

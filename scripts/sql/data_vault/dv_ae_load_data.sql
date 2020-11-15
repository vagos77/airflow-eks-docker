-- Remove previous records. Only use DELETE not TRUNCATE.
-- The former preserves the memory of processed files in Snowflake.
DELETE FROM ae_property_data_raw;
DELETE FROM ae_coa_data_raw;
DELETE FROM ae_lit_data_raw;

-- Copy data from S3 onto table
COPY INTO ae_property_data_raw FROM @ae_property_data
  FILE_FORMAT = AE_JSON_FORMAT;

COPY INTO ae_coa_data_raw FROM @ae_coa_data
  FILE_FORMAT = AE_JSON_FORMAT;

COPY INTO ae_lit_data_raw FROM @ae_lit_data
  FILE_FORMAT = AE_JSON_FORMAT;
  
/* *********************************************************************************** */
/* *** VARIABLES ********************************************************************* */
/* *********************************************************************************** */

ALTER SESSION SET TIMEZONE = 'UTC';
SET LoadDTS = CURRENT_TIMESTAMP;

-- Temporary variables required where source files are not currently providing data
-- Change these AS required for testing, e.g. incrementing version number
-- FIXME
SET PropertyVersion = 1; -- Required anywhere property data or children of property data is required, AS everything hangs off the property version.

/* *********************************************************************************** */
/* *** TEMP TABLES ******************************************************************* */
/* *********************************************************************************** */

CREATE OR REPLACE TRANSIENT TABLE transient_ae_property_info AS (
SELECT
    $LoadDTS AS LoadDTS
  , 'AE_JSON_EXPORT' AS StageRecSrc
  , src:PropertyInfo:ScenarioId::integer AS ScenarioId
  , src:PropertyInfo:PropertyId::integer AS PropertyId
  , src:PropertyInfo:ExternalId::string AS ExternalId
  , src:PropertyInfo:PropertyName::string AS PropertyName
  , src:PropertyInfo:Address::string AS Address
  , src:PropertyInfo:ValuationDate::date AS ValuationDate
  , src:PropertyInfo:ResaleDate::date AS ResaleDate
  , src:PropertyInfo:PropertyType::integer AS PropertyType
  , src:PropertyInfo:PropertyTypeEnum::string AS PropertyTypeEnum
  , src:PropertyInfo:PropertyCurrency::string AS PropertyCurrency
  , src:PropertyInfo:PropertyAreaMeasure::integer AS PropertyAreaMeasure
  , src:PropertyInfo:PropertyAreaMeasureEnum::string AS PropertyAreaMeasureEnum
  , src:PropertyInfo:ScenarioCurrency::string AS ScenarioCurrency
  , src:PropertyInfo:ScenarioAreaMeasure::integer AS ScenarioAreaMeasure
  , src:PropertyInfo:ScenarioName::string AS ScenarioName
  , src:PropertyInfo:ScenarioDescription::string AS ScenarioDescription
  , src:PropertyInfo:PortfolioName::string AS PortfolioName
  , src:PropertyInfo:PortfolioDescription::string AS PortfolioDescription
  , src:PropertyInfo:LastModifiedTime::timestamp AS LastModifiedTime
  , src:PropertyInfo:UserName::string AS UserName
  , src:PropertyInfo:IsBaseScenario::boolean AS IsBaseScenario
  , src:PropertyInfo:IsArchivedPropertyAsset::boolean AS IsArchivedPropertyAsset
  , src:PropertyInfo:IsArchivedScenario::boolean AS IsArchivedScenario
  , src:PropertyInfo:PropertyDescription::string AS PropertyDescription
  , src:PropertyInfo:PropertyComments::string AS PropertyComments
  , $PropertyVersion AS PropertyVersion // To do: replace with correct element
FROM ae_property_data_raw
);

CREATE OR REPLACE TRANSIENT TABLE transient_ae_property_annualised_cash_flow AS (
SELECT
    $LoadDTS AS LoadDTS
  , 'AE_JSON_EXPORT' AS StageRecSrc
  , vm.value:ScenarioId::integer AS ScenarioId
  , vm.value:PropertyId::integer AS PropertyId
  , vm.value:CurrencyBasis::integer AS CurrencyBasis
  , vm.value:IsAssuredResultSet::integer AS IsAssuredResultSet
  , vm.value:LineItemType::integer AS LineItemType
  , vm.value:LineItemName::string AS LineItemName
  , vm.value:AccountCode::string AS AccountCode
  , vm.value:ValuationDateValue::double AS ValuationDateValue
  , vm.value:ResaleValue::double AS ResaleValue
  , vm.value:UnitOfMeasure::integer AS UnitOfMeasure
  , ve.value:Amount::double AS Amount
  , ve.value:Date::date AS Date
FROM ae_property_data_raw
  , lateral flatten (input => src:PropertyAnnualisedCashFlows) vm
  , lateral flatten (input => vm.value:Amounts) ve
);

CREATE OR REPLACE TRANSIENT TABLE transient_ae_property_kpi AS (
SELECT
    $LoadDTS AS LoadDTS
  , 'AE_JSON_EXPORT' AS StageRecSrc
  , vm.value:ScenarioId::integer AS ScenarioId
  , vm.value:PropertyId::integer AS PropertyId
  , vm.value:CurrencyBasis::integer AS CurrencyBasis
  , vm.value:IsAssuredResultSet::integer AS IsAssuredResultSet
  , vm.value:ResultSet::integer AS ResultSet
  , vm.value:ResultSetEnum::string AS ResultSetEnum
  , vm.value:LineItemType::integer AS LineItemType
  , vm.value:LineItemName::string AS LineItemName
  , vm.value:AccountId::string AS AccountId
  , vm.value:ValuationDateValue::double AS ValuationDateValue
  , vm.value:UnitOfMeasure::integer AS UnitOfMeasure
  , ve.value:Amount::double AS Amount
  , ve.value:Date::date AS Date
FROM ae_property_data_raw
  , lateral flatten (input => src:PropertyKpis) vm
  , lateral flatten (input => vm.value:Amounts) ve
);

CREATE OR REPLACE TRANSIENT TABLE transient_ae_property_payment AS (
SELECT
    $LoadDTS AS LoadDTS
  , 'AE_JSON_EXPORT' AS StageRecSrc
  , vm.value:ScenarioId::integer AS ScenarioId
  , vm.value:PropertyId::integer AS PropertyId
  , vm.value:CurrencyBasis::integer AS CurrencyBasis
  , vm.value:IsAssuredResultSet::integer AS IsAssuredResultSet
  , vm.value:ResultSet::integer AS ResultSet
  , vm.value:ResultSetEnum::string AS ResultSetEnum
  , vm.value:LineItemType::integer AS LineItemType
  , vm.value:LineItemName::string AS LineItemName
  , vm.value:AccountCode::string AS AccountCode
  , vm.value:ValuationYearTotal::double AS ValuationYearTotal
  , vm.value:ResaleYearTotal::double AS ResaleYearTotal
  , ve.value:Amount::double AS Amount
  , ve.value:Date::date AS Date
FROM ae_property_data_raw
  , lateral flatten (input => src:PropertyPayments) vm
  , lateral flatten (input => vm.value:Amounts) ve
);

CREATE OR REPLACE TRANSIENT TABLE transient_ae_revex_info AS (
SELECT
    $LoadDTS AS LoadDTS
  , 'AE_JSON_EXPORT' AS StageRecSrc
  , vm.value:ScenarioId::integer AS ScenarioId
  , vm.value:PropertyId::integer AS PropertyId
  , vm.value:RevExType::integer AS RevExType
  , vm.value:RevExTypeEnum::string AS RevExTypeEnum
  , vm.value:RevExId::integer AS RevExId
  , vm.value:ParentId::string AS ParentId
  , vm.value:RevExName::string AS RevExName
  , vm.value:AccountCode::string AS AccountCode
  , vm.value:SortOrder::integer AS SortOrder
  , vm.value:Basis::integer AS Basis
  , vm.value:StartDate::date AS StartDate
  , vm.value:UnitOfMeasure::integer AS UnitOfMeasure
FROM ae_property_data_raw
  , lateral flatten (input => src:RevExInfos) vm
);

CREATE OR REPLACE TRANSIENT TABLE transient_ae_revex_annualised_cash_flow AS (
SELECT
    $LoadDTS AS LoadDTS
  , 'AE_JSON_EXPORT' AS StageRecSrc
  , vm.value:ScenarioId::integer AS ScenarioId
  , vm.value:PropertyId::integer AS PropertyId
  , vm.value:CurrencyBasis::integer AS CurrencyBasis
  , vm.value:IsAssuredResultSet::integer AS IsAssuredResultSet
  , vm.value:RevExType::integer AS RevExType
  , vm.value:RevExTypeEnum::string AS RevExTypeEnum
  , vm.value:RevExId::integer AS RevExId
  , vm.value:LineItemType::integer AS LineItemType
  , vm.value:LineItemName::string AS LineItemName
  , vm.value:UnitOfMeasure::integer AS UnitOfMeasure
  , vm.value:InitialAnnualAmount::double AS InitialAnnualAmount
  , ve.value:Amount::double AS Amount
  , ve.value:Date::date AS Date
FROM ae_property_data_raw
  , lateral flatten (input => src:RevExAnnualisedCashFlows) vm
  , lateral flatten (input => vm.value:Amounts) ve
);

CREATE OR REPLACE TRANSIENT TABLE transient_ae_revex_payment AS (
SELECT
    $LoadDTS AS LoadDTS
  , 'AE_JSON_EXPORT' AS StageRecSrc
  , vm.value:ScenarioId::integer AS ScenarioId
  , vm.value:PropertyId::integer AS PropertyId
  , vm.value:CurrencyBasis::integer AS CurrencyBasis
  , vm.value:IsAssuredResultSet::integer AS IsAssuredResultSet
  , vm.value:ResultSet::integer AS ResultSet
  , vm.value:ResultSetEnum::string AS ResultSetEnum
  , vm.value:RevExType::integer AS RevExType
  , vm.value:RevExTypeEnum::string AS RevExTypeEnum
  , vm.value:RevExId::integer AS RevExId
  , vm.value:LineItemType::integer AS LineItemType
  , vm.value:LineItemName::string AS LineItemName
  , ve.value:Amount::double AS Amount
  , ve.value:Date::date AS Date
FROM ae_property_data_raw
  , lateral flatten (input => src:RevExPayments) vm
  , lateral flatten (input => vm.value:Amounts) ve
);

CREATE OR REPLACE TRANSIENT TABLE transient_ae_lease_info AS (
SELECT
    $LoadDTS AS LoadDTS
  , 'AE_JSON_EXPORT' AS StageRecSrc
  , vm.value:ScenarioId::integer AS ScenarioId
  , vm.value:PropertyId::integer AS PropertyId
  , vm.value:LeaseId::integer AS LeaseId
  , vm.value:IsAssuredResultSet::integer AS IsAssuredResultSet
  , vm.value:LeaseAggregationKey::integer AS LeaseAggregationKey
  , vm.value:IsBaseLease::boolean AS IsBaseLease
  , vm.value:LeaseExternalId::string AS LeaseExternalId
  , vm.value:TenantName::string AS TenantName
  , vm.value:Suite::string AS Suite
  , vm.value:Tenure::string AS Tenure
  , vm.value:LeaseType::integer AS LeaseType
  , vm.value:LeaseTypeEnum::string AS LeaseTypeEnum
  , vm.value:CustomLeaseType::string AS CustomLeaseType
  , vm.value:LeaseBegin::date AS LeaseBegin
  , vm.value:LeaseExpiry::date AS LeaseExpiry
  , vm.value:ExpiryType::integer AS ExpiryType
  , vm.value:ExpiryTypeEnum::string AS ExpiryTypeEnum
  , vm.value:NextLeaseId::integer AS NextLeaseId
  , vm.value:PriorLeaseId::integer AS PriorLeaseId
  , vm.value:BaseLeaseId::integer AS BaseLeaseId
  , vm.value:EarliestBreak::date AS EarliestBreak
  , vm.value:RemainingTermDays::integer AS RemainingTermDays
  , vm.value:LeaseStatus::string AS LeaseStatus
  , vm.value:LeasePeriodDays::integer AS LeasePeriodDays
  , vm.value:MarketLeaseProfileName::string AS MarketLeaseProfileName
  , vm.value:RecoveryStructureName::string AS RecoveryStructureName
  , vm.value:CpiType::integer AS CpiType
  , vm.value:CpiTiming::string AS CpiTiming
  , vm.value:CpiRateOrIndex::string AS CpiRateOrIndex
FROM ae_property_data_raw
  , lateral flatten (input => src:LeaseInfos) vm
);

CREATE OR REPLACE TRANSIENT TABLE transient_ae_lease_annualised_cash_flows AS (
SELECT
    $LoadDTS AS LoadDTS
  , 'AE_JSON_EXPORT' AS StageRecSrc
  , vm.value:ScenarioId::integer AS ScenarioId
  , vm.value:PropertyId::integer AS PropertyId
  , vm.value:CurrencyBasis::integer AS CurrencyBasis
  , vm.value:IsAssuredResultSet::integer AS IsAssuredResultSet
  , vm.value:LeaseId::integer AS LeaseId
  , vm.value:LineItemType::integer AS LineItemType
  , vm.value:LineItemName::string AS LineItemName
  , vm.value:LeaseAggregationKey::integer AS LeaseAggregationKey
  , vm.value:IsBaseLease::boolean AS IsBaseLease
  , vm.value:LeaseBeginValue::double AS LeaseBeginValue
  , vm.value:ValuationDateValue::double AS ValuationDateValue
  , vm.value:LeaseExpiryValue::double AS LeaseExpiryValue
  , vm.value:ResaleValue::double AS ResaleValue
  , vm.value:UnitOfMeasure::integer AS UnitOfMeasure
  , ve.value:Amount::double AS Amount
  , ve.value:Date::date AS Date
FROM ae_property_data_raw
  , lateral flatten (input => src:LeaseAnnualisedCashFlows) vm
  , lateral flatten (input => vm.value:Amounts) ve
);

CREATE OR REPLACE TRANSIENT TABLE transient_ae_lease_payment AS (
SELECT
    $LoadDTS AS LoadDTS
  , 'AE_JSON_EXPORT' AS StageRecSrc
  , vm.value:ScenarioId::integer AS ScenarioId
  , vm.value:PropertyId::integer AS PropertyId
  , vm.value:CurrencyBasis::integer AS CurrencyBasis
  , vm.value:IsAssuredResultSet::integer AS IsAssuredResultSet
  , vm.value:LeaseId::integer AS LeaseId
  , vm.value:LineItemType::integer AS LineItemType
  , vm.value:LineItemName::string AS LineItemName
  , vm.value:LeaseAggregationKey::integer AS LeaseAggregationKey
  , vm.value:IsBaseLease::boolean AS IsBaseLease
  , ve.value:Amount::double AS Amount
  , ve.value:Date::date AS Date
FROM ae_property_data_raw
  , lateral flatten (input => src:LeasePayments) vm
  , lateral flatten (input => vm.value:Amounts) ve
);

CREATE OR REPLACE TRANSIENT TABLE transient_ae_loan_info AS (
SELECT
    $LoadDTS AS LoadDTS
  , 'AE_JSON_EXPORT' AS StageRecSrc
  , vm.value:ScenarioId::integer AS ScenarioId
  , vm.value:PropertyId::integer AS PropertyId
  , vm.value:LoanId::integer AS LoanId
  , vm.value:LoanName::string AS LoanName
  , vm.value:LoanType::integer AS LoanType
  , vm.value:LoanTypeEnum::string AS LoanTypeEnum
  , vm.value:Seniority::integer AS Seniority
  , vm.value:HowInput::integer AS HowInput
  , vm.value:HowInputEnum::string AS HowInputEnum
  , vm.value:LoanDate::date AS LoanDate
  , vm.value:LoanEnd::date AS LoanEnd
FROM ae_property_data_raw
  , lateral flatten (input => src:LoanInfos) vm
);

CREATE OR REPLACE TRANSIENT TABLE transient_ae_loan_payment AS (
SELECT
    $LoadDTS AS LoadDTS
  , 'AE_JSON_EXPORT' AS StageRecSrc
  , vm.value:ScenarioId::integer AS ScenarioId
  , vm.value:PropertyId::integer AS PropertyId
  , vm.value:LoanId::integer AS LoanId
  , vm.value:CurrencyBasis::integer AS CurrencyBasis
  , vm.value:IsAssuredResultSet::integer AS IsAssuredResultSet
  , vm.value:ResultSet::integer AS ResultSet
  , vm.value:ResultSetEnum::string AS ResultSetEnum
  , vm.value:LineItemType::integer AS LineItemType
  , vm.value:LineItemName::string AS LineItemName
  , vm.value:AccountCode::string AS AccountCode
  , ve.value:Amount::double AS Amount
  , ve.value:Date::date AS Date
FROM ae_property_data_raw
  , lateral flatten (input => src:LoanPayments) vm
  , lateral flatten (input => vm.value:Amounts) ve
);

/* *********************************************************************************** */
/* *** TEMP TABLES ******************************************************************* */
/* *********************************************************************************** */

-- FIXME tables have a `tmp_` prefix so they are not deleted at the end of the workflow
-- the `tmp_` prefix must be removed when the COA will be loaded into the data vault

CREATE OR REPLACE TRANSIENT TABLE transient_ae_lineitemtype AS
SELECT
    CURRENT_TIMESTAMP AS LoadDTS
  , 'AE_LIT_EXPORT' AS StageRecSrc
  , value:LineItemTypeId::integer AS LineItemTypeId
  , value:LineItemTypeName::string AS LineItemTypeName
  , value:ClassType::string AS ClassType
  , value:AccountRelationType::string AS AccountRelationType
  , value:SortOrder::integer AS SortOrder
  , value:ReportDescription::string AS ReportDescription
  , value:ReportGroup::string AS ReportGroup
  , value:ShowEntities::boolean AS ShowEntities
  , value:IsHeader::integer AS IsHeader
  , value:CostCodeType::string AS CostCodeType
  , value:LineItemParentID::integer AS LineItemParentID
  , value:LineItemDepth::integer AS LineItemDepth
  , value:UnitOfMeasureType::string AS UnitOfMeasureType
  , value:LineItemAggregationType::integer AS LineItemAggregationType
  , value:ResultStorageLocation::string AS ResultStorageLocation
  , value:SecondaryResultStorageLocation::string AS SecondaryResultStorageLocation
  , value:ThirdResultStorageLocation::string AS ThirdResultStorageLocation
  , value:AccountCode::string AS AccountCode
FROM ae_lit_data_raw
  , lateral flatten (input => src)
;

CREATE OR REPLACE TRANSIENT TABLE transient_ae_coa_info AS
SELECT
    $LoadDTS AS LoadDTS
  , 'AE_JSON_EXPORT' AS StageRecSrc
  , src:Id::integer AS ChartOfAccountId
  , src:Name::string AS ChartOfAccountName
  , src:Description::string AS Description
  , src:IsActive::boolean AS IsActive
  , src:LastModifiedTime::timestamp AS LastModifiedTime
  , src:ExternalId::string AS ExternalId
FROM ae_coa_data_raw
;

CREATE OR REPLACE TRANSIENT TABLE transient_ae_coa_accounts AS
SELECT
    $LoadDTS AS LoadDTS
  , 'AE_JSON_EXPORT' AS StageRecSrc
  , vm.value:Id::integer AS AccountId
  , vm.value:ChartOfAccountId::integer AS ChartOfAccountId
  , vm.value:AccountNumber::string AS AccountNumber
  , vm.value:Description::string AS Description
  , vm.value:ClassType::string AS ClassType
  , src:IsActive::boolean AS IsActive
  , vm.value:ParentAccountId::integer AS ParentAccountId
  , src:HasSubAccounts::boolean AS HasSubAccounts
  , vm.value:LineItemType::string AS LineItemType
  , vm.value:Level::string AS Level
  , vm.value:FxRateType::string AS FxRateType
  , vm.value:ParentAccountNumber::string AS ParentAccountNumber
  , vm.value:CostCodeType::string AS CostCodeType
FROM ae_coa_data_raw
, lateral flatten (input => src:Accounts) vm
;

CREATE OR REPLACE TRANSIENT TABLE transient_ae_coa_tenant_account_code_categories AS
SELECT
    $LoadDTS AS LoadDTS
  , 'AE_JSON_EXPORT' AS StageRecSrc
  , vm.value:Id::integer AS TCACId
  , vm.value:ChartOfAccountId::integer AS ChartOfAccountId
  , vm.value:Name::string AS Name
  , vm.value:PotentialBaseRentAccount::string AS PotentialBaseRentAccount
  , vm.value:AbsorptionAndTurnoverVacancyAccount::string AS AbsorptionAndTurnoverVacancyAccount
  , vm.value:FreeRentAccount::string AS FreeRentAccount
  , vm.value:ScheduledBaseRentAccount::string AS ScheduledBaseRentAccount
  , vm.value:StraightLineRentAccount::string AS StraightLineRentAccount
  , vm.value:RentalValueAccount::string AS RentalValueAccount
  , vm.value:CPIAccount::string AS CPIAccount
  , vm.value:TenantImprovementsAccount::string AS TenantImprovementsAccount
  , vm.value:LeasingCommissionsAccount::string AS LeasingCommissionsAccount
  , vm.value:LeaseDisplayName::string AS LeaseDisplayName
FROM ae_coa_data_raw
, lateral flatten (input => src:TenantAccountCodeCategories) vm
;

CREATE OR REPLACE TRANSIENT TABLE transient_ae_coa_multi_family_tenant_account_code_categories AS
SELECT
    $LoadDTS AS LoadDTS
  , 'AE_JSON_EXPORT' AS StageRecSrc
  , vm.value:Id::integer AS MFTCACId
  , vm.value:ChartOfAccountId::integer AS ChartOfAccountId
  , vm.value:Name::string AS Name
  , vm.value:MultiFamilyPotentialRentAfterLossToLeaseAccount::string AS MultiFamilyPotentialRentAfterLossToLeaseAccount
  , vm.value:MultiFamilyFreeRentAccount::string AS MultiFamilyFreeRentAccount
  , vm.value:MultiFamilyScheduledRentAccount::string AS MultiFamilyScheduledRentAccount
  , vm.value:MultiFamiliyAdditionalConcessionsAccount::string AS MultiFamilyAdditionalConcessionsAccount
  , vm.value:MultiFamilyUnitImprovementsAccount::string AS MultiFamilyUnitImprovementsAccount
  , vm.value:MultiFamilyUnitLeasingCostsAccount::string AS MultiFamilyUnitLeasingCostsAccount
  , vm.value:CanDelete::boolean AS CanDelete
FROM ae_coa_data_raw
, lateral flatten (input => src:MultiFamilyTenantAccountCodeCategories) vm
;

/* *********************************************************************************** */
/* *** LOAD DATA ********************************************************************* */
/* *********************************************************************************** */

/* *********************************************************************************** */
/* *** HUBs ************************************************************************** */
/* *********************************************************************************** */

INSERT ALL
  -- Create hub data for AE portfolios
  -- HUB_AE_PORTFOLIO
  -- TODO replace all instances of PortfolioName AS key with PortfolioId once we get it in the JSON file
  WHEN (SELECT COUNT(*) FROM HUB_AE_PORTFOLIO HEL WHERE HEL.MD5_HUB_AE_PORTFOLIO = MD5_AE_PortfolioName) = 0
  THEN
    INTO HUB_AE_PORTFOLIO (MD5_HUB_AE_PORTFOLIO
                         , PORTFOLIO_NAME
                         , LDTS
                         , RSRC)
    VALUES (MD5_AE_PortfolioName
          , PortfolioName
          , LoadDTS
          , StageRecSrc)
SELECT DISTINCT
    MD5(UPPER(COALESCE(TPI.PortfolioName, ''))) AS MD5_AE_PortfolioName
  , COALESCE(TPI.PortfolioName, '') AS PortfolioName
  , TPI.LoadDTS
  , TPI.StageRecSrc
FROM
    transient_ae_property_info TPI
;

INSERT ALL
  -- Create hub data for AE scenarios
  -- HUB_AE_SCENARIO
  WHEN (SELECT COUNT(*) FROM HUB_AE_SCENARIO HEL WHERE HEL.MD5_HUB_AE_SCENARIO = MD5_AE_ScenarioId) = 0
  THEN
    INTO HUB_AE_SCENARIO (MD5_HUB_AE_SCENARIO
                        , SCENARIO_ID
                        , LDTS, RSRC)
    VALUES (MD5_AE_ScenarioId
          , ScenarioId
          , LoadDTS
          , StageRecSrc)
SELECT DISTINCT
    MD5(TPI.ScenarioId) AS MD5_AE_ScenarioId
  , TPI.ScenarioId
  , TPI.LoadDTS
  , TPI.StageRecSrc
FROM
    transient_ae_property_info TPI
WHERE
    ScenarioId IS NOT NULL
;

INSERT ALL
  -- Create hub data for AE properties
  -- HUB_AE_PROPERTY
  WHEN (SELECT COUNT(*) FROM HUB_AE_PROPERTY HEL WHERE HEL.MD5_HUB_AE_PROPERTY = MD5_AE_PropertyId) = 0
  THEN
    INTO HUB_AE_PROPERTY (MD5_HUB_AE_PROPERTY
                        , PROPERTY_ID
                        , LDTS
                        , RSRC)
    VALUES (MD5_AE_PropertyId
          , PropertyId
          , LoadDTS
          , StageRecSrc)
SELECT DISTINCT
    MD5(TPI.PropertyId) AS MD5_AE_PropertyId
  , TPI.PropertyId
  , TPI.LoadDTS
  , TPI.StageRecSrc
FROM
    transient_ae_property_info TPI
WHERE
    TPI.PropertyId IS NOT NULL
;

-- Load property data that have ExternalId populated
INSERT ALL
  -- HUB_EXTERNAL_PROPERTY
  WHEN (SELECT COUNT(*) FROM HUB_EXTERNAL_PROPERTY HEP WHERE HEP.MD5_HUB_EXTERNAL_PROPERTY = MD5_ExternalPropertyId) = 0
  THEN
    INTO HUB_EXTERNAL_PROPERTY (MD5_HUB_EXTERNAL_PROPERTY
                              , EXTERNAL_PROPERTY_ID
                              , LDTS
                              , RSRC)
    VALUES (MD5_ExternalPropertyId
          , ExternalId
          , LoadDTS
          , StageRecSrc)
  -- LINK_AE_PROPERTY_EXT_PROPERTY
  WHEN (SELECT COUNT(*) FROM LINK_AE_PROPERTY_EXT_PROPERTY LPEP
       WHERE LPEP.MD5_LINK_AE_PROPERTY_EXT_PROPERTY = MD5_AE_LinkPropertyExtProperty) = 0
  THEN
    INTO LINK_AE_PROPERTY_EXT_PROPERTY (MD5_LINK_AE_PROPERTY_EXT_PROPERTY
                                      , MD5_HUB_AE_PROPERTY
                                      , MD5_HUB_EXTERNAL_PROPERTY
                                      , PROPERTY_ID
                                      , EXTERNAL_PROPERTY_ID
                                      , LDTS
                                      , RSRC)
    VALUES (MD5_AE_LinkPropertyExtProperty
          , MD5_AE_PropertyId
          , MD5_ExternalPropertyId
          , PropertyId
          , ExternalId
          , LoadDTS
          , StageRecSrc)
SELECT DISTINCT
    MD5(TPI.PropertyId || '^' || UPPER(TPI.ExternalId)) AS MD5_AE_LinkPropertyExtProperty
  , MD5(TPI.PropertyId) AS MD5_AE_PropertyId
  , MD5(UPPER(TPI.ExternalId)) AS MD5_ExternalPropertyId
  , TPI.PropertyId
  , TPI.ExternalId
  , TPI.LoadDTS
  , TPI.StageRecSrc
FROM
    transient_ae_property_info TPI
WHERE
    TPI.PropertyId IS NOT NULL
    AND TPI.ExternalId IS NOT NULL
;

INSERT ALL
  -- Create hub data for AE revex
  -- HUB_AE_REVEX
  WHEN (SELECT COUNT(*) FROM HUB_AE_REVEX HEL WHERE HEL.MD5_HUB_AE_REVEX = MD5_AE_RevExId) = 0
  THEN
    INTO HUB_AE_REVEX (MD5_HUB_AE_REVEX
                     , PROPERTY_ID
                     , REVEX_ID
                     , LDTS
                     , RSRC)
    VALUES (MD5_AE_RevExId
          , PropertyId
          , RevExId
          , LoadDTS
          , StageRecSrc)
SELECT DISTINCT
    MD5(PropertyId || '^' || RevExId) AS MD5_AE_RevExId
  , PropertyId
  , RevExId
  , LoadDTS
  , StageRecSrc
FROM
    transient_ae_revex_info
WHERE
    PropertyId IS NOT NULL
    AND RevExId IS NOT NULL
;

INSERT ALL
  -- Create hub data for AE leases
  -- HUB_AE_LEASE
  WHEN (SELECT COUNT(*) FROM HUB_AE_LEASE HEL WHERE HEL.MD5_HUB_AE_LEASE = MD5_AE_LeaseId) = 0
  THEN
    INTO HUB_AE_LEASE (MD5_HUB_AE_LEASE
                     , PROPERTY_ID
                     , LEASE_ID
                     , LDTS
                     , RSRC)
    VALUES (MD5_AE_LeaseId
          , PropertyId
          , LeaseId
          , LoadDTS
          , StageRecSrc)
SELECT DISTINCT
    MD5(TLI.PropertyId || '^' || TLI.LeaseId) AS MD5_AE_LeaseId
  , TLI.PropertyId
  , TLI.LeaseId
  , TLI.LoadDTS
  , TLI.StageRecSrc
FROM
    transient_ae_lease_info TLI
WHERE
    TLI.PropertyId IS NOT NULL
    AND TLI.LeaseId IS NOT NULL
;

-- Load lease data that have ExternalId populated
INSERT ALL
  -- HUB_EXTERNAL_LEASE
  WHEN (SELECT COUNT(*) FROM HUB_EXTERNAL_LEASE HEL WHERE HEL.MD5_HUB_EXTERNAL_LEASE = MD5_ExternalLeaseId) = 0
  THEN
    INTO HUB_EXTERNAL_LEASE (MD5_HUB_EXTERNAL_LEASE
                           , EXTERNAL_LEASE_ID
                           , LDTS
                           , RSRC)
    VALUES (MD5_ExternalLeaseId
          , LeaseExternalId
          , LoadDTS
          , StageRecSrc)
  -- LINK_AE_LEASE_EXT_LEASE
  WHEN (SELECT COUNT(*) FROM LINK_AE_LEASE_EXT_LEASE LLEL WHERE LLEL.MD5_LINK_AE_LEASE_EXT_LEASE = MD5_AE_LinkLeaseExtLease) = 0
  THEN
    INTO LINK_AE_LEASE_EXT_LEASE (MD5_LINK_AE_LEASE_EXT_LEASE
                                , MD5_HUB_AE_LEASE
                                , MD5_HUB_EXTERNAL_LEASE
                                , LEASE_ID
                                , PROPERTY_ID
                                , EXTERNAL_LEASE_ID
                                , LDTS
                                , RSRC)
    VALUES (MD5_AE_LinkLeaseExtLease
          , MD5_AE_LeaseId
          , MD5_ExternalLeaseId
          , LeaseId
          , PropertyId
          , LeaseExternalId
          , LoadDTS
          , StageRecSrc)
SELECT DISTINCT
    MD5(TLI.PropertyId || '^' || TLI.LeaseId) AS MD5_AE_LeaseId
  , MD5(TLI.PropertyId || '^' || '^' || TLI.LeaseId || '^' || UPPER(TLI.LeaseExternalId)) AS MD5_AE_LinkLeaseExtLease
  , MD5(UPPER(TLI.LeaseExternalId)) AS MD5_ExternalLeaseId
  , TLI.PropertyId
  , TLI.LeaseId
  , TLI.LeaseExternalId
  , TLI.LoadDTS
  , TLI.StageRecSrc
FROM
    transient_ae_lease_info TLI
WHERE
    TLI.PropertyId IS NOT NULL
    AND TLI.LeaseId IS NOT NULL
    AND TLI.LeaseExternalId IS NOT NULL
;

-- Load loans
INSERT ALL
  -- HUB_AE_LOAN
  WHEN (SELECT COUNT(*) FROM HUB_AE_LOAN HAL WHERE HAL.MD5_HUB_AE_LOAN = MD5_AE_LoanId) = 0
  THEN
    INTO HUB_AE_LOAN (MD5_HUB_AE_LOAN
                    , PROPERTY_ID
                    , LOAN_ID
                    , LDTS
                    , RSRC)
    VALUES (MD5_AE_LoanId
          , PropertyId
          , LoanId
          , LoadDTS
          , StageRecSRC)
SELECT
      MD5(TOI.PropertyId || '^' || TOI.LoanID) AS MD5_AE_LoanId
    , TOI.PropertyId
    , TOI.LoanId
    , TOI.LoadDTS
    , TOI.StageRecSrc
FROM
    transient_ae_loan_info TOI
WHERE
    TOI.PropertyId IS NOT NULL
    AND TOI.LoanId IS NOT NULL
;

INSERT ALL
  -- Create hub data for AE line item types
  -- HUB_AE_LINE_ITEM_TYPE
  WHEN (SELECT COUNT(*) FROM HUB_AE_LINE_ITEM_TYPE HEL WHERE HEL.MD5_HUB_AE_LINE_ITEM_TYPE = MD5_AE_LineItemType) = 0
  THEN
    INTO HUB_AE_LINE_ITEM_TYPE (MD5_HUB_AE_LINE_ITEM_TYPE
                            , LINE_ITEM_TYPE_NAME
                            , LDTS
                            , RSRC)
    VALUES (MD5_AE_LineItemType
          , LineItemTypeName
          , LoadDTS
          , StageRecSrc)
SELECT DISTINCT
    MD5(T.LineItemTypeName) AS MD5_AE_LineItemType
  , T.LineItemTypeName
  , T.LoadDTS
  , T.StageRecSrc
FROM
    transient_ae_lineitemtype T
;

/* *********************************************************************************** */
/* *** LINKs ************************************************************************* */
/* *********************************************************************************** */

INSERT ALL
  -- LINK_SCENARIO_PORTFOLIO
  WHEN (SELECT COUNT(*) FROM LINK_AE_SCENARIO_PORTFOLIO LPS WHERE LPS.MD5_LINK_AE_SCENARIO_PORTFOLIO = MD5_AE_LinkScenarioPortfolio) = 0
  THEN
    INTO LINK_AE_SCENARIO_PORTFOLIO (MD5_LINK_AE_SCENARIO_PORTFOLIO
                                   , MD5_HUB_AE_SCENARIO
                                   , MD5_HUB_AE_PORTFOLIO
                                   , SCENARIO_ID
                                   , PORTFOLIO_NAME
                                   , LDTS
                                   , RSRC)
    VALUES (MD5_AE_LinkScenarioPortfolio
          , MD5_AE_ScenarioId
          , MD5_AE_PortfolioName
          , ScenarioId
          , PortfolioName
          , LoadDTS
          , StageRecSrc)
SELECT DISTINCT
    MD5(TPI.ScenarioId) AS MD5_AE_ScenarioId,
    MD5(UPPER(COALESCE(TPI.PortfolioName, ''))) AS MD5_AE_PortfolioName,
    MD5(TPI.ScenarioId || '^' || UPPER(COALESCE(TPI.PortfolioName, ''))) AS MD5_AE_LinkScenarioPortfolio,
    TPI.ScenarioId,
    COALESCE(TPI.PortfolioName, '') AS PortfolioName,
    TPI.LoadDTS,
    TPI.StageRecSrc
FROM
    transient_ae_property_info TPI
WHERE
    TPI.ScenarioId IS NOT NULL
;

INSERT ALL
  -- LINK_AE_PROPERTY_SCENARIO
  WHEN (SELECT COUNT(*) FROM LINK_AE_PROPERTY_SCENARIO LPS WHERE LPS.MD5_LINK_AE_PROPERTY_SCENARIO = MD5_AE_LinkPropertyScenario) = 0
  THEN
    INTO LINK_AE_PROPERTY_SCENARIO (MD5_LINK_AE_PROPERTY_SCENARIO
                                  , MD5_HUB_AE_PROPERTY
                                  , MD5_HUB_AE_SCENARIO
                                  , PROPERTY_ID
                                  , SCENARIO_ID
                                  , LDTS
                                  , RSRC)
    VALUES (MD5_AE_LinkPropertyScenario
          , MD5_AE_PropertyId
          , MD5_AE_ScenarioId
          , PropertyId
          , ScenarioId
          , LoadDTS
          , StageRecSrc)
SELECT DISTINCT
    MD5(TPI.PropertyId) AS MD5_AE_PropertyId,
    MD5(TPI.ScenarioId) AS MD5_AE_ScenarioId,
    MD5(TPI.PropertyId || '^' || ScenarioId) AS MD5_AE_LinkPropertyScenario,
    TPI.PropertyId,
    TPI.ScenarioId,
    TPI.LoadDTS,
    TPI.StageRecSrc
FROM
    transient_ae_property_info TPI
WHERE
    TPI.PropertyId IS NOT NULL
    AND TPI.ScenarioId IS NOT NULL
;

INSERT ALL
  -- Create link data for AE leases to AE property
  -- LINK_AE_REVEX_PROPERTY
  WHEN (SELECT COUNT(*) FROM LINK_AE_REVEX_PROPERTY WHERE MD5_LINK_AE_REVEX_PROPERTY = MD5_AE_LinkPropertyRevEx) = 0
  THEN
    INTO LINK_AE_REVEX_PROPERTY (MD5_LINK_AE_REVEX_PROPERTY
                               , MD5_HUB_AE_REVEX
                               , MD5_HUB_AE_PROPERTY
                               , MD5_HUB_AE_PARENT_PROPERTY
                               , REVEX_ID
                               , PROPERTY_ID
                               , PARENT_ID
                               , LDTS
                               , RSRC)
    VALUES (MD5_AE_LinkPropertyRevEx
          , MD5_AE_PropertyRevExId
          , MD5_AE_PropertyId
          , MD5_AE_ParentId
          , RevExId
          , PropertyId
          , ParentId
          , LoadDTS
          , StageRecSrc)
SELECT DISTINCT
    MD5(TRI.PropertyId || '^' || TRI.RevExId || '^' || TRI.ParentId) AS MD5_AE_LinkPropertyRevEx
  , MD5(TRI.PropertyId || '^' || TRI.RevExId) AS MD5_AE_PropertyRevExId
  , MD5(TRI.PropertyId) AS MD5_AE_PropertyId
  , MD5(TRI.ParentId) AS MD5_AE_ParentId
  , TRI.RevExId
  , TRI.PropertyId
  , TRI.ParentId
  , TRI.LoadDTS
  , TRI.StageRecSrc
FROM
    transient_ae_revex_info TRI
WHERE
    TRI.PropertyID IS NOT NULL
    AND TRI.RevExId IS NOT NULL
    AND TRI.ParentId IS NOT NULL
;

INSERT ALL
  -- Create link data for AE leases to AE property
  -- LINK_AE_LEASE_PROPERTY
  WHEN (SELECT COUNT(*) FROM LINK_AE_LEASE_PROPERTY LLP WHERE LLP.MD5_LINK_AE_LEASE_PROPERTY = MD5_AE_LinkPropertyLease) = 0
  THEN
    INTO LINK_AE_LEASE_PROPERTY (MD5_LINK_AE_LEASE_PROPERTY
                               , MD5_HUB_AE_LEASE
                               , MD5_HUB_AE_PROPERTY
                               , MD5_HUB_AE_BASELEASE
                               , MD5_HUB_AE_PRIORLEASE
                               , MD5_HUB_AE_NEXTLEASE
                               , LEASE_ID
                               , PROPERTY_ID
                               , BASELEASE_ID
                               , PRIORLEASE_ID
                               , NEXTLEASE_ID
                               , LDTS
                               , RSRC)
    VALUES (MD5_AE_LinkPropertyLease
          , MD5_AE_LeaseId
          , MD5_AE_PropertyId
          , MD5_AE_BaseLeaseId
          , MD5_AE_PriorLeaseId
          , MD5_AE_NextLeaseId
          , LeaseId
          , PropertyId
          , BaseLeaseId
          , PriorLeaseId
          , NextLeaseId
          , LoadDTS
          , StageRecSrc)
SELECT DISTINCT
    MD5(TLI.PropertyId || '^' || TLI.LeaseId || '^' || TLI.BaseLeaseId || '^' || TLI.PriorLeaseId || '^' || TLI.NextLeaseId) AS MD5_AE_LinkPropertyLease
  , MD5(TLI.PropertyId || '^' || TLI.LeaseId) AS MD5_AE_LeaseId
  , MD5(TLI.PropertyId) AS MD5_AE_PropertyId
  , MD5(TLI.PropertyId || '^' || TLI.BaseLeaseId) AS MD5_AE_BaseLeaseId
  , MD5(TLI.PropertyId || '^' || TLI.PriorLeaseId) AS MD5_AE_PriorLeaseId
  , MD5(TLI.PropertyId || '^' || TLI.NextLeaseId) AS MD5_AE_NextLeaseId
  , TLI.LeaseId
  , TLI.PropertyId
  , TLI.BaseLeaseId
  , TLI.PriorLeaseId
  , TLI.NextLeaseId
  , TLI.LoadDTS
  , TLI.StageRecSrc
FROM
    transient_ae_lease_info TLI
WHERE
    TLI.PropertyID IS NOT NULL
    AND TLI.LeaseID IS NOT NULL
    AND TLI.BaseLeaseID IS NOT NULL
    AND TLI.PriorLeaseID IS NOT NULL
    AND TLI.NextLeaseID IS NOT NULL
;

INSERT ALL
  -- LINK_EXT_PROPERTY_EXT_LEASE
  WHEN (SELECT COUNT(*) FROM LINK_EXT_PROPERTY_EXT_LEASE EPEL WHERE EPEL.MD5_LINK_EXT_PROPERTY_EXT_LEASE = MD5_AE_LinkExtPropertyExtLease) = 0
  THEN
    INTO LINK_EXT_PROPERTY_EXT_LEASE (MD5_LINK_EXT_PROPERTY_EXT_LEASE
                                    , MD5_HUB_EXTERNAL_PROPERTY
                                    , MD5_HUB_EXTERNAL_LEASE
                                    , EXTERNAL_PROPERTY_ID
                                    , EXTERNAL_LEASE_ID
                                    , LDTS
                                    , RSRC)
    VALUES (MD5_AE_LinkExtPropertyExtLease
          , MD5_ExternalPropertyId
          , MD5_ExternalLeaseId
          , PropertyExternalId
          , LeaseExternalId
          , LoadDTS
          , StageRecSrc)
SELECT DISTINCT
    MD5(UPPER(TPI.ExternalId || '^' || TLI.LeaseExternalId)) AS MD5_AE_LinkExtPropertyExtLease
  , MD5(UPPER(TPI.ExternalId)) AS MD5_ExternalPropertyId
  , MD5(UPPER(TLI.LeaseExternalId)) AS MD5_ExternalLeaseId
  , TPI.ExternalId AS PropertyExternalId
  , TLI.LeaseExternalId
  , TLI.LoadDTS
  , TLI.StageRecSrc
FROM
    transient_ae_lease_info TLI
    INNER JOIN transient_ae_property_info TPI ON TLI.PropertyId = TPI.PropertyId
WHERE
    TLI.LeaseExternalId IS NOT NULL
    AND TPI.ExternalId IS NOT NULL
;

INSERT ALL
  -- LINK_AE_LOAN_PROPERTY
  WHEN (SELECT COUNT(*) FROM LINK_AE_LOAN_PROPERTY LLP WHERE LLP.MD5_LINK_AE_LOAN_PROPERTY = MD5_AE_LinkLoanProperty) = 0
  THEN
    INTO LINK_AE_LOAN_PROPERTY (MD5_LINK_AE_LOAN_PROPERTY
                              , MD5_HUB_AE_LOAN
                              , MD5_HUB_AE_PROPERTY
                              , LOAN_ID
                              , PROPERTY_ID
                              , LDTS
                              , RSRC)
    VALUES (MD5_AE_LinkLoanProperty
          , MD5_AE_LoanId
          , MD5_AE_PropertyId
          , LoanId
          , PropertyId
          , LoadDTS
          , StageRecSrc)
SELECT
    MD5(TOI.PropertyId || '^' || TOI.LoanID) AS MD5_AE_LinkLoanProperty
  , MD5(TOI.PropertyId || '^' || TOI.LoanID) AS MD5_AE_LoanId
  , MD5(TOI.PropertyId) AS MD5_AE_PropertyId
  , TOI.LoanId
  , TOI.PropertyId
  , TOI.LoadDTS
  , TOI.StageRecSrc
FROM
    transient_ae_loan_info TOI
WHERE
    TOI.PropertyId IS NOT NULL
    AND TOI.LoanId IS NOT NULL
;

/* *********************************************************************************** */
/* *** SATELLITEs ******************************************************************** */
/* *********************************************************************************** */

INSERT ALL
  -- Load data for AE portfolio details
  -- SAT_AE_PORTFOLIO_DETAILS
  WHEN (SELECT COUNT(*) FROM SAT_AE_PORTFOLIO_DETAILS SD WHERE SD.MD5_HUB_AE_PORTFOLIO = MD5_AE_PortfolioName AND SD.HASH_DIFF = MD5_PortfolioDetailsHashDiff) = 0
  THEN
    INTO SAT_AE_PORTFOLIO_DETAILS (MD5_HUB_AE_PORTFOLIO
                                 , HASH_DIFF
                                 , PORTFOLIO_NAME
                                 , PORTFOLIO_DESCRIPTION
                                 , LDTS
                                 , RSRC)
    VALUES (MD5_AE_PortfolioName
          , MD5_PortfolioDetailsHashDiff
          , PortfolioName
          , PortfolioDescription
          , LoadDTS
          , StageRecSrc)
SELECT DISTINCT
    MD5(UPPER(COALESCE(TPI.PortfolioName, ''))) AS MD5_AE_PortfolioName
  , MD5(UPPER(COALESCE(TPI.PortfolioName, '') || '^' || COALESCE(TPI.PortfolioDescription, ''))) AS MD5_PortfolioDetailsHashDiff
  , COALESCE(TPI.PortfolioName, '') AS PortfolioName
  , TPI.PortfolioDescription
  , TPI.LoadDTS
  , TPI.StageRecSrc
FROM
    transient_ae_property_info TPI
;

INSERT ALL
  -- Load data for AE scenario details
  -- SAT_AE_SCENARIO_DETAILS
  WHEN (SELECT COUNT(*) FROM SAT_AE_SCENARIO_DETAILS SD WHERE SD.MD5_HUB_AE_SCENARIO = MD5_AE_ScenarioId AND SD.HASH_DIFF = MD5_ScenarioDetailsHashDiff) = 0
  THEN
    INTO SAT_AE_SCENARIO_DETAILS (MD5_HUB_AE_SCENARIO
                                , HASH_DIFF
                                , SCENARIO_NAME
                                , SCENARIO_DESCRIPTION
                                , SCENARIO_CURRENCY
                                , SCENARIO_AREA_MEASURE
                                , SCENARIO_ARCHIVED
                                , IS_BASE_SCENARIO
                                , LDTS
                                , RSRC)
    VALUES (MD5_AE_ScenarioId
          , MD5_ScenarioDetailsHashDiff
          , ScenarioName
          , ScenarioDescription
          , ScenarioCurrency
          , ScenarioAreaMeasure
          , IsArchivedScenario
          , IsBaseScenario
          , LoadDTS
          , StageRecSrc)
SELECT DISTINCT
    MD5(TPI.ScenarioId) AS MD5_AE_ScenarioId
  , MD5(UPPER(COALESCE(TPI.ScenarioName, '')
    || '^' || COALESCE(TPI.ScenarioDescription, '')
    || '^' || COALESCE(TPI.ScenarioCurrency, '')
    || '^' || COALESCE(TO_VARCHAR(TPI.ScenarioAreaMeasure), '')
    || '^' || COALESCE(TO_VARCHAR(TPI.IsArchivedScenario), '')
    || '^' || COALESCE(TO_VARCHAR(TPI.IsBaseScenario), '')
    )) AS MD5_ScenarioDetailsHashDiff
  , TPI.ScenarioName
  , TPI.ScenarioDescription
  , TPI.ScenarioCurrency
  , TPI.ScenarioAreaMeasure
  , TPI.IsArchivedScenario
  , TPI.IsBaseScenario
  , TPI.LoadDTS
  , TPI.StageRecSrc
FROM
    transient_ae_property_info TPI
WHERE
    TPI.ScenarioId IS NOT NULL
;

INSERT ALL
  -- Load data for AE property details
  -- SAT_AE_PROPERTY_DETAILS
  WHEN (SELECT COUNT(*) FROM SAT_AE_PROPERTY_DETAILS SLD WHERE SLD.MD5_HUB_AE_PROPERTY = MD5_AE_PropertyId AND SLD.HASH_DIFF = PropertyInfoHashDiff) = 0
  THEN
    INTO SAT_AE_PROPERTY_DETAILS (MD5_HUB_AE_PROPERTY
                                , HASH_DIFF
                                , PROPERTY_NAME
                                , VALUATION_DATE
                                , RESALE_DATE
                                , PROPERTY_TYPE
                                , PROPERTY_TYPE_ENUM
                                , LOCAL_CURRENCY
                                , PROPERTY_AREA_MEASURE
                                , PROPERTY_AREA_MEASURE_ENUM
                                , PROPERTY_ARCHIVED
                                , PROPERTY_DESCRIPTION
                                , PROPERTY_COMMENTS
                                , LDTS
                                , RSRC)
      VALUES (MD5_AE_PropertyId
            , PropertyInfoHashDiff
            , PropertyName
            , ValuationDate
            , ResaleDate
            , PropertyType
            , PropertyTypeEnum
            , PropertyCurrency
            , PropertyAreaMeasure
            , PropertyAreaMeasureEnum
            , IsArchivedPropertyAsset
            , PropertyDescription
            , PropertyComments
            , LoadDTS
            , StageRecSrc)
  --  SAT_AE_PROPERTY_LOCATION
  WHEN (SELECT COUNT(*) FROM SAT_AE_PROPERTY_LOCATION SLL WHERE SLL.MD5_HUB_AE_PROPERTY = MD5_AE_PropertyId AND SLL.HASH_DIFF = PropertyLocationHashDiff) = 0
  THEN
    INTO SAT_AE_PROPERTY_LOCATION (MD5_HUB_AE_PROPERTY
                                 , HASH_DIFF
                                 , ADDRESS
                                 , LDTS
                                 , RSRC)
    VALUES (MD5_AE_PropertyId
          , PropertyLocationHashDiff
          , Address
          , LoadDTS
          , StageRecSrc)
SELECT DISTINCT
    MD5(TPI.PropertyId) AS MD5_AE_PropertyId
  , MD5(UPPER(COALESCE(TO_VARCHAR(TPI.PropertyName), '')
    || '^' || COALESCE(TO_VARCHAR(TPI.ValuationDate), '')
    || '^' || COALESCE(TO_VARCHAR(TPI.ResaleDate), '')
    || '^' || COALESCE(TO_VARCHAR(TPI.PropertyType), '')
    || '^' || COALESCE(TO_VARCHAR(TPI.PropertyTypeEnum), '')
    || '^' || COALESCE(TO_VARCHAR(TPI.PropertyCurrency), '')
    || '^' || COALESCE(TO_VARCHAR(TPI.PropertyAreaMeasure), '')
    || '^' || COALESCE(TO_VARCHAR(TPI.PropertyAreaMeasureEnum), '')
    || '^' || COALESCE(TO_VARCHAR(TPI.IsArchivedPropertyAsset), '')
    || '^' || COALESCE(TO_VARCHAR(TPI.PropertyDescription), '')
    || '^' || COALESCE(TO_VARCHAR(TPI.PropertyComments), '')
    )) AS PropertyInfoHashDiff
  , MD5(UPPER(COALESCE(TO_VARCHAR(TPI.PropertyName), '')
    || '^' || COALESCE(TO_VARCHAR(TPI.Address), '')
    )) AS PropertyLocationHashDiff
  , TPI.PropertyId
  , TPI.PropertyName
  , TPI.Address
  , TPI.ValuationDate
  , TPI.ResaleDate
  , TPI.PropertyType
  , TPI.PropertyTypeEnum
  , TPI.PropertyCurrency
  , TPI.PropertyAreaMeasure
  , TPI.PropertyAreaMeasureEnum
  , TPI.IsArchivedPropertyAsset
  , TPI.PropertyDescription
  , TPI.PropertyComments
  , TPI.LoadDTS
  , TPI.StageRecSrc
FROM
    transient_ae_property_info TPI
WHERE
    TPI.PropertyId IS NOT NULL
;

INSERT ALL
  -- Load data for AE property metadata
  -- SAT_AE_METADATA_PROPERTY
  WHEN (SELECT COUNT(*) FROM SAT_AE_METADATA_PROPERTY SMP WHERE SMP.MD5_HUB_AE_PROPERTY = MD5_AE_PropertyId AND SMP.HASH_DIFF = PropertyMetadataHashDiff) = 0
  THEN
    INTO SAT_AE_METADATA_PROPERTY (MD5_HUB_AE_PROPERTY
                                 , HASH_DIFF
                                 , PROPERTY_LAST_MODIFIED_BY
                                 , PROPERTY_LAST_MODIFIED_DATE
                                 , LDTS
                                 , RSRC)
    VALUES (MD5_AE_PropertyId
          , PropertyMetadataHashDiff
          , PropertyLastModifiedBy
          , PropertyLastModifiedDate
          , LoadDTS
          , StageRecSrc)
SELECT DISTINCT
    MD5(TPI.PropertyId) AS MD5_AE_PropertyId
  , MD5(UPPER(COALESCE(TO_VARCHAR(TPI.UserName), '')
    || '^' || COALESCE(TO_VARCHAR(TPI.LastModifiedTime), '')
  )) AS PropertyMetadataHashDiff
  , TPI.UserName AS PropertyLastModifiedBy
  , TPI.LastModifiedTime AS PropertyLastModifiedDate
  , TPI.LoadDTS
  , TPI.StageRecSrc
FROM
    transient_ae_property_info TPI
WHERE
    TPI.PropertyId IS NOT NULL
;

INSERT ALL
  -- Load AE property cash flows
  -- SAT_AE_PROPERTY_CASHFLOW
  WHEN (SELECT COUNT(*) FROM SAT_AE_PROPERTY_CASHFLOW SPC WHERE SPC.MD5_HUB_AE_PROPERTY = MD5_AE_PropertyId AND SPC.HASH_DIFF = PropertyCashFlowHashDiff) = 0
  THEN
    INTO SAT_AE_PROPERTY_CASHFLOW (MD5_HUB_AE_PROPERTY
                                 , HASH_DIFF
                                 , CURRENCY_BASIS
                                 , IS_ASSURED_RESULTSET
                                 , LINE_ITEM_TYPE_ID
                                 , LINE_ITEM_TYPE_NAME
                                 , ACCOUNT_CODE
                                 , VALUATION_DATE_VALUE
                                 , RESALE_VALUE
                                 , UNIT_OF_MEASURE
                                 , AMOUNT
                                 , DATE
                                 , LDTS
                                 , RSRC)
    VALUES (MD5_AE_PropertyId
          , PropertyCashFlowHashDiff
          , CurrencyBasis
          , IsAssuredResultSet
          , LineItemType
          , LineItemName
          , AccountCode
          , ValuationDateValue
          , ResaleValue
          , UnitOfMeasure
          , Amount
          , Date
          , LoadDTS
          , StageRecSrc)
SELECT
    MD5(TPC.PropertyId) AS MD5_AE_PropertyId
  , MD5(UPPER(COALESCE(TO_VARCHAR(TPC.CurrencyBasis), '')
    || '^' || COALESCE(TO_VARCHAR(TPC.IsAssuredResultSet), '')
    || '^' || COALESCE(TO_VARCHAR(TPC.LineItemType), '')
    || '^' || COALESCE(TO_VARCHAR(TPC.LineItemName), '')
    || '^' || COALESCE(TO_VARCHAR(TPC.AccountCode), '')
    || '^' || COALESCE(TO_VARCHAR(TPC.ValuationDateValue), '')
    || '^' || COALESCE(TO_VARCHAR(TPC.ResaleValue), '')
    || '^' || COALESCE(TO_VARCHAR(TPC.UnitOfMeasure), '')
    || '^' || COALESCE(TO_VARCHAR(TPC.Amount), '')
    || '^' || COALESCE(TO_VARCHAR(TPC.Date), '')
    )) AS PropertyCashFlowHashDiff
  , TPC.CurrencyBasis
  , TPC.IsAssuredResultSet
  , TPC.LineItemType
  , TPC.LineItemName
  , TPC.AccountCode
  , TPC.ValuationDateValue
  , TPC.ResaleValue
  , TPC.UnitOfMeasure
  , TPC.Amount
  , TPC.Date
  , TPC.LoadDTS
  , TPC.StageRecSrc
FROM
    transient_ae_property_annualised_cash_flow TPC
WHERE
    TPC.PropertyId IS NOT NULL
;

INSERT ALL
  -- Load AE property KPIs
  -- SAT_AE_PROPERTY_KPI
  WHEN (SELECT COUNT(*) FROM SAT_AE_PROPERTY_KPI SPK WHERE SPK.MD5_HUB_AE_PROPERTY = MD5_AE_PropertyId AND SPK.HASH_DIFF = PropertyKpiHashDiff) = 0
  THEN
    INTO SAT_AE_PROPERTY_KPI (MD5_HUB_AE_PROPERTY
                            , HASH_DIFF
                            , CURRENCY_BASIS
                            , IS_ASSURED_RESULTSET
                            , RESULTSET_ID
                            , RESULTSET
                            , LINE_ITEM_TYPE_ID
                            , LINE_ITEM_TYPE_NAME
                            , ACCOUNT_CODE
                            , VALUATION_DATE_VALUE
                            , UNIT_OF_MEASURE
                            , AMOUNT
                            , DATE
                            , LDTS
                            , RSRC)
    VALUES (MD5_AE_PropertyId
          , PropertyKpiHashDiff
          , CurrencyBasis
          , IsAssuredResultSet
          , ResultSet
          , ResultSetEnum
          , LineItemType
          , LineItemName
          , AccountId
          , ValuationDateValue
          , UnitOfMeasure
          , Amount
          , Date
          , LoadDTS
          , StageRecSrc)
SELECT
    MD5(TPK.PropertyId) AS MD5_AE_PropertyId
  , MD5(UPPER(COALESCE(TO_VARCHAR(TPK.CurrencyBasis), '')
    || '^' || COALESCE(TO_VARCHAR(TPK.IsAssuredResultSet), '')
    || '^' || COALESCE(TO_VARCHAR(TPK.ResultSet), '')
    || '^' || COALESCE(TO_VARCHAR(TPK.ResultSetEnum), '')
    || '^' || COALESCE(TO_VARCHAR(TPK.LineItemType), '')
    || '^' || COALESCE(TO_VARCHAR(TPK.LineItemName), '')
    || '^' || COALESCE(TO_VARCHAR(TPK.AccountId), '')
    || '^' || COALESCE(TO_VARCHAR(TPK.ValuationDateValue), '')
    || '^' || COALESCE(TO_VARCHAR(TPK.UnitOfMeasure), '')
    || '^' || COALESCE(TO_VARCHAR(TPK.Amount), '')
    || '^' || COALESCE(TO_VARCHAR(TPK.Date), '')
    )) AS PropertyKpiHashDiff
  , TPK.CurrencyBasis
  , TPK.IsAssuredResultSet
  , TPK.ResultSet
  , TPK.ResultSetEnum
  , TPK.LineItemType
  , TPK.LineItemName
  , TPK.AccountId
  , TPK.ValuationDateValue
  , TPK.UnitOfMeasure
  , TPK.Amount
  , TPK.Date
  , TPK.LoadDTS
  , TPK.StageRecSrc
FROM
    transient_ae_property_kpi TPK
WHERE
    TPK.PropertyId IS NOT NULL
;

INSERT ALL
  -- Load AE property payments
  -- SAT_AE_PROPERTY_PAYMENT
  WHEN (SELECT COUNT(*) FROM SAT_AE_PROPERTY_PAYMENT SPP WHERE SPP.MD5_HUB_AE_PROPERTY = MD5_AE_PropertyId AND SPP.HASH_DIFF = PropertyPaymentHashDiff) = 0
  THEN
    INTO SAT_AE_PROPERTY_PAYMENT (MD5_HUB_AE_PROPERTY
                                , HASH_DIFF
                                , CURRENCY_BASIS
                                , IS_ASSURED_RESULTSET
                                , LINE_ITEM_TYPE_ID
                                , LINE_ITEM_TYPE_NAME
                                , ACCOUNT_CODE
                                , VALUATION_YEAR_TOTAL
                                , RESALE_YEAR_TOTAL
                                , AMOUNT
                                , DATE
                                , LDTS
                                , RSRC)
      VALUES (MD5_AE_PropertyId
            , PropertyPaymentHashDiff
            , CurrencyBasis
            , IsAssuredResultSet
            , LineItemType
            , LineItemName
            , AccountCode
            , ValuationYearTotal
            , ResaleYearTotal
            , Amount
            , Date
            , LoadDTS
            , StageRecSrc)
SELECT
    MD5(TPP.PropertyId) AS MD5_AE_PropertyId
  , MD5(UPPER(COALESCE(TO_VARCHAR(TPP.CurrencyBasis), '')
    || '^' || COALESCE(TO_VARCHAR(TPP.IsAssuredResultSet), '')
    || '^' || COALESCE(TO_VARCHAR(TPP.LineItemType), '')
    || '^' || COALESCE(TO_VARCHAR(TPP.LineItemName), '')
    || '^' || COALESCE(TO_VARCHAR(TPP.AccountCode), '')
    || '^' || COALESCE(TO_VARCHAR(TPP.ValuationYearTotal), '')
    || '^' || COALESCE(TO_VARCHAR(TPP.ResaleYearTotal), '')
    || '^' || COALESCE(TO_VARCHAR(TPP.Amount), '')
    || '^' || COALESCE(TO_VARCHAR(TPP.Date), '')
    )) AS PropertyPaymentHashDiff
  , TPP.CurrencyBasis
  , TPP.IsAssuredResultSet
  , TPP.LineItemType
  , TPP.LineItemName
  , TPP.AccountCode
  , TPP.ValuationYearTotal
  , TPP.ResaleYearTotal
  , TPP.Amount
  , TPP.Date
  , TPP.LoadDTS
  , TPP.StageRecSrc
FROM
    transient_ae_property_payment TPP
WHERE
    TPP.PropertyId IS NOT NULL
;

INSERT ALL
  -- Load data for AE revex details
  -- SAT_AE_REVEX_DETAILS
  WHEN (SELECT COUNT(*) FROM SAT_AE_REVEX_DETAILS SRD WHERE SRD.MD5_HUB_AE_REVEX = MD5_AE_RevExId AND SRD.HASH_DIFF = RevExInfoHashDiff) = 0
    THEN
      INTO SAT_AE_REVEX_DETAILS (MD5_HUB_AE_REVEX
                               , HASH_DIFF
                               , REVEX_NAME
                               , REVEX_TYPE_ID
                               , REVEX_TYPE_ENUM
                               , ACCOUNT_CODE
                               , SORT_ORDER
                               , BASIS
                               , START_DATE
                               , UNIT_OF_MEASURE
                               , LDTS
                               , RSRC)
        VALUES (MD5_AE_RevExId
              , RevExInfoHashDiff
              , RevExName
              , RevExType
              , RevExTypeEnum
              , AccountCode
              , SortOrder
              , Basis
              , StartDate
              , UnitOfMeasure
              , LoadDTS
              , StageRecSrc)
SELECT DISTINCT
    MD5(TRI.PropertyId || '^' || TRI.RevExId) AS MD5_AE_RevExId
  , MD5(UPPER(COALESCE(TO_VARCHAR(TRI.RevExName), '')
    || '^' || COALESCE(TO_VARCHAR(TRI.RevExType), '')
    || '^' || COALESCE(TO_VARCHAR(TRI.RevExTypeEnum), '')
    || '^' || COALESCE(TO_VARCHAR(TRI.AccountCode), '')
    || '^' || COALESCE(TO_VARCHAR(TRI.SortOrder), '')
    || '^' || COALESCE(TO_VARCHAR(TRI.Basis), '')
    || '^' || COALESCE(TO_VARCHAR(TRI.StartDate), '')
    || '^' || COALESCE(TO_VARCHAR(TRI.UnitOfMeasure), '')
  )) AS RevExInfoHashDiff
  , TRI.RevExName
  , TRI.RevExType
  , TRI.RevExTypeEnum
  , TRI.AccountCode
  , TRI.SortOrder
  , TRI.Basis
  , TRI.StartDate
  , TRI.UnitOfMeasure
  , TRI.LoadDTS
  , TRI.StageRecSrc
FROM
    transient_ae_revex_info TRI
WHERE
    TRI.PropertyId IS NOT NULL
    AND TRI.RevExId IS NOT NULL
;

INSERT ALL
  -- Load AE revex cash flows
  -- SAT_AE_REVEX_CASHFLOW
  WHEN (SELECT COUNT(*) FROM SAT_AE_REVEX_CASHFLOW SPC WHERE SPC.MD5_HUB_AE_REVEX = MD5_AE_RevExId AND SPC.HASH_DIFF = RevExCashFlowHashDiff) = 0
  THEN
    INTO SAT_AE_REVEX_CASHFLOW (MD5_HUB_AE_REVEX
                              , HASH_DIFF
                              , CURRENCY_BASIS
                              , IS_ASSURED_RESULTSET
                              , REVEX_TYPE_ID
                              , REVEX_TYPE_ENUM
                              , LINE_ITEM_TYPE_ID
                              , LINE_ITEM_TYPE_NAME
                              , UNIT_OF_MEASURE
                              , INITIAL_ANNUAL_AMOUNT
                              , AMOUNT
                              , DATE
                              , LDTS
                              , RSRC)
    VALUES (MD5_AE_RevExId
          , RevExCashFlowHashDiff
          , CurrencyBasis
          , IsAssuredResultSet
          , RevExType
          , RevExTypeEnum
          , LineItemType
          , LineItemName
          , UnitOfMeasure
          , InitialAnnualAmount
          , Amount
          , Date
          , LoadDTS
          , StageRecSrc)
SELECT
    MD5(TPC.PropertyId || '^' || TPC.RevExId) AS MD5_AE_RevExId
  , MD5(UPPER(COALESCE(TO_VARCHAR(TPC.CurrencyBasis), '')
    || '^' || COALESCE(TO_VARCHAR(TPC.IsAssuredResultSet), '')
    || '^' || COALESCE(TO_VARCHAR(TPC.RevExType), '')
    || '^' || COALESCE(TO_VARCHAR(TPC.RevExTypeEnum), '')
    || '^' || COALESCE(TO_VARCHAR(TPC.LineItemType), '')
    || '^' || COALESCE(TO_VARCHAR(TPC.LineItemName), '')
    || '^' || COALESCE(TO_VARCHAR(TPC.UnitOfMeasure), '')
    || '^' || COALESCE(TO_VARCHAR(TPC.InitialAnnualAmount), '')
    || '^' || COALESCE(TO_VARCHAR(TPC.Amount), '')
    || '^' || COALESCE(TO_VARCHAR(TPC.Date), '')
  )) AS RevExCashFlowHashDiff
  , TPC.CurrencyBasis
  , TPC.IsAssuredResultSet
  , TPC.RevExType
  , TPC.RevExTypeEnum
  , TPC.LineItemType
  , TPC.LineItemName
  , TPC.UnitOfMeasure
  , TPC.InitialAnnualAmount
  , TPC.Amount
  , TPC.Date
  , TPC.LoadDTS
  , TPC.StageRecSrc
FROM
    transient_ae_revex_annualised_cash_flow TPC
;

INSERT ALL
  -- Load AE revex payments
  -- SAT_AE_REVEX_PAYMENT
  WHEN (SELECT COUNT(*) FROM SAT_AE_REVEX_PAYMENT SPP WHERE SPP.MD5_HUB_AE_REVEX = MD5_AE_RevExId AND SPP.HASH_DIFF = RevExPaymentHashDiff) = 0
  THEN
    INTO SAT_AE_REVEX_PAYMENT (MD5_HUB_AE_REVEX
                             , HASH_DIFF
                             , CURRENCY_BASIS
                             , IS_ASSURED_RESULTSET
                             , RESULTSET_ID
                             , RESULTSET
                             , REVEX_TYPE_ID
                             , REVEX_TYPE_ENUM
                             , LINE_ITEM_TYPE_ID
                             , LINE_ITEM_TYPE_NAME
                             , AMOUNT
                             , DATE
                             , LDTS
                             , RSRC)
    VALUES (MD5_AE_RevExId
          , RevExPaymentHashDiff
          , CurrencyBasis
          , IsAssuredResultSet
          , ResultSet
          , ResultSetEnum
          , RevExType
          , RevExTypeEnum
          , LineItemType
          , LineItemName
          , Amount
          , Date
          , LoadDTS
          , StageRecSrc)
SELECT
    MD5(TPP.PropertyId || '^' || TPP.RevExId) AS MD5_AE_RevExId
  , MD5(UPPER(COALESCE(TO_VARCHAR(TPP.CurrencyBasis), '')
    || '^' || COALESCE(TO_VARCHAR(TPP.IsAssuredResultSet), '')
    || '^' || COALESCE(TO_VARCHAR(TPP.ResultSet), '')
    || '^' || COALESCE(TO_VARCHAR(TPP.ResultSetEnum), '')
    || '^' || COALESCE(TO_VARCHAR(TPP.RevExType), '')
    || '^' || COALESCE(TO_VARCHAR(TPP.RevExTypeEnum), '')
    || '^' || COALESCE(TO_VARCHAR(TPP.LineItemType), '')
    || '^' || COALESCE(TO_VARCHAR(TPP.LineItemName), '')
    || '^' || COALESCE(TO_VARCHAR(TPP.Amount), '')
    || '^' || COALESCE(TO_VARCHAR(TPP.Date), '')
  )) AS RevExPaymentHashDiff
  , TPP.CurrencyBasis
  , TPP.IsAssuredResultSet
  , TPP.ResultSet
  , TPP.ResultSetEnum
  , TPP.RevExType
  , TPP.RevExTypeEnum
  , TPP.LineItemType
  , TPP.LineItemName
  , TPP.Amount
  , TPP.Date
  , TPP.LoadDTS
  , TPP.StageRecSrc
FROM
    transient_ae_revex_payment TPP
;

INSERT ALL
  -- Load data for AE lease details
  -- SAT_AE_LEASE_DETAILS
  WHEN (SELECT COUNT(*) FROM SAT_AE_LEASE_DETAILS SLD WHERE SLD.MD5_HUB_AE_LEASE = MD5_AE_LeaseId AND SLD.HASH_DIFF = LeaseInfoHashDiff) = 0
  THEN
    INTO SAT_AE_LEASE_DETAILS (MD5_HUB_AE_LEASE
                             , HASH_DIFF
                             , LEASE_AGGREGATION_KEY
                             , IS_BASE_LEASE
                             , TENANT_NAME
                             , SUITE
                             , TENURE
                             , LEASE_TYPE
                             , CUSTOM_LEASE_TYPE
                             , LEASE_BEGIN
                             , LEASE_EXPIRY
                             , EXPIRY_TYPE
                             , EARLIEST_BREAK
                             , REMAINING_TERM_DAYS
                             , LEASE_STATUS
                             , LEASE_PERIOD_DAYS
                             , MARKET_LEASE_PROFILE_NAME
                             , RECOVERY_STRUCTURE_NAME
                             , CPI_TYPE
                             , CPI_TIMING
                             , CPI_RATE_OR_INDEX
                             , LDTS
                             , RSRC)
      VALUES (MD5_AE_LeaseId
            , LeaseInfoHashDiff
            , LeaseAggregationKey
            , IsBaseLease
            , TenantName
            , Suite
            , Tenure
            , LeaseType
            , CustomLeaseType
            , LeaseBegin
            , LeaseExpiry
            , ExpiryType
            , EarliestBreak
            , RemainingTermDays
            , LeaseStatus
            , LeasePeriodDays
            , MarketLeaseProfileName
            , RecoveryStructureName
            , CpiType
            , CpiTiming
            , CpiRateOrIndex
            , LoadDTS
            , StageRecSrc)
SELECT DISTINCT
    MD5(TLI.PropertyId || '^' || TLI.LeaseId) AS MD5_AE_LeaseId
  , MD5(UPPER(COALESCE(TO_VARCHAR(TLI.LeaseAggregationKey), '')
    || '^' || COALESCE(TO_VARCHAR(TLI.IsBaseLease), '')
    || '^' || COALESCE(TO_VARCHAR(TLI.TenantName), '')
    || '^' || COALESCE(TO_VARCHAR(TLI.Suite), '')
    || '^' || COALESCE(TO_VARCHAR(TLI.Tenure), '')
    || '^' || COALESCE(TO_VARCHAR(TLI.LeaseType), '')
    || '^' || COALESCE(TO_VARCHAR(TLI.CustomLeaseType), '')
    || '^' || COALESCE(TO_VARCHAR(TLI.LeaseBegin), '')
    || '^' || COALESCE(TO_VARCHAR(TLI.LeaseExpiry), '')
    || '^' || COALESCE(TO_VARCHAR(TLI.ExpiryType), '')
    || '^' || COALESCE(TO_VARCHAR(TLI.EarliestBreak), '')
    || '^' || COALESCE(TO_VARCHAR(TLI.RemainingTermDays), '')
    || '^' || COALESCE(TO_VARCHAR(TLI.LeaseStatus), '')
    || '^' || COALESCE(TO_VARCHAR(TLI.LeasePeriodDays), '')
    || '^' || COALESCE(TO_VARCHAR(TLI.MarketLeaseProfileName), '')
    || '^' || COALESCE(TO_VARCHAR(TLI.RecoveryStructureName), '')
    || '^' || COALESCE(TO_VARCHAR(TLI.CpiType), '')
    || '^' || COALESCE(TO_VARCHAR(TLI.CpiTiming), '')
    || '^' || COALESCE(TO_VARCHAR(TLI.CpiRateOrIndex), '')
    )) AS LeaseInfoHashDiff
  , MD5(UPPER(TLI.LeaseExternalId)) AS MD5_ExternalLeaseId
  , TLI.LeaseId
  , TLI.LeaseExternalId
  , TLI.PropertyId
  , TLI.LeaseAggregationKey
  , TLI.IsBaseLease
  , TLI.TenantName
  , TLI.Suite
  , TLI.Tenure
  , TLI.LeaseType
  , TLI.CustomLeaseType
  , TLI.LeaseBegin
  , TLI.LeaseExpiry
  , TLI.ExpiryType
  , TLI.EarliestBreak
  , TLI.RemainingTermDays
  , TLI.LeaseStatus
  , TLI.LeasePeriodDays
  , TLI.MarketLeaseProfileName
  , TLI.RecoveryStructureName
  , TLI.CpiType
  , TLI.CpiTiming
  , TLI.CpiRateOrIndex
  , TLI.LoadDTS
  , TLI.StageRecSrc
FROM
    transient_ae_lease_info TLI
WHERE
    TLI.PropertyId IS NOT NULL
    AND TLI.LeaseId IS NOT NULL
    AND TLI.LeaseExternalId IS NOT NULL
;

INSERT ALL
  -- Load AE lease payments
  -- SAT_AE_LEASE_PAYMENT
  WHEN (SELECT COUNT(*) FROM SAT_AE_LEASE_PAYMENT SLP WHERE SLP.MD5_HUB_AE_LEASE = MD5_AE_LeaseId AND SLP.HASH_DIFF = LeasePaymentHashDiff) = 0
  THEN
    INTO SAT_AE_LEASE_PAYMENT (MD5_HUB_AE_LEASE
                             , HASH_DIFF
                             , CURRENCY_BASIS
                             , IS_ASSURED_RESULTSET
                             , LINE_ITEM_TYPE_ID
                             , LINE_ITEM_TYPE_NAME
                             , AMOUNT
                             , DATE
                             , LDTS
                             , RSRC)
      VALUES (MD5_AE_LeaseId
            , LeasePaymentHashDiff
            , CurrencyBasis
            , IsAssuredResultSet
            , LineItemType
            , LineItemName
            , Amount
            , Date
            , LoadDTS
            , StageRecSrc)
SELECT
    MD5(TLP.PropertyId || '^' || TLP.LeaseId) AS MD5_AE_LeaseId
  , MD5(TLP.PropertyId) AS MD5_AE_PropertyId
  , MD5(UPPER(COALESCE(TO_VARCHAR(TLP.CurrencyBasis), '')
    || '^' || COALESCE(TO_VARCHAR(TLP.IsAssuredResultSet), '')
    || '^' || COALESCE(TO_VARCHAR(TLP.LineItemType), '')
    || '^' || COALESCE(TO_VARCHAR(TLP.LineItemName), '')
    || '^' || COALESCE(TO_VARCHAR(TLP.Amount), '')
    || '^' || COALESCE(TO_VARCHAR(TLP.Date), '')
    )) AS LeasePaymentHashDiff
  , TLP.CurrencyBasis
  , TLP.IsAssuredResultSet
  , TLP.LineItemType
  , TLP.LineItemName
  , TLP.Amount
  , TLP.Date
  , TLP.LoadDTS
  , TLP.StageRecSrc
FROM
    transient_ae_lease_payment TLP
WHERE
    TLP.PropertyId IS NOT NULL
    AND TLP.LeaseId IS NOT NULL
;

INSERT ALL
  -- Load AE lease cash flows
  -- SAT_AE_LEASE_CASHFLOW
  WHEN (SELECT COUNT(*) FROM SAT_AE_LEASE_CASHFLOW SLC WHERE SLC.MD5_HUB_AE_LEASE = MD5_AE_LeaseId AND SLC.HASH_DIFF = LeaseCashFlowHashDiff) = 0
  THEN
    INTO SAT_AE_LEASE_CASHFLOW (MD5_HUB_AE_LEASE
                              , HASH_DIFF
                              , CURRENCY_BASIS
                              , IS_ASSURED_RESULTSET
                              , LINE_ITEM_TYPE_ID
                              , LINE_ITEM_TYPE_NAME
                              , LEASE_BEGIN_VALUE
                              , VALUATION_DATE_VALUE
                              , LEASE_EXPIRY_VALUE
                              , RESALE_VALUE
                              , UNIT_OF_MEASURE
                              , AMOUNT
                              , DATE
                              , LDTS
                              , RSRC)
      VALUES (MD5_AE_LeaseId
            , LeaseCashFlowHashDiff
            , CurrencyBasis
            , IsAssuredResultSet
            , LineItemType
            , LineItemName
            , LeaseBeginValue
            , ValuationDateValue
            , LeaseExpiryValue
            , ResaleValue
            , UnitOfMeasure
            , Amount
            , Date
            , LoadDTS
            , StageRecSrc)
SELECT
    MD5(TLC.PropertyId || '^' || TLC.LeaseId) AS MD5_AE_LeaseId
  , MD5(UPPER(COALESCE(TO_VARCHAR(TLC.CurrencyBasis), '')
    || '^' || COALESCE(TO_VARCHAR(TLC.IsAssuredResultSet), '')
    || '^' || COALESCE(TO_VARCHAR(TLC.LineItemType), '')
    || '^' || COALESCE(TO_VARCHAR(TLC.LineItemName), '')
    || '^' || COALESCE(TO_VARCHAR(TLC.LeaseBeginValue), '')
    || '^' || COALESCE(TO_VARCHAR(TLC.ValuationDateValue), '')
    || '^' || COALESCE(TO_VARCHAR(TLC.LeaseExpiryValue), '')
    || '^' || COALESCE(TO_VARCHAR(TLC.ResaleValue), '')
    || '^' || COALESCE(TO_VARCHAR(TLC.UnitOfMeasure), '')
    || '^' || COALESCE(TO_VARCHAR(TLC.Amount), '')
    || '^' || COALESCE(TO_VARCHAR(TLC.Date), '')
    )) AS LeaseCashFlowHashDiff
  , TLC.CurrencyBasis
  , TLC.IsAssuredResultSet
  , TLC.LineItemType
  , TLC.LineItemName
  , TLC.LeaseBeginValue
  , TLC.ValuationDateValue
  , TLC.LeaseExpiryValue
  , TLC.ResaleValue
  , TLC.UnitOfMeasure
  , TLC.Amount
  , TLC.Date
  , TLC.LoadDTS
  , TLC.StageRecSrc
FROM
    transient_ae_lease_annualised_cash_flows TLC
WHERE
    TLC.PropertyId IS NOT NULL
    AND TLC.LeaseId IS NOT NULL
;

INSERT ALL
  -- Loan data for AE loan info
  -- SAT_AE_LOAN_DETAILS
  WHEN (SELECT COUNT(*) FROM SAT_AE_LOAN_DETAILS SLD WHERE SLD.MD5_HUB_AE_LOAN = MD5_AE_LoanId AND SLD.HASH_DIFF = LoanDetailsHashDiff) = 0
  THEN
    INTO SAT_AE_LOAN_DETAILS (MD5_HUB_AE_LOAN
                            , HASH_DIFF
                            , LOAN_NAME
                            , LOAN_TYPE
                            , LOAN_TYPE_ENUM
                            , SENIORITY
                            , HOW_INPUT
                            , HOW_INPUT_ENUM
                            , LOAN_DATE
                            , LOAN_END
                            , LDTS
                            , RSRC)
      VALUES (MD5_AE_LoanId
            , LoanDetailsHashDiff
            , LoanName
            , LoanType
            , LoanTypeEnum
            , Seniority
            , HowInput
            , HowInputEnum
            , LoanDate
            , LoanEnd
            , LoadDTS
            , StageRecSrc
      )
SELECT
    MD5(TOI.PropertyId || '^' || TOI.LoanID) AS MD5_AE_LoanId
  , MD5(UPPER(COALESCE(TO_VARCHAR(TOI.LoanName), '')
    || '^' || COALESCE(TO_VARCHAR(TOI.LoanType), '')
    || '^' || COALESCE(TO_VARCHAR(TOI.LoanTypeEnum), '')
    || '^' || COALESCE(TO_VARCHAR(TOI.Seniority), '')
    || '^' || COALESCE(TO_VARCHAR(TOI.HowInput), '')
    || '^' || COALESCE(TO_VARCHAR(TOI.HowInputEnum), '')
    || '^' || COALESCE(TO_VARCHAR(TOI.LoanDate), '')
    || '^' || COALESCE(TO_VARCHAR(TOI.LoanEnd), '')
    )) AS LoanDetailsHashDiff
  , TOI.LoanName
  , TOI.LoanType
  , TOI.LoanTypeEnum
  , TOI.Seniority
  , TOI.HowInput
  , TOI.HowInputEnum
  , TOI.LoanDate
  , TOI.LoanEnd
  , TOI.LoadDTS
  , TOI.StageRecSrc
FROM
    transient_ae_loan_info TOI
WHERE
    TOI.PropertyId IS NOT NULL
    AND TOI.LoanId IS NOT NULL
;

INSERT ALL
  -- Load data for AE loan payments
  -- SAT_AE_LOAN_PAYMENT
  WHEN (SELECT COUNT(*) FROM SAT_AE_LOAN_PAYMENT SLP WHERE SLP.MD5_HUB_AE_LOAN = MD5_AE_LoanId AND SLP.HASH_DIFF = LoanPaymentHashDiff) = 0
  THEN
    INTO SAT_AE_LOAN_PAYMENT (MD5_HUB_AE_LOAN
                            , HASH_DIFF
                            , CURRENCY_BASIS
                            , IS_ASSURED_RESULTSET
                            , RESULTSET_ID
                            , RESULTSET
                            , LINE_ITEM_TYPE
                            , LINE_ITEM_NAME
                            , ACCOUNT_CODE
                            , AMOUNT
                            , DATE
                            , LDTS
                            , RSRC)
    VALUES (MD5_AE_LoanId
          , LoanPaymentHashDiff
          , CurrencyBasis
          , IsAssuredResultSet
          , ResultSet
          , ResultSetEnum
          , LineItemType
          , LineItemName
          , AccountCode
          , Amount
          , Date
          , LoadDTS
          , StageRecSrc)
SELECT
    MD5(TOP.PropertyId || '^' || TOP.LoanID) AS MD5_AE_LoanId
  , MD5(UPPER(COALESCE(TO_VARCHAR(TOP.CurrencyBasis), '')
    || '^' || COALESCE(TO_VARCHAR(TOP.IsAssuredResultSet), '')
    || '^' || COALESCE(TO_VARCHAR(TOP.ResultSet), '')
    || '^' || COALESCE(TO_VARCHAR(TOP.ResultSetEnum), '')
    || '^' || COALESCE(TO_VARCHAR(TOP.LineItemType), '')
    || '^' || COALESCE(TO_VARCHAR(TOP.LineItemName), '')
    || '^' || COALESCE(TO_VARCHAR(TOP.AccountCode), '')
    || '^' || COALESCE(TO_VARCHAR(TOP.Amount), '')
    || '^' || COALESCE(TO_VARCHAR(TOP.Date), '')
  )) AS LoanPaymentHashDiff
  , TOP.CurrencyBasis
  , TOP.IsAssuredResultSet
  , TOP.ResultSet
  , TOP.ResultSetEnum
  , TOP.LineItemType
  , TOP.LineItemName
  , TOP.AccountCode
  , TOP.Amount
  , TOP.Date
  , TOP.LoadDTS
  , TOP.StageRecSrc
FROM
    transient_ae_loan_payment TOP
WHERE
    TOP.PropertyId IS NOT NULL
    AND TOP.LoanId IS NOT NULL
;

INSERT ALL
  -- Load data for AE property version
  -- SAT_AE_PROPERTY_VERSION
  WHEN (SELECT COUNT(*) FROM SAT_AE_PROPERTY_VERSION PV WHERE PV.MD5_HUB_AE_PROPERTY = MD5_AE_PropertyId AND PV.HASH_DIFF = PropertyVersionHashDiff) = 0
  THEN
    INTO SAT_AE_PROPERTY_VERSION (MD5_HUB_AE_PROPERTY
                                , HASH_DIFF
                                , PROPERTY_VERSION
                                , LDTS
                                , RSRC)
      VALUES (MD5_AE_PropertyId
            , PropertyVersionHashDiff
            , PropertyVersion
            , LoadDTS
            , StageRecSrc)
SELECT
    MD5(TPI.PropertyId) AS MD5_AE_PropertyId
  , MD5(UPPER(COALESCE(TO_VARCHAR(TPI.PropertyVersion), '')
    )) AS PropertyVersionHashDiff
  , TPI.PropertyVersion
  , TPI.LoadDTS
  , TPI.StageRecSrc
FROM
    transient_ae_property_info TPI;

INSERT ALL
  -- Load data for the AE lease property version
  -- SAT_AE_LEASE_VERSION
  WHEN (SELECT COUNT(*) FROM SAT_AE_LEASE_VERSION LV WHERE LV.MD5_HUB_AE_LEASE = MD5_AE_LeaseId and LV.HASH_DIFF = LeaseVersionHashDiff) = 0
  THEN
    INTO SAT_AE_LEASE_VERSION (MD5_HUB_AE_LEASE
                             , HASH_DIFF
                             , PROPERTY_VERSION
                             , LDTS
                             , RSRC)
      VALUES (MD5_AE_LeaseId
            , LeaseVersionHashDiff
            , PropertyVersion
            , LoadDTS
            , StageRecSrc)
SELECT
    MD5(TLI.PropertyId || '^' || TLI.LeaseId) AS MD5_AE_LeaseId
  , MD5(UPPER(COALESCE(TO_VARCHAR(TPI.PropertyVersion), '')
    )) AS LeaseVersionHashDiff
  , TPI.PropertyVersion
  , TLI.LoadDTS
  , TLI.StageRecSrc
FROM
    transient_ae_lease_info TLI
    INNER JOIN transient_ae_property_info TPI on TLI.PROPERTYID = TPI.PROPERTYID;

INSERT ALL
  -- Load data for the AE loan property version
  -- SAT_AE_LOAN_VERSION
  WHEN (SELECT COUNT(*) FROM SAT_AE_LOAN_VERSION LV WHERE LV.MD5_HUB_AE_LOAN = MD5_AE_LoanId and LV.HASH_DIFF = LoanVersionHashDiff) = 0
  THEN
    INTO SAT_AE_LOAN_VERSION (MD5_HUB_AE_LOAN
                            , HASH_DIFF
                            , PROPERTY_VERSION
                            , LDTS
                            , RSRC)
      VALUES (MD5_AE_LoanId
            , LoanVersionHashDiff
            , PropertyVersion
            , LoadDTS
            , StageRecSrc)
SELECT
    MD5(TLI.PropertyId || '^' || TLI.LoanId) AS MD5_AE_LoanId
  , MD5(UPPER(COALESCE(TO_VARCHAR(TPI.PropertyVersion), '')
    )) AS LoanVersionHashDiff
  , TPI.PropertyVersion
  , TLI.LoadDTS
  , TLI.StageRecSrc
FROM
    transient_ae_loan_info TLI
    INNER JOIN transient_ae_property_info TPI on TLI.PROPERTYID = TPI.PROPERTYID;

INSERT ALL
  -- Load data for the AE revex property version
  -- SAT_AE_REVEX_VERSION
  WHEN (SELECT COUNT(*) FROM SAT_AE_REVEX_VERSION RV WHERE RV.MD5_HUB_AE_REVEX = MD5_AE_RevexId and RV.HASH_DIFF = RevexVersionHashDiff) = 0
  THEN
    INTO SAT_AE_REVEX_VERSION (MD5_HUB_AE_REVEX
                             , HASH_DIFF
                             , PROPERTY_VERSION
                             , LDTS
                             , RSRC)
      VALUES (MD5_AE_RevexId
            , RevexVersionHashDiff
            , PropertyVersion
            , LoadDTS
            , StageRecSrc)
SELECT
    MD5(TRI.PropertyId || '^' || TRI.RevExId) AS MD5_AE_RevExId
  , MD5(UPPER(COALESCE(TO_VARCHAR(TPI.PropertyVersion), '')
    )) AS RevexVersionHashDiff
  , TPI.PropertyVersion
  , TRI.LoadDTS
  , TRI.StageRecSrc
FROM
    transient_ae_revex_info TRI
    INNER JOIN transient_ae_property_info TPI on TRI.PROPERTYID = TPI.PROPERTYID;

INSERT ALL
  -- Load data for AE line item type details
  -- SAT_AE_LINE_ITEM_TYPE_DETAILS
  WHEN (SELECT COUNT(*) FROM SAT_AE_LINE_ITEM_TYPE_DETAILS SD WHERE SD.MD5_HUB_AE_LINE_ITEM_TYPE = MD5_AE_LineItemType AND SD.HASH_DIFF = MD5_LineItemTypeDetailsHashDiff) = 0
  THEN
    INTO SAT_AE_LINE_ITEM_TYPE_DETAILS (MD5_HUB_AE_LINE_ITEM_TYPE
                                    , HASH_DIFF
                                    , LINE_ITEM_TYPE_ID
                                    , LINE_ITEM_TYPE_PARENT_ID
                                    , CLASS_TYPE
                                    , ACCOUNT_RELATION_TYPE
                                    , SORT_ORDER
                                    , REPORT_DESCRIPTION
                                    , REPORT_GROUP
                                    , SHOW_ENTITIES
                                    , IS_HEADER
                                    , COST_CODE_TYPE
                                    , LINEITEM_DEPTH
                                    , UNIT_OF_MEASURE_TYPE
                                    , LINEITEM_AGGREGATION_TYPE
                                    , RESULT_STORAGE_LOCATION
                                    , SECONDARY_RESULT_STORAGE_LOCATION
                                    , THIRD_RESULT_STORAGE_LOCATION
                                    , ACCOUNT_CODE
                                    , LDTS
                                    , RSRC)
    VALUES (MD5_AE_LineItemType
          , MD5_LineItemTypeDetailsHashDiff
          , LineItemTypeId
          , LineItemParentID
          , ClassType
          , AccountRelationType
          , SortOrder
          , ReportDescription
          , ReportGroup
          , ShowEntities
          , IsHeader
          , CostCodeType
          , LineItemDepth
          , UnitOfMeasureType
          , LineItemAggregationType
          , ResultStorageLocation
          , SecondaryResultStorageLocation
          , ThirdResultStorageLocation
          , AccountCode
          , LoadDTS
          , StageRecSrc)
SELECT DISTINCT
    MD5(T.LineItemTypeName) AS MD5_AE_LineItemType
  , MD5(UPPER(COALESCE(TO_VARCHAR(T.LineItemTypeId), '')
    || '^' || COALESCE(TO_VARCHAR(T.LineItemParentID), '')
    || '^' || COALESCE(TO_VARCHAR(T.ClassType), '')
    || '^' || COALESCE(TO_VARCHAR(T.AccountRelationType), '')
    || '^' || COALESCE(TO_VARCHAR(T.SortOrder), '')
    || '^' || COALESCE(TO_VARCHAR(T.ReportDescription), '')
    || '^' || COALESCE(TO_VARCHAR(T.ReportGroup), '')
    || '^' || COALESCE(TO_VARCHAR(T.ShowEntities), '')
    || '^' || COALESCE(TO_VARCHAR(T.IsHeader), '')
    || '^' || COALESCE(TO_VARCHAR(T.CostCodeType), '')
    || '^' || COALESCE(TO_VARCHAR(T.LineItemDepth), '')
    || '^' || COALESCE(TO_VARCHAR(T.UnitOfMeasureType), '')
    || '^' || COALESCE(TO_VARCHAR(T.LineItemAggregationType), '')
    || '^' || COALESCE(TO_VARCHAR(T.ResultStorageLocation), '')
    || '^' || COALESCE(TO_VARCHAR(T.SecondaryResultStorageLocation), '')
    || '^' || COALESCE(TO_VARCHAR(T.ThirdResultStorageLocation), '')
    || '^' || COALESCE(TO_VARCHAR(T.AccountCode), '')
    )) AS MD5_LineItemTypeDetailsHashDiff
  , T.LineItemTypeId
  , T.LineItemParentID
  , T.ClassType
  , T.AccountRelationType
  , T.SortOrder
  , T.ReportDescription
  , T.ReportGroup
  , T.ShowEntities
  , T.IsHeader
  , T.CostCodeType
  , T.LineItemDepth
  , T.UnitOfMeasureType
  , T.LineItemAggregationType
  , T.ResultStorageLocation
  , T.SecondaryResultStorageLocation
  , T.ThirdResultStorageLocation
  , T.AccountCode
  , T.LoadDTS
  , T.StageRecSrc
FROM
    transient_ae_lineitemtype T
;

/* *********************************************************************************** */
/* *** PITs ************************************************************************** */
/* *********************************************************************************** */

INSERT ALL
  -- Load data for AE property PIT
  -- PIT_AE_PROPERTY
  WHEN (SELECT COUNT(*) FROM PIT_AE_PROPERTY PAP WHERE PAP.MD5_HUB_AE_PROPERTY = MD5_AE_PropertyId AND PAP.PROPERTY_VERSION = PropertyVersion) = 0
  THEN
    INTO PIT_AE_PROPERTY (MD5_HUB_AE_PROPERTY
                        , PROPERTY_VERSION
                        , LDTS_SAT_AE_METADATA_PROPERTY
                        , LDTS_SAT_AE_PROPERTY_CASHFLOW
                        , LDTS_SAT_AE_PROPERTY_DETAILS
                        , LDTS_SAT_AE_PROPERTY_KPI
                        , LDTS_SAT_AE_PROPERTY_LOCATION
                        , LDTS_SAT_AE_PROPERTY_PAYMENT
                        , LDTS)
    VALUES (MD5_AE_PropertyId
          , PropertyVersion
          , LDTS_SAT_AE_METADATA_PROPERTY
          , LDTS_SAT_AE_PROPERTY_CASHFLOW
          , LDTS_SAT_AE_PROPERTY_DETAILS
          , LDTS_SAT_AE_PROPERTY_KPI
          , LDTS_SAT_AE_PROPERTY_LOCATION
          , LDTS_SAT_AE_PROPERTY_PAYMENT
          , LDTS)
SELECT
    PV.MD5_HUB_AE_PROPERTY AS MD5_AE_PropertyId
  , PV.PROPERTY_VERSION AS PropertyVersion
  , PM.LDTS_SAT_AE_METADATA_PROPERTY
  , PC.LDTS_SAT_AE_PROPERTY_CASHFLOW
  , PD.LDTS_SAT_AE_PROPERTY_DETAILS
  , PK.LDTS_SAT_AE_PROPERTY_KPI
  , PL.LDTS_SAT_AE_PROPERTY_LOCATION
  , PP.LDTS_SAT_AE_PROPERTY_PAYMENT
  , PV.LDTS
FROM
    SAT_AE_PROPERTY_VERSION PV
    LEFT JOIN (SELECT MD5_HUB_AE_PROPERTY, MAX(LDTS) AS LDTS_SAT_AE_METADATA_PROPERTY FROM SAT_AE_METADATA_PROPERTY GROUP BY MD5_HUB_AE_PROPERTY) AS PM ON PV.MD5_HUB_AE_PROPERTY = PM.MD5_HUB_AE_PROPERTY
    LEFT JOIN (SELECT MD5_HUB_AE_PROPERTY, MAX(LDTS) AS LDTS_SAT_AE_PROPERTY_CASHFLOW FROM SAT_AE_PROPERTY_CASHFLOW GROUP BY MD5_HUB_AE_PROPERTY) AS PC ON PV.MD5_HUB_AE_PROPERTY = PC.MD5_HUB_AE_PROPERTY
    LEFT JOIN (SELECT MD5_HUB_AE_PROPERTY, MAX(LDTS) AS LDTS_SAT_AE_PROPERTY_DETAILS  FROM SAT_AE_PROPERTY_DETAILS  GROUP BY MD5_HUB_AE_PROPERTY) AS PD ON PV.MD5_HUB_AE_PROPERTY = PD.MD5_HUB_AE_PROPERTY
    LEFT JOIN (SELECT MD5_HUB_AE_PROPERTY, MAX(LDTS) AS LDTS_SAT_AE_PROPERTY_KPI      FROM SAT_AE_PROPERTY_KPI      GROUP BY MD5_HUB_AE_PROPERTY) AS PK ON PV.MD5_HUB_AE_PROPERTY = PK.MD5_HUB_AE_PROPERTY
    LEFT JOIN (SELECT MD5_HUB_AE_PROPERTY, MAX(LDTS) AS LDTS_SAT_AE_PROPERTY_LOCATION FROM SAT_AE_PROPERTY_LOCATION GROUP BY MD5_HUB_AE_PROPERTY) AS PL ON PV.MD5_HUB_AE_PROPERTY = PL.MD5_HUB_AE_PROPERTY
    LEFT JOIN (SELECT MD5_HUB_AE_PROPERTY, MAX(LDTS) AS LDTS_SAT_AE_PROPERTY_PAYMENT  FROM SAT_AE_PROPERTY_PAYMENT  GROUP BY MD5_HUB_AE_PROPERTY) AS PP ON PV.MD5_HUB_AE_PROPERTY = PP.MD5_HUB_AE_PROPERTY
WHERE
    PV.LDTS = $LoadDTS
;

INSERT ALL
  -- Load data for AE lease PIT
  -- PIT_AE_LEASE
  WHEN (SELECT COUNT(*) FROM PIT_AE_LEASE PAL WHERE PAL.MD5_HUB_AE_LEASE = MD5_AE_LeaseId AND PAL.PROPERTY_VERSION = PropertyVersion) = 0
  THEN
    INTO PIT_AE_LEASE (MD5_HUB_AE_LEASE
                     , PROPERTY_VERSION
                     , LDTS_SAT_AE_LEASE_CASHFLOW
                     , LDTS_SAT_AE_LEASE_DETAILS
                     , LDTS_SAT_AE_LEASE_PAYMENT
                     , LDTS)
    VALUES (MD5_AE_LeaseId
          , PropertyVersion
          , LDTS_SAT_AE_LEASE_CASHFLOW
          , LDTS_SAT_AE_LEASE_DETAILS
          , LDTS_SAT_AE_LEASE_PAYMENT
          , LDTS)
SELECT
    LV.MD5_HUB_AE_LEASE AS MD5_AE_LeaseId
  , LV.PROPERTY_VERSION AS PropertyVersion
  , LC.LDTS_SAT_AE_LEASE_CASHFLOW
  , LD.LDTS_SAT_AE_LEASE_DETAILS
  , LP.LDTS_SAT_AE_LEASE_PAYMENT
  , LV.LDTS
FROM
    SAT_AE_LEASE_VERSION LV
    LEFT JOIN (SELECT MD5_HUB_AE_LEASE, MAX(LDTS) AS LDTS_SAT_AE_LEASE_CASHFLOW FROM SAT_AE_LEASE_CASHFLOW GROUP BY MD5_HUB_AE_LEASE) AS LC ON LV.MD5_HUB_AE_LEASE = LC.MD5_HUB_AE_LEASE
    LEFT JOIN (SELECT MD5_HUB_AE_LEASE, MAX(LDTS) AS LDTS_SAT_AE_LEASE_DETAILS  FROM SAT_AE_LEASE_DETAILS  GROUP BY MD5_HUB_AE_LEASE) AS LD ON LV.MD5_HUB_AE_LEASE = LD.MD5_HUB_AE_LEASE
    LEFT JOIN (SELECT MD5_HUB_AE_LEASE, MAX(LDTS) AS LDTS_SAT_AE_LEASE_PAYMENT  FROM SAT_AE_LEASE_PAYMENT  GROUP BY MD5_HUB_AE_LEASE) AS LP ON LV.MD5_HUB_AE_LEASE = LP.MD5_HUB_AE_LEASE
WHERE
    LV.LDTS = $LoadDTS
;

INSERT ALL
  -- Load data for AE loan PIT
  -- PIT_AE_LOAN
  WHEN (SELECT COUNT(*) FROM PIT_AE_LOAN PAL WHERE PAL.MD5_HUB_AE_LOAN = MD5_AE_LoanId AND PAL.PROPERTY_VERSION = PropertyVersion) = 0
  THEN
    INTO PIT_AE_LOAN (MD5_HUB_AE_LOAN
                    , PROPERTY_VERSION
                    , LDTS_SAT_AE_LOAN_DETAILS
                    , LDTS_SAT_AE_LOAN_PAYMENT
                    , LDTS)
    VALUES (MD5_AE_LoanId
          , PropertyVersion
          , LDTS_SAT_AE_LOAN_DETAILS
          , LDTS_SAT_AE_LOAN_PAYMENT
          , LDTS)
SELECT
    LV.MD5_HUB_AE_LOAN AS MD5_AE_LoanId
  , LV.PROPERTY_VERSION AS PropertyVersion
  , LD.LDTS_SAT_AE_LOAN_DETAILS
  , LP.LDTS_SAT_AE_LOAN_PAYMENT
  , LV.LDTS
FROM
    SAT_AE_LOAN_VERSION LV
    LEFT JOIN (SELECT MD5_HUB_AE_LOAN, MAX(LDTS) AS LDTS_SAT_AE_LOAN_DETAILS FROM SAT_AE_LOAN_DETAILS GROUP BY MD5_HUB_AE_LOAN) AS LD ON LV.MD5_HUB_AE_LOAN = LD.MD5_HUB_AE_LOAN
    LEFT JOIN (SELECT MD5_HUB_AE_LOAN, MAX(LDTS) AS LDTS_SAT_AE_LOAN_PAYMENT FROM SAT_AE_LOAN_PAYMENT GROUP BY MD5_HUB_AE_LOAN) AS LP ON LV.MD5_HUB_AE_LOAN = LP.MD5_HUB_AE_LOAN
WHERE
    LV.LDTS = $LoadDTS
;

INSERT ALL
  -- Load data for AE revex PIT
  -- PIT_AE_REVEX
  WHEN (SELECT COUNT(*) FROM PIT_AE_REVEX PAL WHERE PAL.MD5_HUB_AE_REVEX = MD5_AE_RevExId AND PAL.PROPERTY_VERSION = PropertyVersion) = 0
  THEN
    INTO PIT_AE_REVEX (MD5_HUB_AE_REVEX
                     , PROPERTY_VERSION
                     , LDTS_SAT_AE_REVEX_CASHFLOW
                     , LDTS_SAT_AE_REVEX_DETAILS
                     , LDTS_SAT_AE_REVEX_PAYMENT
                     , LDTS)
    VALUES (MD5_AE_RevExId
          , PropertyVersion
          , LDTS_SAT_AE_REVEX_CASHFLOW
          , LDTS_SAT_AE_REVEX_DETAILS
          , LDTS_SAT_AE_REVEX_PAYMENT
          , LDTS)
SELECT
    RV.MD5_HUB_AE_REVEX AS MD5_AE_RevExId
  , RV.PROPERTY_VERSION AS PropertyVersion
  , RC.LDTS_SAT_AE_REVEX_CASHFLOW
  , RD.LDTS_SAT_AE_REVEX_DETAILS
  , RP.LDTS_SAT_AE_REVEX_PAYMENT
  , RV.LDTS
FROM
    SAT_AE_REVEX_VERSION RV
    LEFT JOIN (SELECT MD5_HUB_AE_REVEX, MAX(LDTS) AS LDTS_SAT_AE_REVEX_CASHFLOW FROM SAT_AE_REVEX_CASHFLOW GROUP BY MD5_HUB_AE_REVEX) AS RC ON RV.MD5_HUB_AE_REVEX = RC.MD5_HUB_AE_REVEX
    LEFT JOIN (SELECT MD5_HUB_AE_REVEX, MAX(LDTS) AS LDTS_SAT_AE_REVEX_DETAILS  FROM SAT_AE_REVEX_DETAILS  GROUP BY MD5_HUB_AE_REVEX) AS RD ON RV.MD5_HUB_AE_REVEX = RD.MD5_HUB_AE_REVEX
    LEFT JOIN (SELECT MD5_HUB_AE_REVEX, MAX(LDTS) AS LDTS_SAT_AE_REVEX_PAYMENT  FROM SAT_AE_REVEX_PAYMENT  GROUP BY MD5_HUB_AE_REVEX) AS RP ON RV.MD5_HUB_AE_REVEX = RP.MD5_HUB_AE_REVEX
WHERE
    RV.LDTS = $LoadDTS
;

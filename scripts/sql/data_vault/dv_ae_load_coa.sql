-- Remove previous records. Only use DELETE not TRUNCATE.
-- The former preserves the memory of processed files in Snowflake.
DELETE FROM ae_coa_data_raw;

-- Copy data from S3 onto table
COPY INTO ae_coa_data_raw
FROM @ae_coa_data
  FILE_FORMAT = (type = json);

/* *********************************************************************************** */
/* *** VARIABLES ********************************************************************* */
/* *********************************************************************************** */

ALTER SESSION SET TIMEZONE = 'UTC';
SET LoadDTS = CURRENT_TIMESTAMP;

-- Temporary variables required where source files are not currently providing data
-- Change these as required for testing, e.g. incrementing version number
-- FIXME
SET CoAVersion = 1; -- Required anywhere coa data or children of coa data is required, as everything hangs off the coa version.
SET CoALastModifiedBy = 'coa.user@example.com';
SET CoALastModifiedDate = $LoadDTS;

/* *********************************************************************************** */
/* *** TEMP TABLES ******************************************************************* */
/* *********************************************************************************** */

-- FIXME tables have a `tmp_` prefix so they are not deleted at the end of the workflow
-- the `tmp_` prefix must be removed when the COA will be loaded into the data vault

CREATE OR REPLACE TRANSIENT TABLE tmp_transient_coa_info AS (
  SELECT
    $LoadDTS AS LoadDTS
  , 'AE_COA_EXPORT' AS StageRecSrc
  , src:Id::integer AS ChartOfAccountId
  , src:Name::string AS ChartOfAccountName
  , src:Description::string AS Description
  , src:IsActive::boolean AS IsActive
  , src:LastModifiedTime::timestamp AS LastModifiedTime
  , src:ExternalId::string AS ExternalId
  , $CoALastModifiedBy AS CoALastModifiedBy // To do: replace with correct element
  , $CoALastModifiedDate AS CoALastModifiedDate // To do: replace with correct element
FROM ae_coa_data_raw
);

CREATE OR REPLACE TRANSIENT TABLE tmp_transient_coa_accounts AS (
  SELECT
    $LoadDTS AS LoadDTS
  , 'AE_COA_EXPORT' AS StageRecSrc
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
);

CREATE OR REPLACE TRANSIENT TABLE tmp_transient_coa_tenant_account_code_categories AS (
  SELECT
    $LoadDTS AS LoadDTS
  , 'AE_COA_EXPORT' AS StageRecSrc
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
);

CREATE OR REPLACE TRANSIENT TABLE tmp_transient_coa_multi_family_tenant_account_code_categories AS (
  SELECT
    $LoadDTS AS LoadDTS
  , 'AE_COA_EXPORT' AS StageRecSrc
  , vm.value:Id::integer AS MFTCACId
  , vm.value:ChartOfAccountId::integer AS ChartOfAccountId
  , vm.value:Name::string AS Name
  , vm.value:MultiFamilyTenantAccountCodeCategories::string AS MultiFamilyTenantAccountCodeCategories
  , vm.value:MultiFamilyFreeRentAccount::string AS MultiFamilyFreeRentAccount
  , vm.value:MultiFamilyScheduledRentAccount::string AS MultiFamilyScheduledRentAccount
  , vm.value:MultiFamiliyAdditionalConcessionsAccount::string AS MultiFamiliyAdditionalConcessionsAccount
  , vm.value:MultiFamilyUnitImprovementsAccount::string AS MultiFamilyUnitImprovementsAccount
  , vm.value:MultiFamilyUnitLeasingCostsAccount::string AS MultiFamilyUnitLeasingCostsAccount
  , vm.value:CanDelete::boolean AS CanDelete
FROM ae_coa_data_raw
, lateral flatten (input => src:MultiFamilyTenantAccountCodeCategories) vm
)

---- TODO
--
--/* *********************************************************************************** */
--/* *** LOAD DATA ********************************************************************* */
--/* *********************************************************************************** */
--
--/* *********************************************************************************** */
--/* *** HUBs ************************************************************************** */
--/* *********************************************************************************** */
--
--
--/* *********************************************************************************** */
--/* *** LINKs ************************************************************************* */
--/* *********************************************************************************** */
--
--
--/* *********************************************************************************** */
--/* *** SATELLITEs ******************************************************************** */
--/* *********************************************************************************** */
--
--
--/* *********************************************************************************** */
--/* *** PITs ************************************************************************** */
--/* *********************************************************************************** */
;
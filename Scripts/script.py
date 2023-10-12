import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame
import boto3

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Microsoft SQL Server - Patient
MicrosoftSQLServer_node_patient = glueContext.create_dynamic_frame.from_catalog(
    database="sql_server_source",
    table_name="dw_users_dbo_patientmigrationengage",
    transformation_ctx="MicrosoftSQLServer_node_patient",
)

# Script generated for node Microsoft SQL Server - Addresses
MicrosoftSQLServer_node_addresses = glueContext.create_dynamic_frame.from_catalog(
    database="sql_server_source",
    table_name="dw_users_dbo_patientaddressmigrationengage",
    transformation_ctx="MicrosoftSQLServer_node_addresses",
)

# Script generated for node Microsoft SQL Server - Medication_list
MicrosoftSQLServer_node_medication_list = glueContext.create_dynamic_frame.from_catalog(
    database="sql_server_source",
    table_name="dw_users_dbo_patientmedicationsmigrationengage",
    transformation_ctx="MicrosoftSQLServer_node_medication_list",
)

# Script generated for node Microsoft SQL Server - Medication_cycles
MicrosoftSQLServer_node_medication_cycles = glueContext.create_dynamic_frame.from_catalog(
    database="sql_server_source",
    table_name="dw_users_dbo_patientmedicationscyclesmigrationengage",
    transformation_ctx="MicrosoftSQLServer_node_medication_cycles",
)

# Script generated for node Microsoft SQL Server - Practitioners
MicrosoftSQLServer_node_practitioners = glueContext.create_dynamic_frame.from_catalog(
    database="sql_server_source",
    table_name="dw_users_dbo_practitionersmigrationengage",
    transformation_ctx="MicrosoftSQLServer_node_practitioners",
)

# Script generated for node Microsoft SQL Server - Organizations
MicrosoftSQLServer_node_organizations = glueContext.create_dynamic_frame.from_catalog(
    database="sql_server_source",
    table_name="dw_users_dbo_organizationsmigrationengage",
    transformation_ctx="MicrosoftSQLServer_node_organizations",
)

# Script generated for node Microsoft SQL Server - Users
MicrosoftSQLServer_node_users = glueContext.create_dynamic_frame.from_catalog(
    database="sql_server_source",
    table_name="dw_users_dbo_usersmigrationengage",
    transformation_ctx="MicrosoftSQLServer_node_users",
)

# Script generated for node Evaluate Data Quality - Patient
EvaluateDataQuality_node_patient_ruleset = """
    Rules = [
    IsUnique "patient_id_source",
    IsComplete "mrn",
    ColumnValues "mrn" matches "[0-9]*",
    ColumnValues "dob" >= "1920-01-01",
    ColumnValues "gender" in ["M","F"],
    ColumnValues "clinician_npi" matches "[0-9]*",
    IsComplete "clinician_npi"
    ]
"""

EvaluateDataQuality_node_patient = EvaluateDataQuality().process_rows(
    frame=MicrosoftSQLServer_node_patient,
    ruleset=EvaluateDataQuality_node_patient_ruleset,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node_patient",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={"performanceTuning.caching": "CACHE_NOTHING"},
)

# Script generated for node Evaluate Data Quality - Addresses
EvaluateDataQuality_node_addresses_ruleset = """
    Rules = [
    IsUnique "patient_id_source",
    IsComplete "street_address",
    IsComplete "city",
    IsComplete "state",
    IsComplete "zipcode"
    ]
"""

EvaluateDataQuality_node_addresses = EvaluateDataQuality().process_rows(
    frame=MicrosoftSQLServer_node_addresses,
    ruleset=EvaluateDataQuality_node_addresses_ruleset,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node_addresses",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={"performanceTuning.caching": "CACHE_NOTHING"},
)

# Script generated for node Evaluate Data Quality - Medication_list
EvaluateDataQuality_node_medication_list_ruleset = """
    Rules = [
    IsComplete "ndc",
    IsComplete "creation_date",
    IsComplete "name",
    ColumnLength "name" <= 255,
    ColumnLength "direction" <= 150,
    ColumnValues "creation_date" > "1900-01-01"
    ]
"""

EvaluateDataQuality_node_medication_list = EvaluateDataQuality().process_rows(
    frame=MicrosoftSQLServer_node_medication_list,
    ruleset=EvaluateDataQuality_node_medication_list_ruleset,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node_medication_list",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={"performanceTuning.caching": "CACHE_NOTHING"},
)

# Script generated for node Evaluate Data Quality - Medication_cycles
EvaluateDataQuality_node_medication_cycles_ruleset = """
    Rules = [
    IsUnique "patient_id_source",
    IsComplete "dispensing_patient",
    IsComplete "start_date",
    ColumnValues "start_date" > "1900-01-01"
    ]
"""

EvaluateDataQuality_node_medication_cycles = EvaluateDataQuality().process_rows(
    frame=MicrosoftSQLServer_node_medication_cycles,
    ruleset=EvaluateDataQuality_node_medication_cycles_ruleset,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node_medication_cycles",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={"performanceTuning.caching": "CACHE_NOTHING"},
)

# Script generated for node Evaluate Data Quality - Practitioners
EvaluateDataQuality_node_practitioners_ruleset = """
    Rules = [
    IsComplete "npi",
    ColumnValues "npi" matches "[0-9]*",
    IsComplete "full_name",
    IsComplete "clinic_name",
    IsComplete "city",
    IsComplete "email",
    IsComplete "phone_number"
    ]
"""

EvaluateDataQuality_node_practitioners = EvaluateDataQuality().process_rows(
    frame=MicrosoftSQLServer_node_practitioners,
    ruleset=EvaluateDataQuality_node_practitioners_ruleset,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node_practitioners",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={"performanceTuning.caching": "CACHE_NOTHING"},
)

# Script generated for node Evaluate Data Quality - Organizations
EvaluateDataQuality_node_organizations_ruleset = """
    Rules = [
    IsComplete "name"
    ]
"""

EvaluateDataQuality_node_organizations = EvaluateDataQuality().process_rows(
    frame=MicrosoftSQLServer_node_organizations,
    ruleset=EvaluateDataQuality_node_organizations_ruleset,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node_organizations",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={"performanceTuning.caching": "CACHE_NOTHING"},
)

# Script generated for node Evaluate Data Quality - Users
EvaluateDataQuality_node_users_ruleset = """
    Rules = [
    IsComplete "prefix",
    IsComplete "email",
    IsComplete "full_name",
    IsComplete "phone_number"
    ]
"""

EvaluateDataQuality_node_users = EvaluateDataQuality().process_rows(
    frame=MicrosoftSQLServer_node_users,
    ruleset=EvaluateDataQuality_node_users_ruleset,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node_users",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={"performanceTuning.caching": "CACHE_NOTHING"},
)

bucket_name = 'srx-engage-s3-dev-migration'
file_name = 'test/date.txt'

# Crea un cliente de S3
s3_client = boto3.client('s3')

# Lee el contenido del archivo "date.txt" desde S3
response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
etl_start_date = response['Body'].read().decode('utf-8').strip()

# Script generated for node SQL Query - Patient
SqlQuery_patient = """
select * from myDataSource m
where COALESCE(m.update_at,m.created_at) <= '{}' -- Filtra registros con create_date mayor que etl_start_date
    """.format(etl_start_date)
SQLQuery_node_patient = sparkSqlQuery(
    glueContext,
    query=SqlQuery_patient,
    mapping={"myDataSource": MicrosoftSQLServer_node_patient},
    transformation_ctx="SQLQuery_node_patient",
)

# Script generated for node SQL Query - Addresses
SqlQuery_addresses = """
select * from myDataSource m
"""
SQLQuery_node_addresses = sparkSqlQuery(
    glueContext,
    query=SqlQuery_addresses,
    mapping={"myDataSource": MicrosoftSQLServer_node_addresses},
    transformation_ctx="SQLQuery_node_addresses",
)

# Script generated for node SQL Query - Medication_list
SqlQuery_medication_list = """
select * from myDataSource m
"""
SQLQuery_node_medication_list = sparkSqlQuery(
    glueContext,
    query=SqlQuery_medication_list,
    mapping={"myDataSource": MicrosoftSQLServer_node_medication_list},
    transformation_ctx="SQLQuery_node_medication_list",
)

# Script generated for node SQL Query - Medication_cycles
SqlQuery_medication_cycles = """
select * from myDataSource m
"""
SQLQuery_node_medication_cycles = sparkSqlQuery(
    glueContext,
    query=SqlQuery_medication_cycles,
    mapping={"myDataSource": MicrosoftSQLServer_node_medication_cycles},
    transformation_ctx="SQLQuery_node_medication_cycles",
)

# Script generated for node SQL Query - Practitioners
SqlQuery_practitioners = """
select * from myDataSource m
"""
SQLQuery_node_practitioners = sparkSqlQuery(
    glueContext,
    query=SqlQuery_practitioners,
    mapping={"myDataSource": MicrosoftSQLServer_node_practitioners},
    transformation_ctx="SQLQuery_node_practitioners",
)

# Script generated for node SQL Query - Organizations
SqlQuery_organizations = """
select * from myDataSource m
"""
SQLQuery_node_organizations = sparkSqlQuery(
    glueContext,
    query=SqlQuery_organizations,
    mapping={"myDataSource": MicrosoftSQLServer_node_organizations},
    transformation_ctx="SQLQuery_node_organizations",
)

# Script generated for node SQL Query - Users
SqlQuery_users = """
select * from myDataSource m
"""
SQLQuery_node_users = sparkSqlQuery(
    glueContext,
    query=SqlQuery_users,
    mapping={"myDataSource": MicrosoftSQLServer_node_users},
    transformation_ctx="SQLQuery_node_users",
)

# Script generated for node rowLevelOutcomes - Patient
rowLevelOutcomes_node_patient = SelectFromCollection.apply(
    dfc=EvaluateDataQuality_node_patient,
    key="rowLevelOutcomes",
    transformation_ctx="rowLevelOutcomes_node_patient",
)

rowLevelOutcomes_node_patient = rowLevelOutcomes_node_patient.resolveChoice(
    specs=[
        ("DataQualityRulesPass", "cast:string"),
        ("DataQualityRulesFail", "cast:string"),
        ("DataQualityRulesSkip", "cast:string")
    ],
    transformation_ctx="ConvertArraysToStrings_RowLevel_patient"
)

# Script generated for node rowLevelOutcomes - Addresses
rowLevelOutcomes_node_addresses = SelectFromCollection.apply(
    dfc=EvaluateDataQuality_node_addresses,
    key="rowLevelOutcomes",
    transformation_ctx="rowLevelOutcomes_node_addresses",
)

rowLevelOutcomes_node_addresses = rowLevelOutcomes_node_addresses.resolveChoice(
    specs=[
        ("DataQualityRulesPass", "cast:string"),
        ("DataQualityRulesFail", "cast:string"),
        ("DataQualityRulesSkip", "cast:string")
    ],
    transformation_ctx="ConvertArraysToStrings_RowLevel_addresses"
)

# Script generated for node rowLevelOutcomes - Medication_list
rowLevelOutcomes_node_medication_list = SelectFromCollection.apply(
    dfc=EvaluateDataQuality_node_medication_list,
    key="rowLevelOutcomes",
    transformation_ctx="rowLevelOutcomes_node_medication_list",
)

rowLevelOutcomes_node_medication_list = rowLevelOutcomes_node_medication_list.resolveChoice(
    specs=[
        ("DataQualityRulesPass", "cast:string"),
        ("DataQualityRulesFail", "cast:string"),
        ("DataQualityRulesSkip", "cast:string")
    ],
    transformation_ctx="ConvertArraysToStrings_RowLevel_medication_list"
)

# Script generated for node rowLevelOutcomes - Medication_cycles
rowLevelOutcomes_node_medication_cycles = SelectFromCollection.apply(
    dfc=EvaluateDataQuality_node_medication_cycles,
    key="rowLevelOutcomes",
    transformation_ctx="rowLevelOutcomes_node_medication_cycles",
)

rowLevelOutcomes_node_medication_cycles = rowLevelOutcomes_node_medication_cycles.resolveChoice(
    specs=[
        ("DataQualityRulesPass", "cast:string"),
        ("DataQualityRulesFail", "cast:string"),
        ("DataQualityRulesSkip", "cast:string")
    ],
    transformation_ctx="ConvertArraysToStrings_RowLevel_medication_cycles"
)

# Script generated for node rowLevelOutcomes - Practitioners
rowLevelOutcomes_node_practitioners = SelectFromCollection.apply(
    dfc=EvaluateDataQuality_node_practitioners,
    key="rowLevelOutcomes",
    transformation_ctx="rowLevelOutcomes_node_practitioners",
)

rowLevelOutcomes_node_practitioners = rowLevelOutcomes_node_practitioners.resolveChoice(
    specs=[
        ("DataQualityRulesPass", "cast:string"),
        ("DataQualityRulesFail", "cast:string"),
        ("DataQualityRulesSkip", "cast:string")
    ],
    transformation_ctx="ConvertArraysToStrings_RowLevel_practitioners"
)

# Script generated for node rowLevelOutcomes - Organizations
rowLevelOutcomes_node_organizations = SelectFromCollection.apply(
    dfc=EvaluateDataQuality_node_organizations,
    key="rowLevelOutcomes",
    transformation_ctx="rowLevelOutcomes_node_organizations",
)

rowLevelOutcomes_node_organizations = rowLevelOutcomes_node_organizations.resolveChoice(
    specs=[
        ("DataQualityRulesPass", "cast:string"),
        ("DataQualityRulesFail", "cast:string"),
        ("DataQualityRulesSkip", "cast:string")
    ],
    transformation_ctx="ConvertArraysToStrings_RowLevel_organizations"
)

# Script generated for node rowLevelOutcomes - Users
rowLevelOutcomes_node_users = SelectFromCollection.apply(
    dfc=EvaluateDataQuality_node_users,
    key="rowLevelOutcomes",
    transformation_ctx="rowLevelOutcomes_node_users",
)

rowLevelOutcomes_node_users = rowLevelOutcomes_node_users.resolveChoice(
    specs=[
        ("DataQualityRulesPass", "cast:string"),
        ("DataQualityRulesFail", "cast:string"),
        ("DataQualityRulesSkip", "cast:string")
    ],
    transformation_ctx="ConvertArraysToStrings_RowLevel_users"
)

# Script generated for node Amazon S3 - Patient DQR
MySQL_node_qr_patient = glueContext.write_dynamic_frame.from_catalog(
    frame=rowLevelOutcomes_node_patient,
    database="mysql_target",  # Target MySQL database
    table_name="engage_migration_dq_results_patients_enow",  # Target MySQL table for patient data
    transformation_ctx="MySQL_node_qr_patient",
)

# Script generated for node Amazon S3 - Addresses DQR
MySQL_node_qr_addresses = glueContext.write_dynamic_frame.from_catalog(
    frame=rowLevelOutcomes_node_addresses,
    database="mysql_target",  # Target MySQL database
    table_name="engage_migration_dqr_patient_addresses",  # Target MySQL table for addresses
    transformation_ctx="MySQL_node_qr_addresses",
)

# Script generated for node Amazon S3 - Medication_list DQR
MySQL_node_qr_medication_list = glueContext.write_dynamic_frame.from_catalog(
    frame=rowLevelOutcomes_node_medication_list,
    database="mysql_target",  # Target MySQL database
    table_name="engage_migration_dqr_patient_medication_lists",  # Target MySQL table for addresses
    transformation_ctx="MySQL_node_qr_medication_list",
)

# Script generated for node Amazon S3 - Medication_cycles DQR
MySQL_node_qr_medication_cycles = glueContext.write_dynamic_frame.from_catalog(
    frame=rowLevelOutcomes_node_medication_cycles,
    database="mysql_target",  # Target MySQL database
    table_name="engage_migration_dqr_patient_medication_cycles",  # Target MySQL table for addresses
    transformation_ctx="MySQL_node_qr_medication_cycles",
)

# Script generated for node Amazon S3 - Practitioners DQR
MySQL_node_qr_practitioners = glueContext.write_dynamic_frame.from_catalog(
    frame=rowLevelOutcomes_node_practitioners,
    database="mysql_target",  # Target MySQL database
    table_name="engage_migration_dqr_management_practitioners",  # Target MySQL table for addresses
    transformation_ctx="MySQL_node_qr_practitioners",
)

# Script generated for node Amazon S3 - Organizations DQR
MySQL_node_qr_organizations = glueContext.write_dynamic_frame.from_catalog(
    frame=rowLevelOutcomes_node_organizations,
    database="mysql_target",  # Target MySQL database
    table_name="engage_migration_dqr_management_organizations",  # Target MySQL table for addresses
    transformation_ctx="MySQL_node_qr_organizations",
)

# Script generated for node Amazon S3 - Users DQR
MySQL_node_qr_users = glueContext.write_dynamic_frame.from_catalog(
    frame=rowLevelOutcomes_node_users,
    database="mysql_target",  # Target MySQL database
    table_name="engage_migration_dqr_authorization_users",  # Target MySQL table for addresses
    transformation_ctx="MySQL_node_qr_users",
)

# Script generated for node MySQL - Patient
MySQL_node_patient = glueContext.write_dynamic_frame.from_catalog(
    frame=SQLQuery_node_patient,
    database="mysql_target",
    table_name="engage_migration_patients_engage_now",
    transformation_ctx="MySQL_node_patient",
)

# Script generated for node MySQL - Addresses
MySQL_node_addresses = glueContext.write_dynamic_frame.from_catalog(
    frame=SQLQuery_node_addresses,
    database="mysql_target",
    table_name="engage_migration_patient_addresses",
    transformation_ctx="MySQL_node_addresses",
)

# Script generated for node MySQL - Medication_list
MySQL_node_medication_list = glueContext.write_dynamic_frame.from_catalog(
    frame=SQLQuery_node_medication_list,
    database="mysql_target",
    table_name="engage_migration_patient_medication_lists",
    transformation_ctx="MySQL_node_list",
)

# Script generated for node MySQL - Medication_cycles
MySQL_node_medication_cycles = glueContext.write_dynamic_frame.from_catalog(
    frame=SQLQuery_node_medication_cycles,
    database="mysql_target",
    table_name="engage_migration_patient_medication_cycles",
    transformation_ctx="MySQL_node_medication_cycles",
)

# Script generated for node MySQL - Practitioners
MySQL_node_practitioners = glueContext.write_dynamic_frame.from_catalog(
    frame=SQLQuery_node_practitioners,
    database="mysql_target",
    table_name="engage_migration_management_practitioners",
    transformation_ctx="MySQL_node_practitioners",
)

# Script generated for node MySQL - Organizations
MySQL_node_organizations = glueContext.write_dynamic_frame.from_catalog(
    frame=SQLQuery_node_organizations,
    database="mysql_target",
    table_name="engage_migration_management_organizations",
    transformation_ctx="MySQL_node_organizations",
)

# Script generated for node MySQL - Users
MySQL_node_users = glueContext.write_dynamic_frame.from_catalog(
    frame=SQLQuery_node_users,
    database="mysql_target",
    table_name="engage_migration_authorization_users",
    transformation_ctx="MySQL_node_users",
)

job.commit()
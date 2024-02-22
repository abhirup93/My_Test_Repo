# SQL Spark Driver
driver_spark = "com.microsoft.sqlserver.jdbc.spark"
# SQL Server URL
url = "jdbc:sqlserver://<HOST_DETAILS>"
# Database Name
database_name = "<YOUR_SQl_SERVER_DB_NAME>"
# Located in App Registrations from Azure Portal
resource_app_id_url = "https://database.windows.net/"

# In the below example, we're using a Databricks utility that facilitates acquiring secrets from a configured Key Vault.

# Service Principal Client ID - Created in App Registrations from Azure Portal
service_principal_id = dbutils.secrets.get(
    scope="<AZURE_KEY_VAULT_NAME>", key="<AZURE_KEY_VAULT_SECRET_ID>"
)
# Service Principal Secret - Created in App Registrations from Azure Portal
service_principal_secret = dbutils.secrets.get(
    scope="<AZURE_KEY_VAULT_NAME>", key="<AZURE_KEY_VAULT_SECRET_KEY>"
)
# Located in App Registrations from Azure Portal
tenant_id = "<YOUR_TENANT_ID>"
# Authority
authority = "https://login.windows.net/" + tenant_id

context = adal.AuthenticationContext(authority)
token = context.acquire_token_with_client_credentials(
    resource_app_id_url, service_principal_id, service_principal_secret
)
access_token = token["accessToken"]

dbutils.widgets.text("sqltablename", "")
sqltablename = dbutils.widgets.get("sqltablename")
dbutils.widgets.text("adbtablename", "")
adbtablename = dbutils.widgets.get("adbtablename")

##Reading Control table usign dbQuery
try:
    jdbc_db = (
        spark.read.format("jdbc")
        .option("driver", driver)
        .option("url", url)
        .option("query", dbQuery)
        .option("accessToken", access_token)
        .option("encrypt", "true")
        .option("databaseName", database_name)
        .option("mssqlIsolationLevel", "READ_UNCOMMITTED")
        .load()
    )
except ValueError as error:
    print("Connector read failed", error)
	
df = spark.sql(f"SELECT * FROM {adbtablename}")

##Writing into SQL Server DB table using Spark JDBC driver
print(f"Starting process to push table {adbtablename}")

try:
    df.write.format(driver_spark).option("url", url).option(
        "dbtable", sqltable_name
    ).option("accessToken", access_token).option("encrypt", "true").option(
        "databaseName", database_name
    ).option(
        "truncate", "true"
    ).option(
        "isolationLevel", "SERIALIZABLE"
    ).option(
        "tableLock", "true"
    ).option(
        "CheckConstraints", "true"
    ).option(
        "FireTriggers", "false"
    ).option(
        "KeepIdentity", "true"
    ).mode(
        "overwrite"
    ).save()
    print(f"{adbtablename} successfully pushed to SQL MI Instance")
except ValueError as error:
    print("Connector write failed", error)
    print(f"{adbtablename} failed to push to SQL MI Instance")
    raise error
except Exception as error:
    print(f"Exception details- {error}")
    print(f"{adbtablename} failed to push to SQL MI Instance")
    raise error
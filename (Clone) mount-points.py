# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.mystoragedatabricks568.dfs.core.windows.net",
    dbutils.secrets.get(scope="secretecodes", key="mykeystorage"))


# COMMAND ----------



# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# Retrieve the secret
storage_account_key = dbutils.secrets.get(scope="secretecodes", key="key2")

# Set the configuration in Spark
spark.conf.set("fs.azure.account.key.mystoragedatabricks568.dfs.core.windows.net", storage_account_key)

# Load the data


# COMMAND ----------

df = spark.read.format('csv').option("header", "true").load("abfss://rawdata@mystoragedatabricks568.dfs.core.windows.net/data/employees.csv")
df.show()

# COMMAND ----------

secrete_id = dbutils.

# COMMAND ----------

client_id = dbutils.secrets.get(scope="secretecodes",key="clientId")
client_secret = dbutils.secrets.get(scope="secretecodes", key="clientsecrete")
tenant_id = dbutils.secrets.get(scope="secretecodes", key="tenantid")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": client_id,
       "fs.azure.account.oauth2.client.secret": client_secret,
       "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}

dbutils.fs.mount(
source = "abfss://rawdata@mystoragedatabricks568.dfs.core.windows.net/data",
mount_point = "/mnt/employeedata",
extra_configs = configs)

# COMMAND ----------

service_credential = dbutils.secrets.get(scope="<secret-scope>",key="<service-credential-key>")

spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.<storage-account>.dfs.core.windows.net", "<application-id>")
spark.conf.set("fs.azure.account.oauth2.client.secret.<storage-account>.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storage-account>.dfs.core.windows.net", "https://login.microsoftonline.com/<directory-id>/oauth2/token")


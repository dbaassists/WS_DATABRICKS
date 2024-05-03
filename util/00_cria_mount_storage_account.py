# Databricks notebook source
# MAGIC %md
# MAGIC Regras, Descrições (macro) e detalhes
# MAGIC
# MAGIC |N° Controle|Descrição|Dados
# MAGIC |---|---|---|
# MAGIC |01|Camada| Bronze - Silver - Gold |
# MAGIC |02|Data Criação| Quando o Fluxo foi Criado |
# MAGIC |03|Desenvolvida por| Quem desenvolver? |
# MAGIC |04|Objetivo| Por que foi criado? |
# MAGIC |05|Solicitante| Quem é o demandante? |
# MAGIC |06|Projeto| Atende algum projeto? |
# MAGIC |07|Nome do Pipeline| Esse notebool é usado em alguma Pipeline do Azure Data Factory? |
# MAGIC |08|Data Alteração| Foi alterado depois de criado? ||

# COMMAND ----------

# DBTITLE 1,1 - Criação de Mount no Databricks
# Substitua as informações abaixo com os detalhes da sua conta de armazenamento no Azure
blob_account_name = "asatreinamentows"
blob_container_name = "dados"
blob_account_key = "access_key"

# Montagem do armazenamento
mount_name = "dados"
mount_point = "/mnt/" + mount_name

# Configurações para a montagem
configs = {
    "fs.azure.account.key." + blob_account_name + ".blob.core.windows.net": blob_account_key
}

# Montar o armazenamento
dbutils.fs.mount(
    source="wasbs://" + blob_container_name + "@" + blob_account_name + ".blob.core.windows.net/",
    mount_point=mount_point,
    extra_configs=configs
)


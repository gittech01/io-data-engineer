
# main.py

from pyspark.context import SparkContext
from awsglue.context import GlueContext

from config.hudi_config import HudiTableConfig
from data_access.hudi_reader import HudiReader
from data_access.hudi_writer import (
    HudiWriter,
    HudiOperation
)


# Inicialização
sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session


# Configuração da tabela de origem
source_config = HudiTableConfig(
    database="default_database",
    table_name="tbl_processado_operacao_sor",
    record_key="id_operacao",
    precombine_field="dat_atualizacao",
    partition_field="ano_mes"
)


# Configuração da tabela de destino
target_config = HudiTableConfig(
    database="default_database",
    table_name="tbl_processado_operacao_consolidada",
    record_key="id_operacao",
    precombine_field="dat_atualizacao",
    partition_field="ano_mes"
)


# Instanciação
reader = HudiReader(glue_context)
writer = HudiWriter(glue_context)


# Leitura
df_source = reader.read(
    config=source_config,
    transformation_ctx="read_operacoes_sor"
)


# Transformação de exemplo
df_processed = df_source.filter(
    "id_operacao IS NOT NULL"
)


# Escrita incremental
writer.write(
    df=df_processed,
    config=target_config,
    operation=HudiOperation.UPSERT,
    transformation_ctx="write_operacoes_consolidadas"
)


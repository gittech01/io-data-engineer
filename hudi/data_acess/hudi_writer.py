
# data_access/hudi_writer.py

from enum import Enum

from awsglue.context import GlueContext
from pyspark.sql import DataFrame

from config.hudi_config import HudiTableConfig


class HudiOperation(str, Enum):
    INSERT = "insert"
    UPSERT = "upsert"
    BULK_INSERT = "bulk_insert"


class HudiWriter:

    def __init__(self, glue_context: GlueContext):
        self._glue_context = glue_context

    @staticmethod
    def _validate_dataframe(
        df: DataFrame,
        config: HudiTableConfig
    ) -> None:

        required_columns = {
            config.record_key,
            config.precombine_field
        }

        if config.partition_field:
            required_columns.add(config.partition_field)

        missing = required_columns - set(df.columns)

        if missing:
            raise ValueError(
                f"Colunas obrigatórias ausentes: {sorted(missing)}"
            )

        if df.take(1) == []:
            raise ValueError(
                "Não é possível gravar um DataFrame vazio."
            )

        null_keys = (
            df.filter(df[config.record_key].isNull())
              .limit(1)
              .count()
        )

        if null_keys:
            raise ValueError(
                f"A chave {config.record_key} contém valores nulos."
            )

    def write(
        self,
        df: DataFrame,
        config: HudiTableConfig,
        operation: HudiOperation = HudiOperation.UPSERT,
        transformation_ctx: str = ""
    ):

        config.validate()
        self._validate_dataframe(df, config)

        options = {
            "useSparkDataSink": True,

            "hoodie.table.name": config.table_name,
            "hoodie.database.name": config.database,

            "hoodie.datasource.write.table.name": config.table_name,
            "hoodie.datasource.write.storage.type": config.storage_type,

            "hoodie.datasource.write.operation": operation.value,

            "hoodie.datasource.write.recordkey.field": (
                config.record_key
            ),

            "hoodie.datasource.write.precombine.field": (
                config.precombine_field
            ),

            "hoodie.datasource.hive_sync.enable": "true",
            "hoodie.datasource.hive_sync.database": config.database,
            "hoodie.datasource.hive_sync.table": config.table_name,
            "hoodie.datasource.hive_sync.use_jdbc": "false",
            "hoodie.datasource.hive_sync.mode": "hms"
        }

        if config.partition_field:
            options.update({
                "hoodie.datasource.write.partitionpath.field": (
                    config.partition_field
                ),
                "hoodie.datasource.write.hive_style_partitioning": "true",
                "hoodie.datasource.hive_sync.partition_fields": (
                    config.partition_field
                )
            })

        return self._glue_context.write_data_frame.from_catalog(
            frame=df,
            database=config.database,
            table_name=config.table_name,
            transformation_ctx=transformation_ctx,
            additional_options=options
        )

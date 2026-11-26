
# data_access/hudi_reader.py

from awsglue.context import GlueContext

from config.hudi_config import HudiTableConfig


class HudiReader:

    def __init__(self, glue_context: GlueContext):
        self._glue_context = glue_context

    def read(
        self,
        config: HudiTableConfig,
        transformation_ctx: str = ""
    ):
        config.validate()

        return self._glue_context.create_data_frame.from_catalog(
            database=config.database,
            table_name=config.table_name,
            transformation_ctx=transformation_ctx
        )


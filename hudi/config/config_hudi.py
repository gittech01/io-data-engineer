
# config/hudi_config.py

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class HudiTableConfig:
    database: str
    table_name: str
    record_key: str
    precombine_field: str
    partition_field: Optional[str] = None
    storage_type: str = "COPY_ON_WRITE"

    @property
    def table_path_name(self) -> str:
        return f"{self.database}.{self.table_name}"

    def validate(self) -> None:
        required = {
            "database": self.database,
            "table_name": self.table_name,
            "record_key": self.record_key,
            "precombine_field": self.precombine_field,
        }

        missing = [
            name for name, value in required.items()
            if not value or not value.strip()
        ]

        if missing:
            raise ValueError(
                f"Configurações obrigatórias ausentes: {missing}"
            )

        if self.storage_type not in {"COPY_ON_WRITE"}:
            raise ValueError(
                f"Storage type não suportado: {self.storage_type}"
            )


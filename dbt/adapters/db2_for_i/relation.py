from dataclasses import dataclass, field

from dbt.adapters.base.relation import BaseRelation, Policy


@dataclass
class DB2ForIQuotePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = False


@dataclass
class DB2ForIIncludePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class DB2ForIRelation(BaseRelation):
    quote_policy: DB2ForIQuotePolicy = field(
        default_factory=lambda: DB2ForIQuotePolicy()
    )
    include_policy: DB2ForIIncludePolicy = field(
        default_factory=lambda: DB2ForIIncludePolicy()
    )

    @staticmethod
    def add_ephemeral_prefix(name: str):
        return f"DBT_CTE__{name}"

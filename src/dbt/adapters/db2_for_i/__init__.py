from dbt.adapters.db2_for_i.connections import DB2ForIConnectionManager
from dbt.adapters.db2_for_i.connections import DB2ForICredentials
from dbt.adapters.db2_for_i.impl import DB2ForIAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import db2_for_i


Plugin = AdapterPlugin(
    adapter=DB2ForIAdapter,
    credentials=DB2ForICredentials,
    include_path=db2_for_i.PACKAGE_PATH)
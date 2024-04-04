# dbt-db2fori

A [dbt](https://www.getdbt.com/) adapter for IBM's DB2 for i v7.2+. The connection to the warehouse is through `ODBC` and requires that `pyodbc` is installed. All credits to [dbt-sqlserver adapter](https://github.com/dbt-msft/dbt-sqlserver) and [dbt-ibmdb2](https://github.com/aurany/dbt-ibmdb2) projects that heavily inspired this adapter.

## Why this adapter?
A similar adapter [dbt-ibmdb2](https://github.com/aurany/dbt-ibmdb2) exists, however, [dbt-ibmdb2](https://github.com/aurany/dbt-ibmdb2) uses the `ibm_db` Python package to connect to IBM DB2. This adapter connects to the warehouse using `pyodbc`.

## Features
The following materializations are supported:

- Incremental
- Snapshot
- View
- Table
- Seed

Ephemeral models have not been tested yet. 


## Installation
Use pip to install:
```bash
pip install dbt-db2fori
```
An example `profiles.yml` is:
```bash
default:
    outputs:
        dev:
            type: db2_for_i
            threads: 4
            driver: IBM i Access ODBC Driver
            system: system
            username: "{{ env_var('USER_NAME) }}"
            password: "{{ env_var('PASSWORD) }}"
            database: db
            schema: schema

    target: dev
```

To report a bug or request a feature, open an [issue](https://github.com/kaysef/dbt-db2fori/issues/new)

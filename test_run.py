import sys
from pathlib import Path

import yaml
from jinja2 import Environment, FileSystemLoader
from dotenv import dotenv_values

from db_client import SnowflakeApiClient, SnowflakeApiClientConfig

config = dotenv_values(".env")

client = SnowflakeApiClient(
    SnowflakeApiClientConfig(
        account_url=config['SP_SF_ACCT_URL'],
        token=config['SP_SF_TOKEN'],
        warehouse=config['SP_SF_WAREHOUSE'],
        database="MI_XPRESSCLOUD",
        schema="XPRESSFEED",
        role=config['SP_SF_ROLE'],
    )
)

def run(query_name: str) -> None:
    query_dir = Path("sql_inventory") / query_name
    params = yaml.safe_load((query_dir / "params.yaml").read_text())
    if params is None:
        params = {}
    env = Environment(loader=FileSystemLoader(str(query_dir)))
    sql = env.get_template("query.sql.j2").render(**params)

    if not client.ping():
        raise RuntimeError("Snowflake connection check failed")

    res = client.fetch(sql)
    df = client.res_json_to_pandas(res)
    print(df)
    df.to_csv(f"{query_name}_output.csv", index=False)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: python test_run.py <query_name>")
        print(f"Available: {[p.name for p in Path('sql_inventory').iterdir() if p.is_dir()]}")
        sys.exit(1)
    run(sys.argv[1])

from jinja2 import Environment, FileSystemLoader
from dotenv import dotenv_values

from db_client import SnowflakeApiClient, SnowflakeApiClientConfig

config = dotenv_values(".env")

WAREHOUSE = config['SP_SF_WAREHOUSE']
DATABASE = "MI_XPRESSCLOUD"
SCHEMA = "XPRESSFEED"
ROLE = config['SP_SF_ROLE']
TOKEN = config['SP_SF_TOKEN']

client = SnowflakeApiClient(
    SnowflakeApiClientConfig(
        account_url=config['SP_SF_ACCT_URL'],
        token=TOKEN,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA,
        role=ROLE,
    )
)

env = Environment(loader=FileSystemLoader("sql_inventory"))
# sql = env.get_template("financial_data_items.sql.j2").render(
#     company_ids=[24937, 874652],
#     data_item_id=8,
#     date_from="2024-01-01",
#     date_to="2025-12-31",
# )
sql = env.get_template("market_price.sql.j2").render(
    company_ids=[24937, 874652],
    date_from="2024-01-01",
    date_to="2025-12-31",
)

if not client.ping():
    raise RuntimeError("Snowflake connection check failed")

res = client.fetch(sql)

df = client.res_json_to_pandas(res)
print(df)

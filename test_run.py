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

example_sql = """
WITH company_ids AS (
    SELECT value::NUMBER AS company_id
    FROM TABLE(
        FLATTEN(INPUT => PARSE_JSON('[19049, 23013, 24937, 191564, 309845, 324490, 874652]'))
    )
)
SELECT
    fp.COMPANYID                        AS company_id,
    fp.fiscalYear                       AS fiscal_year,
    fp.fiscalQuarter                    AS fiscal_quarter,
    fp.PERIODTYPEID                     AS data_period_type_id,
    fi.FILINGDATE                       AS reporting_date,
    fi.RESTATEMENTTYPEID                AS restatement_type_id,
    fi.periodEndDate                    AS fiscal_period_end,
    fc.CURRENCYID                       AS currency_id,
    fcd.DATAITEMVALUE                   AS data_item_value_raw,
    fcd.UNITTYPEID                      AS unit_type_id,
FROM CIQFINPERIOD fp
JOIN company_ids cid
ON fp.COMPANYID = cid.company_id
JOIN CIQFININSTANCE fi
ON fi.FINANCIALPERIODID = fp.FINANCIALPERIODID
JOIN CIQFININSTANCETOCOLLECTION fic
ON fic.FINANCIALINSTANCEID = fi.FINANCIALINSTANCEID
JOIN CIQFINCOLLECTION fc
ON fc.FINANCIALCOLLECTIONID = fic.FINANCIALCOLLECTIONID
JOIN CIQFINCOLLECTIONDATA fcd
ON fcd.FINANCIALCOLLECTIONID = fc.FINANCIALCOLLECTIONID
WHERE
    fcd.DATAITEMID = 8
    AND fi.PERIODENDDATE BETWEEN '2024-01-01'
                            AND '2025-12-31'
;
"""

if not client.ping():
    raise RuntimeError("Snowflake connection check failed")

res = client.fetch(example_sql)

df = client.res_json_to_pandas(res)
print(df)

from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()

def dim_branch():
    sql = """WITH branches AS (
                SELECT DISTINCT branch_name
                FROM sdp_vodafone_cash_catalog.silver.transactions_statefull
            )

            SELECT
                ROW_NUMBER() OVER (ORDER BY branch_name) AS branch_key,
                branch_name
            FROM branches;
    """
    return spark.sql(sql)


def dim_transaction_type():
    sql = """WITH transactions AS (
                SELECT DISTINCT transaction_type
                FROM sdp_vodafone_cash_catalog.silver.transactions_statefull
            )

            SELECT
                ROW_NUMBER() OVER (ORDER BY transaction_type) AS transaction_type_key,
                transaction_type
            FROM transactions
    """
    return spark.sql(sql)


def dim_date():
    sql = """WITH dates AS (
                SELECT DISTINCT date
                FROM sdp_vodafone_cash_catalog.silver.transactions_statefull
            )
            SELECT
                ROW_NUMBER() OVER (ORDER BY date) AS date_key,
                date AS full_date,
                DAY(date) AS day,
                MONTH(date) AS month,
                date_format(date, 'MMMM') AS month_name,
                QUARTER(date) AS quarter,
                YEAR(date) AS year,
                DAYOFWEEK(date) AS day_of_week,
                date_format(date, 'EEEE') AS day_of_week_name
            FROM dates
    """
    return spark.sql(sql)


def dim_wallet():
    sql = """WITH wallets AS (
                SELECT DISTINCT
                    myphone,
                    branch_name
                FROM sdp_vodafone_cash_catalog.silver.transactions_statefull
            )

            SELECT
                ROW_NUMBER() OVER (ORDER BY w.myphone) AS wallet_key,
                w.myphone AS wallet_number,
                SUBSTR(w.myphone, 1, 3) AS wallet_prefix,
                branch.branch_key
            FROM wallets w
            LEFT JOIN sdp_vodafone_cash_catalog.gold.dim_branch branch
                ON w.branch_name = branch.branch_name;
    """
    return spark.sql(sql)


def dim_customer():
    sql = """WITH customers AS (
                SELECT
                    customer_phone_number,
                    customer_name,
                    branch_name,
                    CASE
                        WHEN COUNT(*) >= 2880 THEN 'VIP'
                        ELSE 'Normal'
                    END AS customer_segment
                FROM sdp_vodafone_cash_catalog.silver.transactions_statefull
                GROUP BY
                    customer_phone_number,
                    customer_name,
                    branch_name
            )

            SELECT
                ROW_NUMBER() OVER (ORDER BY c.customer_name, c.customer_phone_number) AS customer_key,
                c.customer_name,
                c.customer_phone_number,
                c.customer_segment,
                branch.branch_key
            FROM customers c
            LEFT JOIN sdp_vodafone_cash_catalog.gold.dim_branch branch
                ON c.branch_name = branch.branch_name;
    """
    return spark.sql(sql)


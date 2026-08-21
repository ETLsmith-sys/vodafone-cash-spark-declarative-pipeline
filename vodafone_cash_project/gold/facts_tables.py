from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()

def fact_transactions():
   
    sql = """WITH fact_transactions AS (
                SELECT
                    f.transaction_id,
                    c.customer_key,
                    w.wallet_key,
                    d.date_key,
                    t.transaction_type_key,
                    f.amount,
                    f.total_balance_after_transaction,
                    f.commission
                FROM sdp_vodafone_cash_catalog.silver.transactions_statefull f
                LEFT JOIN sdp_vodafone_cash_catalog.gold.dim_customer c
                    ON f.customer_phone_number = c.customer_phone_number
                    AND f.customer_name = c.customer_name
                LEFT JOIN sdp_vodafone_cash_catalog.gold.dim_wallet w
                    ON f.myphone = w.wallet_number
                LEFT JOIN sdp_vodafone_cash_catalog.gold.dim_date d
                    ON f.date = d.full_date
                LEFT JOIN sdp_vodafone_cash_catalog.gold.dim_transaction_type t
                    ON f.transaction_type = t.transaction_type
            )

            SELECT *
            FROM fact_transactions;
    """
    return spark.sql(sql)


def fact_branch_daily_profit():
   
    sql = """WITH base AS (
                SELECT
                    branch_name,
                    date,
                    myphone,
                    transaction_id,
                    ranked,
                    total_before,
                    total_balance_after_transaction,
                    cash_balance,
                    transaction_type,
                    amount,
                    is_cashout_row,
                    commission
                FROM sdp_vodafone_cash_catalog.silver.transactions_statefull
            ),

            wallet_details AS (
                SELECT
                    branch_name,
                    date,
                    myphone,
                    FIRST_VALUE(total_before) OVER (
                        PARTITION BY branch_name, date, myphone
                        ORDER BY ranked
                    ) AS wallet_start,

                 LAST_VALUE(
                    CASE WHEN is_cashout_row THEN 0 ELSE total_balance_after_transaction END
                ) OVER (
                    PARTITION BY branch_name, date, myphone
                    ORDER BY ranked
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS wallet_end
                FROM base
            ),

            wallet_unique AS (
                SELECT DISTINCT
                    branch_name,
                    date,
                    myphone,
                    wallet_start,
                    wallet_end
                FROM wallet_details
            ),

            wallet_balance AS (
                SELECT
                    branch_name,
                    date,
                    SUM(wallet_start) AS wallet_sum_start,
                    SUM(wallet_end) AS wallet_sum_end
                FROM wallet_unique
                GROUP BY branch_name, date
            ),

            cash_details AS (
                SELECT
                    branch_name,
                    date,
                    FIRST_VALUE(cash_balance) OVER (
                        PARTITION BY branch_name, date
                        ORDER BY transaction_id
                    ) AS cash_start,

                    LAST_VALUE(cash_balance) OVER (
                        PARTITION BY branch_name, date
                        ORDER BY transaction_id
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    ) AS cash_end
                FROM base
            ),

            cash_balance AS (
                SELECT
                    branch_name,
                    date,
                    MAX(cash_start) AS cash_start,
                    MAX(cash_end) AS cash_end
                FROM cash_details
                GROUP BY branch_name, date
            ),

            external_amount AS (
                SELECT
                    branch_name,
                    date,
                    SUM(amount) AS external_sum
                FROM base
                WHERE transaction_type = 'external'
                GROUP BY branch_name, date
            ),

            profit AS (
                SELECT
                    branch_name,
                    date,
                    SUM(commission) AS profit
                FROM base
                GROUP BY branch_name, date
            )

            SELECT
                b.branch_key,
                d.date_key,

                wb.wallet_sum_start,
                cb.cash_start,

                wb.wallet_sum_end,
                cb.cash_end,

                COALESCE(ex.external_sum, 0) AS external_sum,
                p.profit,

                CASE
                    WHEN wb.wallet_sum_start + cb.cash_start
                         = wb.wallet_sum_end + cb.cash_end + COALESCE(ex.external_sum, 0)
                    THEN TRUE
                    ELSE FALSE
                END AS is_true_shift

            FROM wallet_balance wb

            JOIN cash_balance cb
                ON wb.branch_name = cb.branch_name
                AND wb.date = cb.date

            LEFT JOIN external_amount ex
                ON wb.branch_name = ex.branch_name
                AND wb.date = ex.date

            JOIN profit p
                ON wb.branch_name = p.branch_name
                AND wb.date = p.date

            LEFT JOIN sdp_vodafone_cash_catalog.gold.dim_branch b
                ON wb.branch_name = b.branch_name

            LEFT JOIN sdp_vodafone_cash_catalog.gold.dim_date d
                ON wb.date = d.full_date
    """
    return spark.sql(sql)



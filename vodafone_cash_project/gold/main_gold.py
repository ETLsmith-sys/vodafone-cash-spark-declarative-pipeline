
from pyspark import pipelines as dp


from gold_expectations import (
    GOLD_DIM_BRANCH_RULES,
    GOLD_DIM_TRANSACTION_TYPE_RULES,
    GOLD_DIM_DATE_RULES,
    GOLD_DIM_WALLET_RULES,
    GOLD_DIM_CUSTOMER_RULES,
    GOLD_FACT_TRANSACTIONS_RULES,
    GOLD_FACT_BRANCH_DAILY_PROFIT_RULES,
    GOLD_FACT_BRANCH_DAILY_PROFIT_CRITICAL_RULES)
    
from dimensions_tables import (
    dim_branch,
    dim_wallet,
    dim_customer,
    dim_date,
    dim_transaction_type,
)
from facts_tables import (
    fact_transactions,
    fact_branch_daily_profit,
)



@dp.materialized_view(name="gold.dim_branch")
@dp.expect_all_or_drop(GOLD_DIM_BRANCH_RULES)
def gold_dim_branch():
    return dim_branch()


@dp.materialized_view(name="gold.dim_wallet")
@dp.expect_all_or_drop(GOLD_DIM_WALLET_RULES)
def gold_dim_wallet():
    return dim_wallet()


@dp.materialized_view(name="gold.dim_customer")
@dp.expect_all_or_drop(GOLD_DIM_CUSTOMER_RULES)
def gold_dim_customer():
    return dim_customer()


@dp.materialized_view(name="gold.dim_date")
@dp.expect_all_or_drop(GOLD_DIM_DATE_RULES)
def gold_dim_date():
    return dim_date()


@dp.materialized_view(name="gold.dim_transaction_type")
@dp.expect_all_or_drop(GOLD_DIM_TRANSACTION_TYPE_RULES)
def gold_dim_transaction_type():
    return dim_transaction_type()




@dp.materialized_view(name="gold.fact_transactions")
@dp.expect_all_or_drop(GOLD_FACT_TRANSACTIONS_RULES)
def gold_fact_transactions():
    return fact_transactions()


@dp.materialized_view(name="gold.fact_branch_daily_profit")
@dp.expect_all_or_fail(GOLD_FACT_BRANCH_DAILY_PROFIT_RULES)
@dp.expect_all_or_fail(GOLD_FACT_BRANCH_DAILY_PROFIT_CRITICAL_RULES)
def gold_fact_branch_daily_profit():
    return fact_branch_daily_profit()



from pyspark.sql.types import DecimalType

DATA_SOURCE ="s3://vodafone-cash-transactions/outputs/*/*/*.csv"
MONEY_TYPE = DecimalType(18,2)
BRONZE_TABLE = "sdp_vodafone_cash_catalog.bronze.raw_transactions"
INITIAL_CASH_BALANCE = 1200000
STATELESS_RULE = {
    "valid_phone":
        "customer_phone_number RLIKE '^01[0125][0-9]{8}$'",

    "commission_positive":
        "commission >= 0",

    "balance_positive":
        "total_balance_after_transaction IS NULL OR total_balance_after_transaction >= 0",

    "valid_date":
        "date IS NOT NULL",

    "date_in_range":
        "date >= '2015-01-01' AND date <= '2026-12-31'" 

        
}

STATELESS_SILVER_TABLE = "sdp_vodafone_cash_catalog.silver.transactions_stateless"
STATEFULL_RULE = {

    "amount_not_null":
        "amount IS NOT NULL",
    "balance_not_null":
        "total_balance_after_transaction IS NOT NULL",
    "before_not_null":
        "total_before IS NOT NULL",

    "amount_positive":
        "amount >= 0",  
    "commission_non_negative":
        "commission >= 0",
    "receive_send_commission_positive":
        "transaction_type = 'external' OR commission > 0",
    "external_zero_commission":
        "transaction_type <> 'external' OR commission = 0",

    "receive_balance":
        "transaction_type <> 'receive' OR total_balance_after_transaction > total_before",
    "send_balance":
        "transaction_type NOT IN ('send','external') OR total_balance_after_transaction <= total_before",

    "balance_max_cap":
        "total_balance_after_transaction <= 200000",
    "balance_non_negative":
        "total_balance_after_transaction >= 0",
    "before_non_negative":
        "total_before >= 0",
    "transaction_type_valid":
        "transaction_type IN ('receive', 'send', 'external')",
}
STATEFULL_SILVER_TABLE = "sdp_vodafone_cash_catalog.silver.transactions_statefull"


GOLD_FACT_TRANSACTIONS_RULES = {
    "valid_transaction_id":
        "transaction_id IS NOT NULL",
    "valid_customer_key":
        "customer_key IS NOT NULL",
    "valid_wallet_key":
        "wallet_key IS NOT NULL",
    "valid_date_key":
        "date_key IS NOT NULL",
    "valid_transaction_type_key":
        "transaction_type_key IS NOT NULL",

    "amount_not_null":
        "amount IS NOT NULL",
    "amount_non_negative":
        "amount >= 0",
    "balance_not_null":
        "total_balance_after_transaction IS NOT NULL",
    "balance_range":
        "total_balance_after_transaction BETWEEN 0 AND 200000",
    "commission_non_negative":
        "commission >= 0",
}

GOLD_FACT_BRANCH_DAILY_PROFIT_RULES = {
    "valid_branch_key":
        "branch_key IS NOT NULL",
    "valid_date_key":
        "date_key IS NOT NULL",

    "wallet_sum_start_not_null":
        "wallet_sum_start IS NOT NULL",
    "cash_start_not_null":
        "cash_start IS NOT NULL",
    "wallet_sum_end_not_null":
        "wallet_sum_end IS NOT NULL",
    "cash_end_not_null":
        "cash_end IS NOT NULL",

    "wallet_sum_end_non_negative":
        "wallet_sum_end >= 0",
    "cash_end_non_negative":
        "cash_end >= 0",          
    "external_sum_non_negative":
        "external_sum >= 0",
    "profit_non_negative":
        "profit >= 0",

   
    "shift_always_balanced":
        "is_true_shift = TRUE",
}


STATEFULL_SILVER_TABLE = "sdp_vodafone_cash_catalog.silver.transactions_statefull"





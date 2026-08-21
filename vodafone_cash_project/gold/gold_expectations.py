

GOLD_DIM_BRANCH_RULES = {
    "valid_branch_key":
        "branch_key IS NOT NULL",
    
    "valid_branch_name":
        "branch_name IS NOT NULL",
}

GOLD_DIM_TRANSACTION_TYPE_RULES = {
    "valid_transaction_type_key":
        "transaction_type_key IS NOT NULL",
  
    "valid_transaction_type_domain":
        "transaction_type IN ('receive', 'send', 'external')",
}

GOLD_DIM_DATE_RULES = {
    "valid_date_key":
        "date_key IS NOT NULL",

    "valid_full_date":
        "full_date IS NOT NULL",
    "valid_year_range":
        "year BETWEEN 2015 AND 2026",
}

GOLD_DIM_WALLET_RULES = {
    "valid_wallet_key":
        "wallet_key IS NOT NULL",
    "valid_wallet_number":
        "wallet_number IS NOT NULL",
    "valid_wallet_prefix":
        "wallet_prefix IN ('010', '012')",
    "valid_branch_key":
        "branch_key IS NOT NULL",
}

GOLD_DIM_CUSTOMER_RULES = {
    "valid_customer_key":
        "customer_key IS NOT NULL",
    "valid_customer_phone_number":
        "customer_phone_number IS NOT NULL",
    "valid_customer_segment":
        "customer_segment IN ('VIP', 'Normal')",
    "valid_branch_key":
        "branch_key IS NOT NULL",
}

# --------------------------- Facts ---------------------------

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
}
GOLD_FACT_BRANCH_DAILY_PROFIT_CRITICAL_RULES = {
    "shift_always_balanced":
        "is_true_shift = TRUE"
}
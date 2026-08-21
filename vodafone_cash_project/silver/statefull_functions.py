from pyspark.sql import  Window
from pyspark.sql.types import *
from pyspark.sql import functions as F
from vodafone_cash_project.common.config import MONEY_TYPE


def add_ranked(df):
    window = Window.partitionBy("myphone").orderBy("transaction_id") 
    df = df.withColumn("ranked",F.row_number().over(window))
    return df

def add_total_before(df):

    window = Window.partitionBy("myphone").orderBy("ranked")
    prev_after = F.lag("total_balance_after_transaction", 1).over(window)
    prev_type = F.lag("transaction_type", 1).over(window)

    prev_triggered_cashout = (
        (prev_type == "receive") &
        (prev_after >= F.lit(190000).cast(MONEY_TYPE))
    )
    wallet_send = (
    (F.col("ranked") == 1) &
    (F.col("transaction_type").isin("send", "external")))
    
    df = df.withColumn(
        "total_before",
        F.when(wallet_send,F.lit(100000).cast(MONEY_TYPE))
        .when(F.col("ranked") == 1, F.lit(0).cast(MONEY_TYPE))
        .when(prev_triggered_cashout, F.lit(0).cast(MONEY_TYPE))
        .otherwise(prev_after)
    )
    return df

def get_true_total_balances(df):
    missing_balance = F.col("total_balance_after_transaction").isNull()
    amount = F.col("amount")
    before = F.col("total_before")

    receive_value = (before + amount).cast(MONEY_TYPE)
    balance_after_outgoing = (before - amount).cast(MONEY_TYPE)

    df = df.withColumn(
        "total_balance_after_transaction",
        F.when(missing_balance & (F.col("transaction_type") == "receive"), receive_value)
        .when(missing_balance & (F.col("transaction_type").isin("send", "external")), balance_after_outgoing)
        .otherwise(F.col("total_balance_after_transaction")),
    )
    return df

def clean_total_balance(df):
    df = add_ranked(df)
    df = add_total_before(df)
    df = get_true_total_balances(df)
    df = add_total_before(df)
    return df

def clean_amount(df):
    before = F.col("total_before")
    after = F.col("total_balance_after_transaction")
    receive_amount = (after - before).cast(MONEY_TYPE)
    outgoing_amount = (before - after).cast(MONEY_TYPE)

    df = df.withColumn(
        "amount",
        F.when(
            F.col("amount").isNull(),
            F.when(F.col("transaction_type") == "receive", receive_amount)
            .otherwise(outgoing_amount),
        ).otherwise(F.col("amount")),
    )
    return df

def get_true_commission(df):
    df = df.withColumn("commission",
        F.when(
            (F.col("transaction_type") == "receive") &
            (F.col("amount").isNotNull()),
            F.when(
            (F.col("amount") >= 50) & (F.col("amount") <= 500),
            F.lit(5).cast(MONEY_TYPE))
            .otherwise(
                (F.ceil((F.col("amount") * 0.01) / 5.0) * 5).cast(MONEY_TYPE)))
        
        .when((F.col("transaction_type") == "send") & 
              (F.col("amount").isNotNull()),
            F.when(
                (F.col("amount") >= 50) & 
                (F.col("amount") <= 1000),
                F.lit(5).cast(MONEY_TYPE))
            .otherwise(
                (F.ceil(F.col("amount") / 1000.0) * 5).cast(MONEY_TYPE)
            ))
        
        .otherwise(F.lit(0).cast(MONEY_TYPE)))
    return df


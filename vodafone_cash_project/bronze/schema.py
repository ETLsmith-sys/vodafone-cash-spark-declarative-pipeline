
from pyspark.sql.types import *

from vodafone_cash_project.common.config import MONEY_TYPE 


bronze_schema = StructType([
    StructField("transaction_id", IntegerType(), False),
    StructField("transaction_type", StringType(), True),
    StructField("amount", MONEY_TYPE, True),
    StructField("customer_phone_number", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("total_balance_after_transaction", MONEY_TYPE, True),
    StructField("timestamp", StringType(), True),
    StructField("commission", MONEY_TYPE, True),
    StructField("myphone", StringType(), True),
    StructField("branch_name", StringType(), True),
    StructField("cash_balance", MONEY_TYPE, True),
    StructField("is_cashout_row", BooleanType(), False)
])
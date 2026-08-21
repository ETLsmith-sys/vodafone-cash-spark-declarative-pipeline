from pyspark.sql import  Window
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark import pipelines as dp
from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()

from vodafone_cash_project.common.config import(
 STATELESS_RULE,BRONZE_TABLE,
 STATEFULL_RULE,
 STATELESS_SILVER_TABLE,
 INITIAL_CASH_BALANCE
)
from stateless_functions import(
handel_nagative_values,
handel_phone_number_format,
get_true_date)
from statefull_functions import ( 
clean_total_balance,
clean_amount,
get_true_commission)
from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()

@dp.materialized_view(name="silver.transactions_stateless")
@dp.expect_all_or_fail(STATELESS_RULE)
def transactions_stateless():
    df = spark.read.table(BRONZE_TABLE)
    df = handel_nagative_values(df)
    df = handel_phone_number_format(df)
    df = get_true_date(df)
    
    return df 

@dp.materialized_view(name="silver.transactions_statefull")
@dp.expect_all_or_fail(STATEFULL_RULE)
def transactions_statefull():
    df = spark.read.table(STATELESS_SILVER_TABLE)
    df = clean_total_balance(df)
    df = clean_amount(df)
    df = get_true_commission(df)
    
    return df





    


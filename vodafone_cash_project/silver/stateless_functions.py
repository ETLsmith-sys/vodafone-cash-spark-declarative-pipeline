
from pyspark.sql.types import *
from pyspark.sql import functions as F

def get_true_date(df):
    df = df.withColumn(
        "timestamp",
        F.to_date(
            F.coalesce(
                F.expr("try_to_timestamp(timestamp, 'yyyy-MM-dd HH:mm:ss')"),
                F.expr("try_to_timestamp(timestamp, 'dd/MM/yyyy HH:mm:ss')"))))
    
    df = df.withColumnRenamed("timestamp", "date")
    
    return df

def handel_nagative_values(df):
    
    df = df.withColumn('commission',
            F.abs(F.col("commission")))
                
    df = df.withColumn('total_balance_after_transaction',         
            F.abs(F.col('total_balance_after_transaction')))
    
    return df 
            
def handel_phone_number_format(df):
    
    df = df.withColumn("customer_phone_number",
                        
        F.when(F.col("customer_phone_number").startswith("+20"),  
        F.concat(F.lit("0"), F.col("customer_phone_number").substr(4, 10)))
        
        .when((F.col("customer_phone_number").startswith("20")) & 
        (F.length(F.col("customer_phone_number")) == 12),
        F.concat(F.lit("0"), F.col("customer_phone_number").substr(3, 10)))
        
        .when(F.length(F.col("customer_phone_number")) == 10,    
        F.lpad(F.col("customer_phone_number"), 11, "0")) 
        
        .otherwise(F.col("customer_phone_number")) )

    return df 



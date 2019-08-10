from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import Row
import re

conf = SparkConf().setAppName("Table24_hacktron").setMaster("local")
sc = SparkContext(conf=conf)


def datacleansing(record,num_of_columns):
    regex = re.compile('[_!#%^&*()<>?}{~]')
    if len(record.split('|'))==num_of_columns:
        if (regex.search(record) == None):
            value= "True"
        else:
            value= "False"
    else :
        value="False"
    return value


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value").config("spark.sql.warehouse.dir", "file:///C:/Manasa/")  \
        .getOrCreate()
    product_schema = ['product_id', 'product_name', 'product_type', 'product_version', 'product_price']
    product = sc.textFile("file:///C:\Manasa\Datasets\Product.txt")
    products_filtered=product.map(lambda x : (x,datacleansing(x,len(product_schema)))).filter(lambda x: x[1]=="True")\
    .map(lambda x:x[0].split('|'))
    products_df=spark.createDataFrame(products_filtered, schema=product_schema)
    products_df_cleaned=products_df.dropna(how='any').dropDuplicates()
    products_df_cleaned.coalesce(1).write.option("header", "false").csv("file:///C:\Manasa\Datasets\product")

    sales_schema = ['transaction_id', 'customer_id', 'product_id', 'timestamp' ,'total_amount', 'total_quantity']
    sales = sc.textFile("file:///C:\Manasa\Datasets\Sales.txt")
    sales_filtered=sales.map(lambda x : (x,datacleansing(x,len(sales_schema)))).filter(lambda x: x[1]=="True")\
    .map(lambda x:x[0].split('|'))
    sales_df=spark.createDataFrame(sales_filtered, schema=sales_schema)
    sales_df_cleaned=sales_df.dropna(how='any').dropDuplicates()
    sales_df_cleaned.coalesce(1).write.option("header", "false").csv("file:///C:\Manasa\Datasets\sales")

    customer_schema = ['customer_id', 'customer_first_name', 'customer_last_name', 'phone_number']
    customer = sc.textFile("file:///C:\Manasa\Datasets\customer.txt")
    customer_filtered=customer.map(lambda x : (x,datacleansing(x,len(customer_schema)))).filter(lambda x: x[1]=="True")\
    .map(lambda x:x[0].split('|'))
    customer_df=spark.createDataFrame(customer_filtered, schema=customer_schema)
    customer_df_cleaned=customer_df.dropna(how='any').dropDuplicates()
    customer_df_cleaned.coalesce(1).write.option("header", "false").csv("file:///C:\Manasa\Datasets\Customers")

    refund_schema = ['refund_id', 'original_transaction_id', 'customer_id', 'product_id','timestamp','refund_amount','refund_quantity']
    refund = sc.textFile("file:///C:\Manasa\Datasets\Refund.txt")
    refund_filtered=refund.map(lambda x : (x,datacleansing(x,len(refund_schema)))).filter(lambda x: x[1]=="True")\
    .map(lambda x:x[0].split('|'))
    refund_df=spark.createDataFrame(refund_filtered, schema=refund_schema)
    refund_df_cleaned=refund_df.dropna(how='any').dropDuplicates()
    refund_df_cleaned.coalesce(1).write.option("header", "false").csv("file:///C:\Manasa\Datasets//refund")


    spark.stop()

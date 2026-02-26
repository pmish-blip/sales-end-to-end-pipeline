from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, row_number, current_timestamp
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

customers_first = spark.table("customer_first")
customers_second = spark.table("customers_second")
orders_first = spark.table("orders_first")
orders_second = spark.table("orders_second")
products_first = spark.table("products_first")
products_second = spark.table("products_second")
regions = spark.table("regions")

customers_union = customers_first.unionByName(customers_second)
orders_union = orders_first.unionByName(orders_second)
products_union = products_first.unionByName(products_second)

customers_silver = customers_union \
    .withColumn("customer_id", col("customer_id").cast("string")) \
    .withColumn("email", lower(trim(col("email")))) \
    .withColumn("updated_at", col("updated_at").cast("timestamp")) \
    .filter(col("customer_id").isNotNull())

orders_silver = orders_union \
    .withColumn("order_id", col("order_id").cast("string")) \
    .withColumn("customer_id", col("customer_id").cast("string")) \
    .withColumn("product_id", col("product_id").cast("string")) \
    .withColumn("amount", col("amount").cast("double")) \
    .withColumn("order_date", col("order_date").cast("timestamp")) \
    .filter(col("order_id").isNotNull())

products_silver = products_union \
    .withColumn("product_id", col("product_id").cast("string")) \
    .withColumn("price", col("price").cast("double")) \
    .filter(col("product_id").isNotNull())

customers_window = Window.partitionBy("customer_id").orderBy(col("updated_at").desc())
orders_window = Window.partitionBy("order_id").orderBy(col("order_date").desc())
products_window = Window.partitionBy("product_id").orderBy(col("updated_at").desc())

customers_gold = customers_silver \
    .withColumn("rn", row_number().over(customers_window)) \
    .filter(col("rn") == 1) \
    .drop("rn") \
    .join(regions, "region_id", "left") \
    .withColumn("processed_at", current_timestamp())

orders_gold = orders_silver \
    .withColumn("rn", row_number().over(orders_window)) \
    .filter(col("rn") == 1) \
    .drop("rn") \
    .withColumn("processed_at", current_timestamp())

products_gold = products_silver \
    .withColumn("rn", row_number().over(products_window)) \
    .filter(col("rn") == 1) \
    .drop("rn") \
    .withColumn("processed_at", current_timestamp())

customers_gold.write.mode("overwrite").format("delta").saveAsTable("gold.customers")
orders_gold.write.mode("overwrite").format("delta").saveAsTable("gold.orders")
products_gold.write.mode("overwrite").format("delta").saveAsTable("gold.products")

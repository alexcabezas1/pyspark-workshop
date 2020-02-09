ROOT_PATH = 's3://datasets-spark-workshops/'
DATASET_PATH = {
    "sellers": "sellers.csv",
    "products": "products.csv",
    "product_prices": "product_prices.csv",
    "categories": "categories.csv",
    "customers": "customers.csv",
    "orders": "orders.csv",
    "orders_details": "order_details.csv",
    "product_reviews": "product_reviews.csv"
}

def get_dataset_path(name):
    return "{}{}".format(ROOT_PATH, DATASET_PATH[name])

sellers_csv_rdd = sc.textFile(get_dataset_path('sellers'))
products_csv_rdd = sc.textFile(get_dataset_path('products'))
product_prices_csv_rdd = sc.textFile(get_dataset_path('product_prices'))
categories_csv_rdd = sc.textFile(get_dataset_path('categories'))
customers_csv_rdd = sc.textFile(get_dataset_path('customers'))
orders_csv_rdd = sc.textFile(get_dataset_path('orders'))
orders_details_csv_rdd = sc.textFile(get_dataset_path('orders_details'))
product_reviews_csv_rdd = sc.textFile(get_dataset_path('product_reviews'))

import datetime
def convert_datetime(dt):
    return datetime.datetime.strptime(dt, '%Y-%m-%dT%H:%M:%SZ')
    
sellers_rdd = sellers_csv_rdd.zipWithIndex().filter(lambda x: x[1] > 0).map(lambda e: e[0].split(","))

# seller_id,category_id,product_id,product_name
products_rdd = products_csv_rdd.zipWithIndex().filter(lambda x: x[1] > 0).map(lambda e: e[0].split(","))

# product_price_id,price,seller_id,register_on,product_id
product_prices_rdd = (
    product_prices_csv_rdd
    .zipWithIndex()
    .filter(lambda x: x[1] > 0)
    .map(lambda e: e[0].split(","))
    .map(lambda e: (e[0], e[1], e[2], convert_datetime(e[3]), e[4]))
)

products_rdd.map(lambda e: (e[1], 1)).countByKey().items()

products_rdd.map(lambda e: (e[0], 1)).countByKey().items()

products_keyed_rdd = products_rdd.map(lambda e: (e[2], e))
(product_prices_rdd
    .map(lambda e: (e[4], e))
    .reduceByKey(lambda e1, e2: e1 if e1[3] > e2[3] else e2)
    .join(products_keyed_rdd)
    .map(lambda e: (e[0], e[1][1][3], e[1][0][1])
).collect()

Dataframe with Python only

import pyspark.sql.functions as F
from pyspark.sql import Window

products_df = spark.read.load(get_dataset_path('products'), format="csv", inferSchema="true", header="true")
products_df.printSchema()

categories_df = spark.read.load(get_dataset_path('categories'), format="csv", inferSchema="true", header="true")
categories_df.printSchema()

product_prices_df = spark.read.load(get_dataset_path('product_pricesproduct_prices'), format="csv", inferSchema="true", header="true")
product_prices_df.printSchema()

sellers_df = spark.read.load(get_dataset_path('sellers'), format="csv", inferSchema="true", header="true")
sellers_df.printSchema()

customers_df = spark.read.load(get_dataset_path('customers'), format="csv", inferSchema="true", header="true")
customers_df.printSchema()

orders_df = spark.read.load(get_dataset_path('orders'), format="csv", inferSchema="true", header="true")
orders_df.printSchema()

orders_details_df = spark.read.load(get_dataset_path('orders_details'), format="csv", inferSchema="true", header="true")
orders_details_df.printSchema()

product_reviews_df = spark.read.load(get_dataset_path('product_reviews'), format="csv", inferSchema="true", header="true")
product_reviews_df.printSchema()


products_x_category_df = products_df.groupBy("category_id").agg(F.count("product_id").alias("products_quantity"))
products_x_category_df.join(categories_df, products_df.category_id == categories_df.category_id, "inner").select('name', products_x_category_df['products_quantity']).show()

z.show(
products_df
    .groupBy("seller_id")
    .agg(F.count("product_id")
    .alias("products_quantity"))
)

over_product_id = Window.partitionBy(product_prices_df["product_id"]).orderBy(product_prices_df["register_on"].desc())
(product_prices_df
    .withColumn("row_number", F.row_number().over(over_product_id))
    .select("product_id", "price")
    .where("row_number = 1")
).show()

over_product_id = Window.partitionBy(product_prices_df["product_id"], product_prices_df["seller_id"]).orderBy(product_prices_df["register_on"].desc())
(product_prices_df
    .withColumn("row_number", F.row_number().over(over_product_id))
    .where("row_number = 1")
    .join(sellers_df, product_prices_df.seller_id == sellers_df.seller_id, "inner")
    .join(products_df, product_prices_df.product_id == products_df.product_id, "inner")
    .select(product_prices_df["product_id"], products_df["product_name"], product_prices_df["price"], product_prices_df["seller_id"], sellers_df["name"].alias("seller"))
    .orderBy(product_prices_df["seller_id"], product_prices_df["price"])
).show(100)

Dataframes with SQL

products_df.createOrReplaceTempView("products")
product_prices_df.createOrReplaceTempView("product_prices")
sellers_df.createOrReplaceTempView("sellers")


spark.sql("""
SELECT 
    sl.name, count(0) as product_quantity 
FROM products pr 
INNER JOIN sellers sl ON pr.seller_id = sl.seller_id 
GROUP BY sl.name
""").show()


sql = """
SELECT 
    product_id,
    price
FROM (
    SELECT 
        product_id,
        price,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY register_on DESC) as row_number
    FROM product_prices
)
WHERE row_number = 1
ORDER BY product_id
"""

spark.sql(sql).show()
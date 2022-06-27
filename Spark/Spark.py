from pyspark.sql import SparkSession
from sqlalchemy import create_engine
import pandas as pd
from pyspark.sql.types import *
import pyspark.sql.functions as func

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('PySpark_SQL') \
    .getOrCreate()

engine = create_engine(
    "postgresql+psycopg2://postgres:123@localhost/pagila?client_encoding=utf8")

film_schema = StructType(
    [
        StructField('film_id', IntegerType(), True),
        StructField('title', StringType(), True),
        StructField('description', StringType(), True),
        StructField('release_year', IntegerType(), True),
        StructField('language_id', IntegerType(), True),
        StructField('original_language_id', IntegerType(), True),
        StructField('rental_duration', ShortType(), True),
        StructField('rental_rate', DoubleType(), True),
        StructField('length', ShortType(), True),
        StructField('replacement_cost', DoubleType(), True),
        StructField('rating', StringType(), True),
        StructField('last_update', TimestampType(), True),
        StructField('special_features', ArrayType(StringType()), True),
        StructField('fulltext', StringType(), True)
    ]
)

staff_schema = StructType(
    [
        StructField('staff_id', IntegerType(), True),
        StructField('first_name', StringType(), True),
        StructField('last_name', StringType(), True),
        StructField('address_id', IntegerType(), True),
        StructField('email', StringType(), True),
        StructField('store_id', IntegerType(), True),
        StructField('active', BooleanType(), True),
        StructField('username', StringType(), True),
        StructField('password', StringType(), True),
        StructField('last_update', TimestampType(), True),
        StructField('picture', StringType(), True),

    ]
)

actor = spark.createDataFrame(pd.read_sql('select * from actor', engine))
address = spark.createDataFrame(pd.read_sql('select * from address', engine))
category = spark.createDataFrame(pd.read_sql('select * from category', engine))
city = spark.createDataFrame(pd.read_sql('select * from city', engine))
country = spark.createDataFrame(pd.read_sql('select * from country', engine))
customer = spark.createDataFrame(pd.read_sql('select * from customer', engine))
film = spark.createDataFrame(pd.read_sql('select * from film', engine), film_schema)
film_actor = spark.createDataFrame(pd.read_sql('select * from film_actor', engine))
film_category = spark.createDataFrame(pd.read_sql('select * from film_category', engine))
inventory = spark.createDataFrame(pd.read_sql('select * from inventory', engine))
language = spark.createDataFrame(pd.read_sql('select * from language', engine))
payment = spark.createDataFrame(pd.read_sql('select * from payment', engine))
rental = spark.createDataFrame(pd.read_sql('select * from rental', engine))
staff = spark.createDataFrame(pd.read_sql('select * from staff', engine), staff_schema)
store = spark.createDataFrame(pd.read_sql('select * from store', engine))

print("\n///////////////////////////////////////////////////////////////\n")

# -----------------------------------------------------------------------------------------------------------------#1
category.join(film_category, "category_id", "left") \
    .groupBy("name").agg(func.count("film_id").alias("cnt_films")).orderBy(
    func.col("cnt_films").desc()).show()

# -----------------------------------------------------------------------------------------------------------------#2
actor.join(film_actor, "actor_id", "left") \
    .groupBy("first_name").agg(func.count("film_id").alias("cnt_films")).orderBy(
    func.col("cnt_films").desc()).limit(10).show()

# -----------------------------------------------------------------------------------------------------------------#3
category.join(film_category, "category_id", "left").\
    join(film, "film_id", "left").\
    join(inventory, "film_id", "left").\
    join(rental, "inventory_id", "left").\
    join(payment, "rental_id", "left").\
    groupBy("name").agg(func.sum("amount").alias("sum")).orderBy(func.col("sum").desc()).limit(1).show()

# -----------------------------------------------------------------------------------------------------------------#4
film.join(inventory, "film_id", "leftanti").select("title").show()

# -----------------------------------------------------------------------------------------------------------------#5
actor_name = actor.join(film_actor, "actor_id", "inner")
child_cat_id = list(category.select(func.col("category_id")).filter(category.name == "Children").collect()[0])
film_id = [row.film_id for row in film_category[film_category.category_id.isin(child_cat_id)].collect()]
actors_in_child_cat = actor_name.filter(actor_name.film_id.isin(film_id)).\
     groupBy("first_name").agg(func.count("film_id").alias("cnt")).orderBy(func.col("cnt").desc())
uniq_values = [row.cnt for row in list(actors_in_child_cat.select("cnt").distinct().
                                       orderBy(func.col("cnt").desc()).limit(3).collect())]
actors_in_child_cat.filter(actors_in_child_cat.cnt.isin(uniq_values)).show()

# -----------------------------------------------------------------------------------------------------------------#6
records_with_active = city.join(address, "city_id", "left")\
    .join(customer, "address_id", "left").where(func.col("active") == 1)\
    .groupBy("city").agg(func.sum("customer_id").alias("active"))

records_with_notactive = city.join(address, "city_id", "left")\
    .join(customer, "address_id", "left").where(func.col("active") == 0)\
    .groupBy("city").agg(func.sum("customer_id").alias("not_active"))

records_with_active.join(records_with_notactive, "city", "full")\
    .select("city", "active", "not_active").\
    orderBy(func.col("not_active").desc()).show(1000)

# -----------------------------------------------------------------------------------------------------------------#7
films_and_rental_duration = category.join(film_category, "category_id", "left")\
    .join(film, "film_id", "left")\
    .join(inventory, "film_id", "left")\
    .join(rental, "inventory_id", "left")\
    .join(customer, "customer_id", "left")\
    .join(address, "address_id", "left")\
    .join(city, "city_id", "left")\
    .groupBy("name").agg(func.sum("rental_duration").alias("rental_duration"))\
    .orderBy(func.col("rental_duration").desc())
like_hyphen = films_and_rental_duration.select("name", "rental_duration").where(func.col("name").like("%-%")).limit(1)
like_A = films_and_rental_duration.select("name", "rental_duration").where(func.col("name").like("A%")).limit(1)
like_A.union(like_hyphen).show()

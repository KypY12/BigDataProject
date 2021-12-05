import itertools

from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import ArrayType, StructType, StructField, StringType

if __name__ == '__main__':
    session = SparkSession \
        .builder \
        .appName("Testing") \
        .getOrCreate()

    sample_size = 100

    metadata_df = session.read.json("arxiv-metadata-oai-snapshot.json").limit(sample_size)

    metadata_df.printSchema()
    metadata_df.show()

    # print(metadata_df.rdd.getNumPartitions())

    udf_res_schema = ArrayType(StructType([
        StructField("author_1", StringType(), False),
        StructField("author_2", StringType(), False)
    ]))
    comb_udf = f.udf(lambda x: list(itertools.combinations([" ".join(y) for y in x], 2)),
                     udf_res_schema)

    authors_df = metadata_df \
        .withColumn("authors_processed", f.explode(comb_udf(f.col("authors_parsed")))) \
        .select(f.col("authors_processed")["author_1"].alias("author_1"),
                f.col("authors_processed")["author_2"].alias("author_2"),
                f.col("id").alias("article_id"),
                f.split(f.col("categories"), " ").alias("article_categories"),
                f.col("title"),
                f.col("update_date"))
    authors_df.show()

    # authors_df.write \
    #     .option("header", True) \
    #     .mode("overwrite") \
    #     .json("arxiv-processed")

# IF WE USE A BIPARTITE GRAPH
# authors_df = metadata_df.select(f.explode(f.col("authors_parsed")), *[f.col(c) for c in metadata_df.columns if
#                                                                       c not in ["authors", "authors_parsed", ""]]) \
#     .select(f.concat_ws(" ", f.col("col")),
#             *[f.col(c) for c in metadata_df.columns if c not in ["authors", "authors_parsed", ""]])
# authors_df = metadata_df.select(f.explode(f.col("authors_parsed")), f.col("id")) \
#     .select(f.concat_ws(" ", f.col("col")), f.col("id")) \
#     .toDF("author", "article_id")

# USING SELF JOIN (with duplicate rows ...)
# authors_df = metadata_df.select(f.explode(f.col("authors_parsed")).alias("authors"),
#                                 f.col("id").alias("article_id"),
#                                 f.split(f.col("categories"), " ").alias("article_categories"),
#                                 f.col("title"),
#                                 f.col("update_date")) \
#     .select(f.concat_ws(" ", f.col("authors")).alias("authors"),
#             *[f.col(c) for c in ["article_id", "article_categories", "title", "update_date"]])
# authors_df.show()
# another_df = authors_df.alias("a1").join(authors_df.alias("a2"),
#                                          on=f.col("a1.article_id") == f.col("a2.article_id"),
#                                          how="inner") \
#     .select(f.col("a1.authors").alias("author_1"),
#             f.col("a2.authors").alias("author_2"),
#             *[f.col("a1." + c).alias(c) for c in ["article_id", "article_categories", "title", "update_date"]]) \
#     .where(f.col("author_1") != f.col("author_2"))
# another_df.show()

import itertools

from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import ArrayType, StructType, StructField, StringType


def get_combinations(x):
    if len(x) == 1:
        name = " ".join(x[0])
        return [(name, name)]
    else:
        # return list(itertools.combinations([" ".join(y) for y in x], 2))
        return list(filter(lambda t: t[0] != t[1], list(itertools.product([" ".join(y) for y in x], repeat=2))))


if __name__ == '__main__':
    session = SparkSession \
        .builder \
        .appName("Testing") \
        .getOrCreate()

    session.sparkContext.setCheckpointDir("./checkpoint")

    sample_size = 100

    metadata_df = session.read.json("arxiv-metadata-oai-snapshot.json").limit(sample_size)

    # metadata_df.printSchema()
    # metadata_df.show()

    # print(metadata_df.rdd.getNumPartitions())

    udf_res_schema = ArrayType(StructType([
        StructField("author_1", StringType(), False),
        StructField("author_2", StringType(), False)
    ]))
    comb_udf = f.udf(get_combinations, udf_res_schema)

    authors_e = metadata_df \
        .withColumn("authors_processed",
                    f.explode(comb_udf(f.col("authors_parsed")))) \
        .select(f.col("authors_processed")["author_1"].alias("src"),
                f.col("authors_processed")["author_2"].alias("dst"),
                f.col("id").alias("article_id"),
                f.split(f.col("categories"), " ").alias("article_categories"),
                f.col("update_date")) \
        .groupBy([f.col("src").alias("src"), f.col("dst")]) \
        .agg(f.count(f.col("article_id")).alias("articles_count"),
             f.collect_list("article_id").alias("articles_ids"),
             f.collect_list("article_categories").alias("articles_categories"),
             f.collect_list("update_date").alias("articles_update_date")) \
        .orderBy("src", ascending=True)
    # .orderBy("articles_count", ascending=False)

    authors_e.show(20)

    # Create a Vertex DataFrame with unique ID column "id"
    authors_v = metadata_df \
        .select(f.explode(f.col("authors_parsed")).alias("author")) \
        .select(f.concat_ws(" ", f.col("author")).alias("id")) \
        .distinct() \
        .orderBy("author", ascending=True)

    # authors_v.show()

    # name = "Berger E. L. "
    # authors_e.where((authors_e["src"] == name) | (authors_e["dst"] == name)).show(100)
    # print(authors_e.where((authors_e["src"] == "Abu-Shammala Wael ") & (authors_e["dst"] == "Torchinsky Alberto "))
    #       .select(f.col("articles_ids"),
    #               f.col("articles_categories"),
    #               f.col("articles_update_date")).collect())
    # authors_e.where(f.col("src") == "Callan David ").show()
    # authors_e.where(f.col("dst") == "Callan David ").show()
    # authors_v.where(f.col("id") == "Choi Dohoon ").show()

    try:
        import graphframes as gf
    except:
        print("Couldn't import graphframes package!")

    g = gf.GraphFrame(authors_v, authors_e)
    # PERSIST THE GRAPH !!!
    g.cache()

    g.degrees.where(f.col("id") == "Berger E. L. ").show()
    g.degrees.orderBy("degree", ascending=False).show()

    # g.connectedComponents().show()
    # g.connectedComponents().orderBy("component").show()

    # authors_e.write \
    #     .option("header", True) \
    #     .mode("overwrite") \
    #     .json("arxiv-processed")

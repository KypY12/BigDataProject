import itertools
import sys

from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import ArrayType, StructType, StructField, StringType


def construct_e_and_v(metadata_df):
    def get_combinations(x):
        if len(x) == 1:
            name = " ".join(x[0])
            return [(name, name)]
        else:
            # return list(itertools.combinations([" ".join(y) for y in x], 2))
            return list(filter(lambda t: t[0] != t[1], list(itertools.product([" ".join(y) for y in x], repeat=2))))

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

    # Create a Vertex DataFrame with unique ID column "id"
    authors_v = metadata_df \
        .select(f.explode(f.col("authors_parsed")).alias("author")) \
        .select(f.concat_ws(" ", f.col("author")).alias("id")) \
        .distinct() \
        .orderBy("author", ascending=True)

    return authors_v, authors_e


def construct_coauthorship_graph(authors_v, authors_e):
    try:
        if "graphframes" not in sys.modules:
            import graphframes as gf
    except:
        print("Couldn't import graphframes package!")

    g = gf.GraphFrame(authors_v, authors_e)
    # PERSIST THE GRAPH !!!
    g.cache()

    return g


def preprocess_data(metadata_df):
    authors_v, authors_e = construct_e_and_v(metadata_df)
    graph = construct_coauthorship_graph(authors_v, authors_e)

    return graph


if __name__ == '__main__':
    session = SparkSession \
        .builder \
        .appName("Testing") \
        .getOrCreate()

    session.sparkContext.setCheckpointDir("./checkpoint")

    sample_size = 100

    metadata_df = session.read.json("../data/original/arxiv-metadata-oai-snapshot.json").limit(sample_size)

    g = preprocess_data(metadata_df)

    g.vertices.show()
    g.edges.show()

    # metadata_df.printSchema()
    # metadata_df.show(20)
    # print(metadata_df.rdd.getNumPartitions())

    # g.degrees.where(f.col("id") == "Berger E. L. ").show()
    # g.degrees.orderBy("degree", ascending=False).show()

    # g.connectedComponents().show()
    # g.connectedComponents().orderBy("component").show()

    # authors_e.write \
    #     .option("header", True) \
    #     .mode("overwrite") \
    #     .json("arxiv-processed")

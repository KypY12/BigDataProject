import sys

from pyspark.sql import SparkSession
import pyspark.sql.functions as f


# import itertools
# from pyspark import StorageLevel
# from pyspark.sql.types import ArrayType, StructType, StructField, StringType


def construct_e_and_v(metadata_df):
    # def get_combinations(x):
    #     if len(x) == 1:
    #         name = " ".join(x[0])
    #         return [(name, name)]
    #     else:
    #         # return list(itertools.combinations([" ".join(y) for y in x], 2))
    #         return list(filter(lambda t: t[0] != t[1], list(itertools.product([" ".join(y) for y in x], repeat=2))))
    #
    # udf_res_schema = ArrayType(StructType([
    #     StructField("author_1", StringType(), False),
    #     StructField("author_2", StringType(), False)
    # ]))
    # comb_udf = f.udf(get_combinations, udf_res_schema)

    # authors_e = metadata_df \
    #     .withColumn("authors_processed",
    #                 f.explode(comb_udf(f.col("authors_parsed"))))
    # authors_e = authors_e.checkpoint()
    #
    # authors_e = authors_e.select(f.col("authors_processed")["author_1"].alias("src"),
    #                              f.col("authors_processed")["author_2"].alias("dst"),
    #                              f.col("id").alias("article_id"),
    #                              f.split(f.col("categories"), " ").alias("article_categories"),
    #                              f.col("update_date")) \
    #     .groupBy([f.col("src"), f.col("dst")]) \
    #     .agg(f.count(f.col("article_id")).alias("articles_count"),
    #          f.collect_list("article_id").alias("articles_ids"),
    #          f.collect_list("article_categories").alias("articles_categories"),
    #          f.collect_list("update_date").alias("articles_update_date")) \
    #     .orderBy("src", ascending=True)
    # # .orderBy("articles_count", ascending=False)

    authors_e = metadata_df \
        .withColumn("authors_processed",
                    f.explode(f.col("authors_parsed"))) \
        .select(*metadata_df.columns,
                f.array_join(f.col("authors_processed"), delimiter=" ").alias("author_name"))

    authors_e.persist()
    # authors_e = authors_e.checkpoint()
    #
    # authors_e = authors_e.alias("left") \
    #     .join(authors_e.alias("right"),
    #           f.col("left.id") == f.col("right.id")) \
    #     .select(f.col("left.author_name").alias("src"),
    #             f.col("right.author_name").alias("dst"),
    #             f.col("left.id").alias("article_id"),
    #             f.split(f.col("left.categories"), " ").alias("article_categories"),
    #             f.col("left.update_date")) \
    #     .where(f.col("src") != f.col("dst"))

    # authors_e = authors_e.checkpoint()

    # authors_e = authors_e \
    #     .groupBy([f.col("src"), f.col("dst")]) \
    #     .agg(f.count(f.col("article_id")).alias("articles_count"),
    #          # f.collect_list("article_id").alias("articles_ids"),
    #          # f.collect_list("article_categories").alias("articles_categories"),
    #          # f.collect_list("update_date").alias("articles_update_date")
    #          ) \
    #     .orderBy("src", ascending=True)
    # .orderBy("articles_count", ascending=False)

    # authors_e = authors_e \
    #     .groupBy([f.col("src"), f.col("dst")]) \
    #     .agg(f.count(f.col("article_id")).alias("articles_count"),
    #          f.collect_list("article_id").alias("articles_ids"),
    #          f.collect_list("article_categories").alias("articles_categories"),
    #          f.collect_list("update_date").alias("articles_update_date")) \
    #     .orderBy("src", ascending=True)
    # .orderBy("articles_count", ascending=False)

    authors_e.show()

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
    g.persist()
    # g.persist(StorageLevel(True, False, False, False, 2))

    return g


def preprocess_data(metadata_df):
    authors_v, authors_e = construct_e_and_v(metadata_df)
    # graph = construct_coauthorship_graph(authors_v, authors_e)

    # return graph


def write_coauthorship_graph(g, path):
    g.vertices.write \
        .option("header", True) \
        .mode("overwrite") \
        .json(f"{path}/vertices")

    g.edges.write \
        .option("header", True) \
        .mode("overwrite") \
        .json(f"{path}/edges")


def read_coauthorship_graph(session, path):
    return construct_coauthorship_graph(session.read.json(f"{path}/vertices"),
                                        session.read.json(f"{path}/edges"))


if __name__ == '__main__':
    session = SparkSession \
        .builder \
        .appName("Preprocessing Main") \
        .getOrCreate()
        # .config("spark.driver.memory", "5g") \

    session.sparkContext.setCheckpointDir("../data/checkpoint_dir")

    metadata_df = session.read.json("../data/original/arxiv-metadata-oai-snapshot.json")

    # sample_size = 100
    # metadata_df = session.read.json("../data/original/arxiv-metadata-oai-snapshot.json").limit(sample_size)

    # metadata_df.select(f.col("id"),
    #                    "submitter",
    #                    "versions").show()

    g = preprocess_data(metadata_df)

    # g.vertices.show()
    # g.edges.show()
    #
    # print(f"Vertx Count : {g.vertices.count()}")
    # print(f"Edge Count : {g.edges.count()}")
    #
    # write_coauthorship_graph(g, "../data/authors_graph")
    # g = read_coauthorship_graph(session, "../data/authors_graph")

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

import sys

from pyspark.sql import SparkSession
import pyspark.sql.functions as f


def construct_e_and_v(metadata_df):
    authors_relations = metadata_df \
        .withColumn("authors_processed",
                    f.explode(f.col("authors_parsed"))) \
        .select(*metadata_df.columns,
                f.array_join(f.col("authors_processed"), delimiter=" ").alias("author_name"))

    authors_relations = authors_relations.alias("left") \
        .join(authors_relations.alias("right"),
              f.col("left.id") == f.col("right.id")) \
        .select(f.col("left.author_name").alias("src"),
                f.col("right.author_name").alias("dst"),
                f.col("left.id").alias("article_id"),
                f.split(f.col("left.categories"), " ").alias("article_categories"),
                f.col("left.update_date")) \
        .where(f.col("src") != f.col("dst"))

    # authors_relations.persist()

    # authors_e = authors_relations \
    #     .groupBy([f.col("src"), f.col("dst")]) \
    #     .agg(f.count(f.col("article_id")).alias("articles_count"))

    authors_e = authors_relations \
        .groupBy([f.col("src"), f.col("dst")]) \
        .agg(f.count(f.col("article_id")).alias("articles_count"),
             f.min(f.col("update_date")).alias("oldest_update_date"),
             f.max(f.col("update_date")).alias("newest_update_date"))

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
            import graphframes
    except:
        print("Couldn't import graphframes package!")

    g = graphframes.GraphFrame(authors_v, authors_e)

    g.persist()
    # g.persist(StorageLevel(True, False, False, False, 2))

    return g


def preprocess_data(metadata_df):
    authors_v, authors_e = construct_e_and_v(metadata_df)
    graph = construct_coauthorship_graph(authors_v, authors_e)

    return graph


def write_coauthorship_graph(g, path):
    g.vertices.write \
        .option("header", True) \
        .mode("overwrite") \
        .parquet(f"{path}/vertices")

    g.edges.write \
        .option("header", True) \
        .mode("overwrite") \
        .parquet(f"{path}/edges")


def read_coauthorship_graph(session, path):
    return construct_coauthorship_graph(session.read.parquet(f"{path}/vertices"),
                                        session.read.parquet(f"{path}/edges"))


if __name__ == '__main__':
    session = SparkSession \
        .builder \
        .appName("Preprocessing Main") \
        .config("spark.executor.memory", "7g") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()
    # .config("spark.default.parallelism", "30") \

    session.sparkContext.setCheckpointDir("../data/checkpoint_dir")

    # metadata_df = session.read.json("../data/original/arxiv-metadata-oai-snapshot.json")

    # g = preprocess_data(metadata_df)
    # write_coauthorship_graph(g, "../data")
    # write_coauthorship_graph(g, "../data/graph_with_more_info")

    # g = read_coauthorship_graph(session, "../data")
    g = read_coauthorship_graph(session, "../data/graph_with_more_info")

    g.vertices.show()
    g.edges.show()

    print(f"Vertex Count : {g.vertices.count()}")
    print(f"Edge Count : {g.edges.count()}")

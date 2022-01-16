from pyspark.ml.clustering import PowerIterationClustering
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

from component_size_analysis.create_connected_components import get_saved_connected_component_subgraph

NUMBER_OF_CLUSTERS_PIC = 2
MAX_ITERATIONS_PIC = 5

session = SparkSession \
    .builder \
    .appName(f"Power Iteration Clustering run clusters{NUMBER_OF_CLUSTERS_PIC}:max_iter{MAX_ITERATIONS_PIC}") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "5") \
    .config("spark.driver.memory", "8g") \
    .getOrCreate()

# from processing.preprocess import preprocess_data

if __name__ == '__main__':
    # sample_size = 100
    # metadata_df = session.read.json("../data/original/arxiv-metadata-oai-snapshot.json").limit(sample_size)
    # metadata_df.show()
    # g = preprocess_data(metadata_df)

    component_id = 1

    g = get_saved_connected_component_subgraph(session, component_id)

    g.vertices.show()
    g.edges.show()

    # Assing an integer id to vertices
    vertices_ids = g.vertices.withColumn("node_id", f.monotonically_increasing_id())
    vertices_ids = vertices_ids.persist()

    vertices_ids.show()

    # Replace vertices string id with their int integer id
    edges_with_ids = g.edges.alias("edges") \
        .join(vertices_ids.alias("vids1"),
              f.col("vids1.id") == f.col("edges.src")) \
        .join(vertices_ids.alias("vids2"),
              f.col("vids2.id") == f.col("edges.dst")) \
        .select(f.col("vids1.node_id").alias("src"),
                f.col("vids2.node_id").alias("dst"),
                f.col("edges.articles_count"))
    edges_with_ids = edges_with_ids.persist()

    edges_with_ids.show()

    g.unpersist()

    # Create PIC model
    pic = PowerIterationClustering(k=NUMBER_OF_CLUSTERS_PIC,
                                   maxIter=MAX_ITERATIONS_PIC,
                                   initMode="degree",
                                   weightCol="articles_count")

    # Cluster data (find communities in the graph)
    communities = pic.assignClusters(edges_with_ids)
    communities = communities.persist()
    edges_with_ids.unpersist()

    # Replace vertices integer id with their initial string id
    communities = communities.alias("comm") \
        .join(vertices_ids.alias("vids"),
              f.col("comm.id") == f.col("vids.node_id")) \
        .select(f.col("vids.id"),
                f.col("comm.cluster").alias("community")) \

    communities = communities.persist()
    vertices_ids.unpersist()

    communities.write \
        .option("header", True) \
        .mode("overwrite") \
        .parquet(f"../data/pic_communities_{component_id}_{NUMBER_OF_CLUSTERS_PIC}_{MAX_ITERATIONS_PIC}")

    communities.show()
    communities.unpersist()

    communities = session.read.parquet(
        f"../data/pic_communities_{component_id}_{NUMBER_OF_CLUSTERS_PIC}_{MAX_ITERATIONS_PIC}")

    communities.show()

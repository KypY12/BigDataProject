from pyspark.sql import SparkSession
import pyspark.sql.functions as f

from processing.preprocess import read_coauthorship_graph

if __name__ == '__main__':
    session = SparkSession \
        .builder \
        .appName("Preprocessing Main") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "8g") \
        .getOrCreate()

    session.sparkContext.setCheckpointDir("../data/checkpoint_dir")

    # g = read_coauthorship_graph(session, "../data")
    #
    # g.vertices.show()
    # g.edges.show()
    #
    # print(f"Vertx Count : {g.vertices.count()}")
    # print(f"Edge Count : {g.edges.count()}")
    #
    # components = g.connectedComponents()
    #
    # components.show()
    #
    # components.write \
    #     .option("header", True) \
    #     .mode("overwrite") \
    #     .parquet("../data/connected_components")

    components = session.read.parquet("../data/connected_components")

    components_counts = components\
        .groupBy(f.col("component"))\
        .count() \
        .orderBy(f.col("count"), ascending=False)

    components_counts.show()

    print("Connected components : ", components.groupBy(f.col("component")).agg(f.col("component")).count())

    print("Max connected component size : ", components.groupBy(f.col("component")).count().agg(f.max("count")).collect())

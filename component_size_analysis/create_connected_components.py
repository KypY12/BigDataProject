from pyspark.sql import SparkSession

from processing.preprocess import read_coauthorship_graph

if __name__ == '__main__':
    session = SparkSession \
        .builder \
        .appName("Preprocessing Main") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "8g") \
        .getOrCreate()

    session.sparkContext.setCheckpointDir("../data/checkpoint_dir")
    g = read_coauthorship_graph(session, "../data")

    g.vertices.show()
    g.edges.show()

    print(f"Vertx Count : {g.vertices.count()}")
    print(f"Edge Count : {g.edges.count()}")

    components = g.connectedComponents()

    components.show()
    print(f"Connected components : {components.count()}")

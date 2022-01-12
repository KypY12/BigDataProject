from pyspark.sql import SparkSession
import pyspark.sql.functions as f

from processing.preprocess import read_coauthorship_graph, construct_coauthorship_graph


def get_component_subgraph(graph, components, target_component):
    # Get the vertices of the component subgraph
    subgraph_vertices = components \
        .where(f.col("component") == target_component) \
        .select(f.col("id"))

    # Get the edges of the component subgraph
    #   Add src components ids
    subgraph_edges = current_graph.edges \
        .join(components,
              current_graph.edges["src"] == components["id"]) \
        .select(f.col("id_component").alias("src_id_component"),
                f.col("src"),
                f.col("dst"),
                f.col("articles_count"))

    #   Add dst components ids
    subgraph_edges = subgraph_edges \
        .join(components,
              subgraph_edges["dst"] == components["id"]) \
        .select(f.col("src_id_component"),
                f.col("id_component").alias("dst_id_component"),
                f.col("src"),
                f.col("dst"),
                f.col("articles_count"))

    #   Keep only edges with nodes in the component
    subgraph_edges = subgraph_edges \
        .where((f.col("src_id_component") == target_component) &
               (f.col("src_id_component") == f.col("dst_id_component")))

    return construct_coauthorship_graph(subgraph_vertices, subgraph_edges)


if __name__ == '__main__':
    session = SparkSession \
        .builder \
        .appName("Preprocessing Main") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "8g") \
        .getOrCreate()

    session.sparkContext.setCheckpointDir("../data/checkpoint_dir")

    current_graph = read_coauthorship_graph(session, "../data")

    # current_graph.vertices.show()
    # current_graph.edges.show()
    #
    # print(f"Vertex Count : {current_graph.vertices.count()}")
    # print(f"Edge Count : {current_graph.edges.count()}")
    #
    # components = current_graph.connectedComponents()
    #
    # components.show()
    #
    # components.write \
    #     .option("header", True) \
    #     .mode("overwrite") \
    #     .parquet("../data/connected_components")

    components = session.read.parquet("../data/connected_components")

    components_counts = components \
        .groupBy(f.col("component")) \
        .count() \
        .orderBy(f.col("count"), ascending=False)

    components_counts.show()

    first_n = 6

    print(components_counts.head(first_n))

    # print("Connected components : ", components.groupBy(f.col("component")).agg(f.col("component")).count())
    #
    # print("Max connected component size : ",
    #       components.groupBy(f.col("component")).count().agg(f.max("count")).collect())

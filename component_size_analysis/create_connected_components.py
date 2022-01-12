from pyspark.sql import SparkSession
import pyspark.sql.functions as f

from processing.preprocess import read_coauthorship_graph, construct_coauthorship_graph


def generate_connected_components(current_graph):
    # Generates the connected components and writes to disk -> cols : [id, component]
    components = current_graph.connectedComponents()

    components.show()

    components.write \
        .option("header", True) \
        .mode("overwrite") \
        .parquet("../data/connected_components")


def get_saved_connected_components(session_):
    # Returns the connected components -> cols : [id, component]
    return session_.read.parquet("../data/connected_components")


def get_saved_connected_component_subgraph(session_, component):
    # Reads from disk and creates the subgraph of connected component given as parameter
    # The component parameter is a number corresponding to the component (its rank by number of nodes)
    comp_vertices = session_.read.parquet(f"../data/connected_components_subgraphs/component_{component}/vertices")
    comp_edges = session_.read.parquet(f"../data/connected_components_subgraphs/component_{component}/edges")

    return construct_coauthorship_graph(comp_vertices, comp_edges)


def get_component_subgraph(graph, components, target_component):
    # Creates the subgraph of graph, where all nodes are in target_component (the actual component id) from components

    # Get the vertices of the component subgraph
    subgraph_vertices = components \
        .where(f.col("component") == target_component) \
        .select(f.col("id"))

    # Get the edges of the component subgraph
    #   Add src components ids
    #   [src, dst, articles_count] -> [src, dst, articles_count, src_component]
    subgraph_edges = graph.edges \
        .join(components,
              graph.edges["src"] == components["id"]) \
        .select(f.col("component").alias("src_component"),
                f.col("src"),
                f.col("dst"),
                f.col("articles_count"))

    #   Add dst components ids
    #   [src, dst, articles_count, src_component] -> [src, dst, articles_count, src_component, dst_component]
    subgraph_edges = subgraph_edges \
        .join(components,
              subgraph_edges["dst"] == components["id"]) \
        .select(f.col("src_component"),
                f.col("component").alias("dst_component"),
                f.col("src"),
                f.col("dst"),
                f.col("articles_count"))

    #   Keep only edges with nodes in the component
    subgraph_edges = subgraph_edges \
        .where((f.col("src_component") == target_component) &
               (f.col("src_component") == f.col("dst_component"))) \
        .select(f.col("src"),
                f.col("dst"),
                f.col("articles_count"))

    return subgraph_vertices, subgraph_edges


def write_first_n_components(current_graph, components, first_n):
    components_counts = components \
        .groupBy(f.col("component")) \
        .count() \
        .orderBy(f.col("count"), ascending=False)

    components_counts.show()
    # print(f"Connected components : {components_counts.count()}")
    # print(f"Max connected component size : {components_counts.agg(f.max('count').alias('max')).collect()[0]['max']}")

    target_connected_components = components_counts.head(first_n)

    for rank, target_comp in enumerate(target_connected_components):
        vertices, edges = get_component_subgraph(current_graph, components, target_comp["component"])

        vertices.write \
            .option("header", True) \
            .mode("overwrite") \
            .parquet(f"../data/connected_components_subgraphs/component_{rank + 1}/vertices")

        edges.write \
            .option("header", True) \
            .mode("overwrite") \
            .parquet(f"../data/connected_components_subgraphs/component_{rank + 1}/edges")


if __name__ == '__main__':
    session = SparkSession \
        .builder \
        .appName("Connected components") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "8g") \
        .getOrCreate()

    session.sparkContext.setCheckpointDir("../data/checkpoint_dir")

    # current_graph = read_coauthorship_graph(session, "../data")

    # generate_connected_components(current_graph)

    # components = get_saved_connected_components(session)

    # write_first_n_components(current_graph, components, first_n=6)

    component_subgraph = get_saved_connected_component_subgraph(session, 2)

    component_subgraph.vertices.show()
    component_subgraph.edges.show()
    print(f"Component vertices : {component_subgraph.vertices.count()}")
    print(f"Component edges : {component_subgraph.edges.count()}")

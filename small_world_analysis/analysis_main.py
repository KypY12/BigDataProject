import pandas as pd
import numpy as np

from processing.preprocess import preprocess_data, write_coauthorship_graph, read_coauthorship_graph
from component_size_analysis.create_connected_components import get_saved_connected_component_subgraph
from small_world_analysis.characteristic_path_length import get_characteristic_path_length
from small_world_analysis.clustering_coefficient import get_clustering_coefficient

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType
import pyspark.sql.functions as f

from networkx.generators.random_graphs import dense_gnm_random_graph, gnp_random_graph
from igraph import Graph
import dask.dataframe as dd


# Generate random graph with same number of authors and connections
def create_random_graph(num_authors, num_connections):

    graph_generated_random = Graph.Erdos_Renyi(n=num_authors, m=int(num_connections))

    df_vertices = pd.DataFrame(data=list(graph_generated_random.get_vertex_dataframe().index), columns=["id"])

    df_edges = dd.from_pandas(graph_generated_random.get_edge_dataframe().astype('uint32'), npartitions=10)
    df_edges.columns = ['src', 'dst']

    # graph_generated_random = dense_gnm_random_graph(n=num_authors, m=num_connections)
    #
    # vertices_graph_generated_random = np.array(graph_generated_random.nodes)
    # edges_graph_generated_random = np.array(graph_generated_random.edges)
    #
    # df_vertices = pd.DataFrame(data=vertices_graph_generated_random, columns=["id"])
    # df_edges = pd.DataFrame(data=edges_graph_generated_random, columns=["src", "dst"])

    vertices_random_graph = session.createDataFrame(data=df_vertices)
    edges_random_graph = session.createDataFrame(data=df_edges.compute())

    df_edges.columns = ['dst', 'src']
    reversed_edges_random_graph = session.createDataFrame(data=df_edges.compute())

    edges_random_graph = edges_random_graph.unionByName(reversed_edges_random_graph)

    try:
        import graphframes
    except:
        print("Couldn't import graphframes package!")

    r_g = graphframes.GraphFrame(vertices_random_graph, edges_random_graph)

    r_g.persist()

    return r_g


if __name__ == "__main__":
    session = SparkSession \
        .builder \
        .appName("Small World Analysis") \
        .config("spark.executor.memory", "8g") \
        .config("spark.executor.cores", "5") \
        .config("spark.driver.memory", "8g") \
        .getOrCreate()
    # .config("spark.driver.memory", "8g") \]

    # Testing Locally
    # sample_size = 15 #500  # 15
    # metadata_df = session.read.json("../data/original/arxiv-metadata-oai-snapshot.json")
    # metadata_df = session.read.json("../data/original/arxiv-metadata-oai-snapshot.json").limit(sample_size)

    # component = preprocess_data(metadata_df)

    component_id = 1
    component = get_saved_connected_component_subgraph(session, component_id)

    # characteristic_path_length_component = get_characteristic_path_length(component_graph=component)
    # print(f"Characteristic Path Length of the Component {component_id}: {characteristic_path_length_component}")

    clustering_coefficient_component = get_clustering_coefficient(component_graph=component)
    print(f"Clustering Coefficient of the Component {component_id}: {clustering_coefficient_component}\n")

    # Generate random graph similar to the component
    random_graph = create_random_graph(num_authors=component.vertices.count(),
                                       num_connections=component.edges.count() / 2)

    # characteristic_path_length_random_graph = get_characteristic_path_length(component_graph=random_graph)
    # print(f"Characteristic Path Length of the Graph Random Generated similar to Component {component_id}: {characteristic_path_length_random_graph}")

    clustering_coefficient_random_graph = get_clustering_coefficient(component_graph=random_graph)
    print(f"Clustering Coefficient of the Graph Random Generated similar to Component {component_id}: {clustering_coefficient_random_graph}\n")
from pyspark.sql import SparkSession

from component_size_analysis.create_connected_components import get_saved_connected_component_subgraph

session = SparkSession \
    .builder \
    .appName("Louvain run") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "5") \
    .config("spark.driver.memory", "8g") \
    .getOrCreate()

from algorithm.louvain import Louvain
# from processing.preprocess import preprocess_data, read_coauthorship_graph

if __name__ == '__main__':

    # sample_size = 100
    # metadata_df = session.read.json("../data/original/arxiv-metadata-oai-snapshot.json").limit(sample_size)
    # metadata_df.show()
    # g = preprocess_data(metadata_df)
    # g = read_coauthorship_graph(session, "../data")

    component_id = 1

    g = get_saved_connected_component_subgraph(session, component_id)

    g.vertices.show()
    g.edges.show()

    louvain_alg = Louvain(g, session, component_name=str(component_id))

    louvain_alg.execute_first_phase(distinct_save_name="after17",
                                    load_from_path="../data/louvain_communities_1/first_phase")

    g.unpersist()

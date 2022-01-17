from processing.preprocess import preprocess_data, write_coauthorship_graph, read_coauthorship_graph
from component_size_analysis.create_connected_components import get_saved_connected_component_subgraph
from small_world_analysis.characteristic_path_length import get_characteristic_path_length
from small_world_analysis.clustering_coefficient import get_clustering_coefficient

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType
import pyspark.sql.functions as f


if __name__ == "__main__":
    session = SparkSession \
        .builder \
        .appName("Communities Analysis") \
        .config("spark.executor.memory", "7g") \
        .config("spark.executor.cores", "5") \
        .getOrCreate()
    # .config("spark.driver.memory", "8g") \

    # Testing Locally
    sample_size = 15 #500  # 15
    # metadata_df = session.read.json("../data/original/arxiv-metadata-oai-snapshot.json")
    metadata_df = session.read.json("../data/original/arxiv-metadata-oai-snapshot.json").limit(sample_size)

    component = preprocess_data(metadata_df)

    component_id = 1
    # component = get_saved_connected_component_subgraph(session, component_id)

    characteristic_path_length_component = get_characteristic_path_length(component_graph=component)
    print(f"Characteristic Path Length of the Component {component_id}: {characteristic_path_length_component}")

    clustering_coefficient_component = get_clustering_coefficient(component_graph=component)
    print(f"Clustering Coefficient of the Component {component_id}: {clustering_coefficient_component}\n")

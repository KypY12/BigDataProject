from processing.preprocess import preprocess_data, write_coauthorship_graph, read_coauthorship_graph
from label_propagation import lpa
from component_size_analysis.create_connected_components import get_saved_connected_component_subgraph
from communities_analysis.modularity_score import get_modularity_score, get_modularity_score_biggest_community

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType
import pyspark.sql.functions as f


# Get number of all communities found
def count_communities(communities_graph):

    num_communities = communities_graph \
        .select(f.col("id_community")).distinct() \
        .count()

    return num_communities


# Get the number of nodes in all the communities
def count_authors_in_every_community(communities_graph):

    num_authors_in_communities = communities_graph \
        .groupBy(f.col("id_community")) \
        .count() \
        .orderBy("count", ascending=False) \
        .withColumnRenamed("count", "nodes_community_count")

    return num_authors_in_communities


if __name__ == "__main__":
    session = SparkSession \
        .builder \
        .appName("Communities Analysis") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "8g") \
        .getOrCreate()

    #Testing Locally
    #sample_size = 500  # 15
    #metadata_df = session.read.json("../data/original/arxiv-metadata-oai-snapshot.json")
    #metadata_df = session.read.json("../data/original/arxiv-metadata-oai-snapshot.json").limit(sample_size)

    #first_component = preprocess_data(metadata_df)

    component_id = 1
    first_component = get_saved_connected_component_subgraph(session, component_id)


    communities = lpa.find_communities_in_graph(graph=first_component)

    # Creates graph that joins the authors with the connected components
    communities_data = communities \
        .join(first_component.edges, communities["author"] == first_component.edges["src"]) \
        .select(f.col("id_community"),
                f.col("author").alias("author/src"),
                f.col("dst"),
                f.col("articles_count"))

    #communities_data = communities_data.persist()
    communities_data.show()

    counter_communities = count_communities(communities_graph=communities_data)
    print(f"Number communities found : {counter_communities}")

    counter_authors_in_every_community = count_authors_in_every_community(communities_graph=communities_data)
    counter_authors_in_every_community.show()

    modularity_score_all_comunities, modularity_score_per_community = get_modularity_score(communities_graph=communities_data)

    print("Modularity score per community:")
    modularity_score_per_community.show()
    print(f"Modularity score all communities : {modularity_score_all_comunities}")

    modularity_score_biggest_community = get_modularity_score_biggest_community(communities_graph=communities_data)
    print(f"Modularity score of the biggest community: {modularity_score_biggest_community}")

    communities.unpersit()
    modularity_score_per_community.unpersist()
    communities_data.unpersist()
from processing.preprocess import preprocess_data, write_coauthorship_graph, read_coauthorship_graph
from label_propagation import lpa
from component_size_analysis.create_connected_components import get_saved_connected_component_subgraph
from communities_analysis.modularity_score import get_modularity_score, get_modularity_score_biggest_community
from communities_analysis.radicchi_strong_score import get_radicchi_strong_score

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


# Get the number of authors in all the communities
def count_authors_in_every_community(communities_graph):

    # Put in a list the authors which belongs to the same community
    grouped_authors_by_community = communities_graph \
        .groupBy(f.col("id_community")) \
        .agg(f.collect_set("author/src").alias("authors_list"))

    # Count the authors in every community
    num_authors_in_communities = grouped_authors_by_community \
        .select("*", f.explode("authors_list").alias("exploded")) \
        .groupBy(f.col("id_community")) \
        .agg(f.count("exploded").alias("authors_count")) \
        .orderBy("authors_count", ascending=False)
    return num_authors_in_communities


if __name__ == "__main__":
    session = SparkSession \
        .builder \
        .appName("Communities Analysis") \
        .config("spark.executor.memory", "7g") \
        .config("spark.executor.cores", "5") \
        .getOrCreate()
    # .config("spark.driver.memory", "8g") \

    # Testing Locally
    # sample_size = 500  # 15
    # metadata_df = session.read.json("../data/original/arxiv-metadata-oai-snapshot.json")
    # metadata_df = session.read.json("../data/original/arxiv-metadata-oai-snapshot.json").limit(sample_size)

    # first_component = preprocess_data(metadata_df)

    component_id = 1
    first_component = get_saved_connected_component_subgraph(session, component_id)

    # communities = lpa.find_communities_in_graph(graph=first_component,
    #                                             save_path=f"../data/lpa_communities_{component_id}")

    # Communities found by LPA after 2 iterations
    # communities = session.read.parquet("/user/data/lpa_communities_1").persist()

    # Communities found by Louvain after 111 iterations
    communities = session.read.parquet("/user/data/louvain_communities_1/first_phase_checkpoint").persist()

    # Creates graph that joins the authors with the connected components
    communities_data = communities \
        .join(first_component.edges, communities["author"] == first_component.edges["src"]) \
        .select(f.col("id_community"),
                f.col("author").alias("author/src"),
                f.col("dst"),
                f.col("articles_count"))

    communities_data = communities_data.persist()
    communities_data.show()

    counter_communities = count_communities(communities_graph=communities_data)
    print(f"Number communities found : {counter_communities}")

    counter_authors_in_every_community = count_authors_in_every_community(communities_graph=communities_data)
    counter_authors_in_every_community.show()

    modularity_score_all_communities, modularity_score_per_community = get_modularity_score(communities_graph=communities_data)

    print("Modularity score per community:")
    modularity_score_per_community.show()
    print(f"Modularity score all communities : {modularity_score_all_communities}")

    # modularity_score_biggest_community = get_modularity_score_biggest_community(communities_graph=communities_data)
    # print(f"Modularity score of the biggest community: {modularity_score_biggest_community}")

    radicchi_strong_score_all_communities = get_radicchi_strong_score(communities_graph=communities_data)
    print(f"Radicchi Strong Score all communities : {radicchi_strong_score_all_communities}")

    modularity_score_per_community.unpersist()
    communities_data.unpersist()
    # communities.unpersit()

import pandas as pd
import copy

from pyspark import StorageLevel
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import LongType

from processing.preprocess import preprocess_data, write_coauthorship_graph, read_coauthorship_graph


if __name__ == '__main__':
    session = SparkSession \
        .builder \
        .appName("Testing") \
        .getOrCreate()

    session.sparkContext.setCheckpointDir("../data/checkpoint_dir")

    sample_size = 500 #15
    metadata_df = session.read.json("../data/original/arxiv-metadata-oai-snapshot.json").limit(sample_size)
    ##metadata_df = session.read.json("../data/original/arxiv-metadata-oai-snapshot.json").limit(1_000_000)
    #metadata_df.show(20)
    g = preprocess_data(metadata_df)
    #
    #write_coauthorship_graph(g, "../data/authors_graph")

    #g = read_coauthorship_graph(session, "../data/authors_graph")

    # g.edges.persist(StorageLevel(True, False, False, False, 2))
    # print(metadata_df.count())
    # print(g.vertices.count())
    # print(g.edges.count())

    #g.vertices.show()
    #g.edges.orderBy("articles_count", ascending=False).show()

    #g.unpersist()

    components = g.labelPropagation(maxIter=10).withColumnRenamed("id", "author").withColumnRenamed("label", "id_component")

    #components = g.connectedComponents().withColumnRenamed("id", "author").withColumnRenamed("component", "id_component")   # .orderBy("component").show()

    components.show(5)

    # Get number of all components
    components.select(f.approx_count_distinct("id_component").alias("Number of connected components")).show()

    # Get values of all components
    components.select(f.col("id_component")).distinct().show()

    # Get the number of nodes in all the components
    components\
        .groupBy(f.col("id_component")) \
        .count() \
        .orderBy("count", ascending=False) \
        .withColumnRenamed("count", "nodes_component_count").show()

    # Get the number of authors(nodes) and their name in all the components
    grouped_authors_by_component = components \
                                    .groupBy(f.col("id_component")) \
                                    .agg(f.count(f.col("author")).alias("authors_count"),
                                         f.collect_list("author").alias("authors")) \
                                    .orderBy("authors_count", ascending=False) \

    grouped_authors_by_component.show()  # (truncate=False)

    current_graph = g

    # Compute W
    W = current_graph.edges \
        .where(f.col("src") != f.col("dst")) \
        .select(f.sum("articles_count").alias("articles_sum")).first()["articles_sum"]
    W = W / 2
    print(W)

    # Get the biggest connected component
    id_first_component = components\
                            .groupBy(f.col("id_component")) \
                            .count() \
                            .orderBy("count", ascending=False) \
                            .first()["id_component"]

    print(id_first_component)

    # Get the authors of the biggest connected component
    authors_first_component = components \
                                .where(f.col("id_component") == id_first_component) \
                                .select(f.col("author"))
    authors_first_component.show()

    # Creates graph that joins the authors with the connected components
    components_data = components \
        .join(current_graph.edges, components["author"] == current_graph.edges["src"]) \
        .select(f.col("id_component"), f.col("author").alias("author/src"), f.col("dst"), f.col("articles_count"))

    components_data.show()

    # Compute S_c to the first connected component
    S_c = components_data \
            .where(f.col("author/src") != f.col("dst")) \
            .where(f.col("id_component") == id_first_component) \
            .select(f.sum("articles_count").alias("articles_sum")).first()["articles_sum"]

    S_c /= 2
    print(S_c)

    # Compute S_c to all the connected components
    sum_weights_between_nodes_in_component = components_data \
        .where(f.col("author/src") != f.col("dst")) \
        .groupBy(f.col("id_component")) \
        .agg((f.sum(f.col("articles_count")) / 2).alias("articles_count_of_component")) \
        .orderBy("articles_count_of_component", ascending=False)

    sum_weights_between_nodes_in_component.show()

    # Compute W_c to the first connected component
    W_c = components_data \
        .where(f.col("author/src") != f.col("dst")) \
        .where(f.col("id_component") == id_first_component) \
        .where(f.col("dst").isin(authors_first_component.select(f.collect_list('author')).first()[0]) == True) \
        .select(f.sum("articles_count").alias("articles_sum")).first()["articles_sum"]

    W_c /= 2
    print(W_c)

    # Creates graph that joins the authors lists with the connected components and authors
    close_components_data = components_data \
        .join(grouped_authors_by_component, components_data["id_component"] == grouped_authors_by_component["id_component"]) \
        .select(components_data["id_component"], f.col("author/src"), f.col("dst"), f.col("articles_count")) \
        .where(f.array_contains(f.col("authors"), f.col("dst")))

    close_components_data.show()

    # Compute W_c to all the connected components
    sum_weights_all_nodes_in_components = close_components_data \
        .where(f.col("author/src") != f.col("dst")) \
        .groupBy(f.col("id_component")) \
        .agg((f.sum(f.col("articles_count")) / 2).alias("articles_count_in_component")) \
        .orderBy("articles_count_in_component", ascending=False)

    sum_weights_all_nodes_in_components.show()

    Q = W_c / W - (S_c / (2 * W)) ** 2
    print(Q)

    modularity_components = sum_weights_between_nodes_in_component \
        .join(sum_weights_all_nodes_in_components, sum_weights_between_nodes_in_component["id_component"] == sum_weights_all_nodes_in_components["id_component"]) \
        .select(sum_weights_between_nodes_in_component["id_component"],
                (f.col("articles_count_in_component") / W - (f.col("articles_count_of_component") / (2 * W)) ** 2).alias("modularity_value")) \
        .orderBy("modularity_value", ascending=False)

    modularity_components.show()

    modularity = modularity_components \
                    .select(f.sum(f.col("modularity_value")))
    modularity.show()










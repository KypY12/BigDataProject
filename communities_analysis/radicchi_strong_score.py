import pyspark.sql.functions as f


# Put in a list the authors which belongs to the same community
def __group_the_authors_to_communities__(communities_graph):

    grouped_authors = communities_graph \
        .groupBy(f.col("id_community")) \
        .agg(f.collect_set("author/src").alias("authors"))

    grouped_authors = grouped_authors.persist()

    return grouped_authors


# Get the number of connections outer the community for every author
def __count_authors_inter_community_links_communities__(communities_graph, grouped_authors_by_community):

    # Creates graph that joins the authors lists with the communities and authors
    inter_communities_data = communities_graph \
        .join(grouped_authors_by_community,
              communities_graph["id_community"] == grouped_authors_by_community["id_community"]) \
        .drop(grouped_authors_by_community["id_community"]) \
        .select(f.col("id_community"),
                f.col("author/src"),
                f.col("dst"),
                f.col("articles_count")) \
        .where(f.array_contains(f.col("authors"), f.col("dst")) == False)

    inter_communities_data = inter_communities_data.persist()

    # Count the connections outer the community for every author
    num_connections_authors_out_community = inter_communities_data \
        .groupBy(f.col("author/src")) \
        .agg((f.sum(f.col("articles_count")) / 2).alias("outer_connections_counter"))

    num_connections_authors_out_community = num_connections_authors_out_community.persist()

    inter_communities_data.unpersist()

    return num_connections_authors_out_community


# Get the number of connections within the community for every author
def __count_authors_intra_community_links_communities__(communities_graph, grouped_authors_by_community):

    # Creates graph that joins the authors lists with the communities and authors
    intra_communities_data = communities_graph \
        .join(grouped_authors_by_community,
              communities_graph["id_community"] == grouped_authors_by_community["id_community"]) \
        .drop(grouped_authors_by_community["id_community"]) \
        .select(f.col("id_community"),
                f.col("author/src"),
                f.col("dst"),
                f.col("articles_count")) \
        .where(f.array_contains(f.col("authors"), f.col("dst")) == True)

    intra_communities_data = intra_communities_data.persist()

    # Count the connections within the community for every author
    num_connections_authors_in_community = intra_communities_data \
        .groupBy(f.col("author/src")) \
        .agg((f.sum(f.col("articles_count")) / 2).alias("within_connections_counter")) \

    num_connections_authors_in_community = num_connections_authors_in_community.persist()

    intra_communities_data.unpersist()

    return num_connections_authors_in_community


def get_radicchi_strong_score(communities_graph):

    grouped_authors_by_community = __group_the_authors_to_communities__(communities_graph)

    connections_authors_within_commnunity = __count_authors_intra_community_links_communities__(communities_graph, grouped_authors_by_community)
    connections_authors_outer_commnunity = __count_authors_inter_community_links_communities__(communities_graph, grouped_authors_by_community)

    connections_authors = connections_authors_within_commnunity \
        .join(connections_authors_outer_commnunity,
              connections_authors_within_commnunity["author/src"]==connections_authors_outer_commnunity["author/src"]) \
        .drop(connections_authors_within_commnunity["author/src"]) \
        .select(f.col("author/src"),
                f.col("within_connections_counter"),
                f.col("outer_connections_counter"))

    connections_authors = connections_authors.persist()

    num_strong_authors = connections_authors \
        .where(f.col("within_connections_counter") > f.col("outer_connections_counter")) \
        .count()

    num_all_authors = connections_authors.distinct().count()

    radicchi_strong_score = None
    if num_all_authors > 0:
        radicchi_strong_score = num_strong_authors / num_all_authors

    grouped_authors_by_community.unpersist()
    connections_authors_within_commnunity.unpersist()
    connections_authors_outer_commnunity.unpersist()
    connections_authors.unpersist()

    return radicchi_strong_score

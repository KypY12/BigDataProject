import pyspark.sql.functions as f


# Compute W
def __compute_total_weight_links_communities_graph__(communities_graph):

    total_weight = communities_graph \
        .where(f.col("author/src") != f.col("dst")) \
        .select(f.sum("articles_count").alias("articles_sum")).first()["articles_sum"]

    total_weight /= 2

    return total_weight

########################################################################################################################

# Compute modularity scores for all the communities #


# Compute S_c of all the communities
def __compute_sum_weight_inter_community_links_communities__(communities_graph):

    # Compute the sum of all link weights of nodes in communities
    sum_weights_all_nodes_in_community = communities_graph \
        .where(f.col("author/src") != f.col("dst")) \
        .groupBy(f.col("id_community")) \
        .agg((f.sum(f.col("articles_count")) / 2).alias("articles_count_of_community")) \
        .orderBy("articles_count_of_community", ascending=False)

    sum_weights_all_nodes_in_community = sum_weights_all_nodes_in_community.persist()

    return sum_weights_all_nodes_in_community


# Compute W_c of all the communities
def __compute_sum_weight_intra_community_links_communities__(communities_graph):

    # Put in a list the authors which belongs to the same community and count them
    grouped_authors_by_community = communities_graph \
        .groupBy(f.col("id_community")) \
        .agg(f.count(f.col("author/src")).alias("authors_count"),
             f.collect_list("author/src").alias("authors")) \
        .orderBy("authors_count", ascending=False)

    grouped_authors_by_community = grouped_authors_by_community.persist()

    # Creates graph that joins the authors lists with the communities and authors
    intra_communities_data = communities_graph \
        .join(grouped_authors_by_community, communities_graph["id_community"] == grouped_authors_by_community["id_community"]) \
        .drop(grouped_authors_by_community["id_community"]) \
        .select(f.col("id_community"),
                f.col("author/src"),
                f.col("dst"),
                f.col("articles_count")) \
        .where(f.array_contains(f.col("authors"), f.col("dst")))

    intra_communities_data = intra_communities_data.persist()

    # Compute the sum of link weights between nodes in communities
    sum_weights_between_nodes_in_community = intra_communities_data \
        .where(f.col("author/src") != f.col("dst")) \
        .groupBy(f.col("id_community")) \
        .agg((f.sum(f.col("articles_count")) / 2).alias("articles_count_in_community")) \
        .orderBy("articles_count_in_community", ascending=False)

    sum_weights_between_nodes_in_community = sum_weights_between_nodes_in_community.persist()

    grouped_authors_by_community.unpersist()
    intra_communities_data.unpersist()

    return sum_weights_between_nodes_in_community


# Compute modularity score based on the formula from paper for all communities
def get_modularity_score(communities_graph):

    total_weight = __compute_total_weight_links_communities_graph__(communities_graph)
    sum_weights_all_nodes_in_community = __compute_sum_weight_inter_community_links_communities__(communities_graph)
    sum_weights_between_nodes_in_community = __compute_sum_weight_intra_community_links_communities__(communities_graph)

    modularity_score_communities = sum_weights_between_nodes_in_community \
        .join(sum_weights_all_nodes_in_community, sum_weights_between_nodes_in_community["id_community"] == sum_weights_all_nodes_in_community["id_community"]) \
        .select(sum_weights_between_nodes_in_community["id_community"],
                (f.col("articles_count_in_community") / total_weight -
                 (f.col("articles_count_of_community") / (2 * total_weight)) ** 2).alias("modularity_score")) \
        .orderBy("modularity_score", ascending=False)

    modularity_score_communities = modularity_score_communities.persist()

    sum_weights_all_nodes_in_community.unpersist()
    sum_weights_between_nodes_in_community.unpersist()

    modularity_score = modularity_score_communities \
        .select(f.sum(f.col("modularity_score")).alias("sum_modularity_scores")) \
        .first()["sum_modularity_scores"]

    return modularity_score, modularity_score_communities


########################################################################################################################

# Compute modularity score for the biggest community #


# Compute S_c of the biggest community
def __compute_sum_weight_inter_community_links_biggest_community__(communities_graph, id_biggest_community):

    # Compute the sum of all link weights of nodes in the biggest community
    S_c = communities_graph \
        .where(f.col("author/src") != f.col("dst")) \
        .where(f.col("id_community") == id_biggest_community) \
        .select(f.sum("articles_count").alias("articles_sum")).first()["articles_sum"]

    S_c /= 2

    return S_c


# Compute W_c of the biggest community
def __compute_sum_weight_intra_community_links_biggest_community__(communities_graph, id_biggest_community):

    # Get the authors of the biggest community
    authors_biggest_community = communities_graph \
        .where(f.col("id_community") == id_biggest_community) \
        .select(f.col("author/src"))

    authors_biggest_community = authors_biggest_community.persist()

    # Compute the sum of link weights between nodes in the biggest community
    W_c = communities_graph \
        .where(f.col("author/src") != f.col("dst")) \
        .where(f.col("id_community") == id_biggest_community) \
        .where(f.col("dst").isin(authors_biggest_community.select(f.collect_list('author/src')).first()[0]) == True) \
        .select(f.sum("articles_count").alias("articles_sum")).first()["articles_sum"]

    W_c /= 2

    authors_biggest_community.unpersist()

    return W_c


# Compute modularity score based on the formula from paper for biggest community
def get_modularity_score_biggest_community(communities_graph):

    id_biggest_community = communities_graph \
        .groupBy(f.col("id_community")) \
        .count() \
        .orderBy("count", ascending=False) \
        .first()["id_community"]

    W = __compute_total_weight_links_communities_graph__(communities_graph)
    S_c = __compute_sum_weight_inter_community_links_biggest_community__(communities_graph, id_biggest_community)
    W_c = __compute_sum_weight_intra_community_links_biggest_community__(communities_graph, id_biggest_community)

    Q = W_c / W - (S_c / (2 * W)) ** 2

    return Q

########################################################################################################################


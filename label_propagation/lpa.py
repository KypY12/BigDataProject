
def find_communities_in_graph(graph, num_iterations_lpa=10):
    found_communities = graph.labelPropagation(maxIter=num_iterations_lpa) \
                             .withColumnRenamed("id", "author").withColumnRenamed("label", "id_community")

    return found_communities


def find_communities_in_graph(graph, num_iterations_lpa=2):
    found_communities = graph.labelPropagation(maxIter=num_iterations_lpa)

    print("Finished0")
    print("count0 : ", found_communities.count())

    found_communities = found_communities.withColumnRenamed("id", "author").withColumnRenamed("label", "id_community")

    print("Finished1")
    print("count1 : ", found_communities.count())

    found_communities = found_communities.persist()

    return found_communities

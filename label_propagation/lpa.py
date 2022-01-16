def find_communities_in_graph(graph, save_path, num_iterations_lpa=5):
    found_communities = graph.labelPropagation(maxIter=num_iterations_lpa)

    print("Finished0")
    print("count0 : ", found_communities.count())

    found_communities = found_communities.withColumnRenamed("id", "author").withColumnRenamed("label", "id_community")

    print("Finished1")
    print("count1 : ", found_communities.count())

    found_communities = found_communities.persist()

    found_communities.write \
        .option("header", True) \
        .mode("overwrite") \
        .parquet(save_path)

    return found_communities

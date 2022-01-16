def find_communities_in_graph(graph, save_path, num_iterations_lpa=2):
    found_communities = graph.labelPropagation(maxIter=num_iterations_lpa) \
        .withColumnRenamed("id", "author").withColumnRenamed("label", "id_community") \
        .persist()

    found_communities.write \
        .option("header", True) \
        .mode("overwrite") \
        .parquet(save_path)

    return found_communities



import pyspark.sql.functions as f


# Compute the average shortest path length between every pair of authors of the component
def get_characteristic_path_length(component_graph):

    print("Print 1 ", component_graph.vertices.count())

    # Compute the shortest paths using already implemented method
    shortest_paths = component_graph \
        .shortestPaths(landmarks=component_graph.edges.select(f.collect_set('src')).first()[0]) \
        .withColumnRenamed("id", "author") \
        .persist()

    print("Print 2 ", component_graph.vertices.count())

    # Get only the lengths of all shortest paths for every author
    shortest_paths = shortest_paths \
        .select(f.col('author'), f.map_values('distances').alias('list_distances'))

    print("Print 3 ", component_graph.vertices.count())

    # Sum the lengths of all shortest paths for every author
    shortest_paths = shortest_paths \
        .select("*", f.explode("list_distances").alias("exploded")) \
        .groupBy(f.col("author")) \
        .agg(f.sum("exploded").alias("summed_distances")) \
        .orderBy("summed_distances", ascending=False)

    shortest_paths = shortest_paths.persist()
    print("Print 4 ", component_graph.vertices.count())

    # Sum the lengths of all shortest paths for all the authors
    sum_all_distances = shortest_paths \
        .select(f.sum(f.col('summed_distances'))) \
        .first()[0]

    # Get number of all authors in the component
    counter_authors = shortest_paths \
        .select(f.col('author')) \
        .count()

    # Compute characteristic path length
    avg_shortest_path_length = sum_all_distances / counter_authors

    shortest_paths.unpersist()

    return avg_shortest_path_length
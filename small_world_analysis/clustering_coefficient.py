import pyspark.sql.functions as f


# Compute the average clustering coefficient of authors of the component
def get_clustering_coefficient(component_graph):

    # Put in a list the authors which are direct neighbors with the specific author
    direct_neighbours_of_authors = component_graph.edges \
        .where(f.col("src") != f.col("dst")) \
        .groupBy(f.col("src")) \
        .agg(f.count(f.col('dst')).alias("author_links_count"),
             f.collect_set("dst").alias("author_neighbors")) \
        .withColumnRenamed("src", "author")

    direct_neighbours_of_authors = direct_neighbours_of_authors.persist()

    # Count the authors which are direct neighbors with the specific author
    num_direct_neighbours_of_authors = direct_neighbours_of_authors \
        .select("*", f.explode("author_neighbors").alias("exploded")) \
        .groupBy(f.col("author"), f.col("author_links_count")) \
        .agg(f.count("exploded").alias("author_neighbors_count"))

    num_direct_neighbours_of_authors = num_direct_neighbours_of_authors.persist()

    # Compute the clustering coefficient for every author
    clustering_coefficient_of_authors = num_direct_neighbours_of_authors \
        .select(f.col("author"),
                (f.col("author_links_count") / (
                            ((f.col("author_neighbors_count") + 1) * f.col("author_neighbors_count")) / 2))
                .alias("clustering_coefficient"))

    clustering_coefficient_of_authors = clustering_coefficient_of_authors.persist()

    # Sum the clustering coefficients for all the authors
    sum_all_clustering_coefficients = clustering_coefficient_of_authors \
        .select(f.sum(f.col('clustering_coefficient'))) \
        .first()[0]

    # Get number of all authors in the component
    counter_authors = clustering_coefficient_of_authors \
        .select(f.col('author')) \
        .count()

    # Compute clustering coefficient for the component
    avg_clustering_coefficient = sum_all_clustering_coefficients / counter_authors

    direct_neighbours_of_authors.unpersist()
    num_direct_neighbours_of_authors.unpersist()
    clustering_coefficient_of_authors.unpersist()

    return avg_clustering_coefficient
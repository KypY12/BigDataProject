from pyspark.sql import SparkSession, functions as f

from component_size_analysis.create_connected_components import get_saved_connected_components

if __name__ == '__main__':
    session = SparkSession \
        .builder \
        .appName("Connected components") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "8g") \
        .getOrCreate()

    components = get_saved_connected_components(session)

    components_counts = components \
        .groupBy("component") \
        .count() \
        .orderBy("count", ascending=False)

    print("Number of connected components : ", components_counts.count())
    print("Number of single-node components : ", components_counts.where(f.col("count") == 1).count())
    print("Number of multiple-node components : ", components_counts.where(f.col("count") > 1).count())
    print("Number of components with at least 10 nodes : ", components_counts.where(f.col("count") > 10).count())
    print("Number of components with at least 100 nodes : ", components_counts.where(f.col("count") > 100).count())
    print("Number of components with at least 1000 nodes : ", components_counts.where(f.col("count") > 1000).count())
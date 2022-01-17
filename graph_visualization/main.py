from pyspark.sql import SparkSession, functions as f

if __name__ == '__main__':
    session = SparkSession \
        .builder \
        .appName("Graph visualization preprocessing") \
        .getOrCreate()
    # .config("spark.executor.memory", "8g") \
    # .config("spark.driver.memory", "8g") \

    component = 1

    # comp_vertices = session.read.parquet(f"../local_data/connected_components/component_{component}/vertices")
    comp_edges = session.read.parquet(f"../local_data/connected_components/component_{component}/edges")

    comp_edges.coalesce(1).write.csv(path=f"../local_data/connected_components/component_1/csv_edges",
                                     header=True,
                                     sep=";")
    # comp_vertices.coalesce(1).sort(f.col("id"))\
    #     .write.csv(path=f"../local_data/connected_components/component_1/csv_vertices",
    #                header=True,
    #                sep=";")

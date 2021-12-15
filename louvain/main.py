import itertools

from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import ArrayType, StructType, StructField, StringType
from processing.preprocess import preprocess_data


if __name__ == '__main__':
    session = SparkSession \
        .builder \
        .appName("Testing") \
        .getOrCreate()

    session.sparkContext.setCheckpointDir("./checkpoint")

    sample_size = 100

    metadata_df = session.read.json("../data/original/arxiv-metadata-oai-snapshot.json").limit(sample_size)

    g = preprocess_data(metadata_df)

    g.vertices.show()
    g.edges.show()

    # metadata_df.printSchema()
    # metadata_df.show(20)
    # print(metadata_df.rdd.getNumPartitions())

    # g.degrees.where(f.col("id") == "Berger E. L. ").show()
    # g.degrees.orderBy("degree", ascending=False).show()

    # g.connectedComponents().show()
    # g.connectedComponents().orderBy("component").show()

    # authors_e.write \
    #     .option("header", True) \
    #     .mode("overwrite") \
    #     .json("arxiv-processed")

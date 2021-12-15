import itertools

from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import ArrayType, StructType, StructField, StringType
from processing.preprocess import preprocess_data, write_coauthorship_graph, read_coauthorship_graph


class Louvain:

    def __init__(self,
                 graph,
                 alg_max_iterations=-1,
                 fp_max_iterations=-1,
                 sp_max_iterations=-1):
        self.alg_max_iterations = alg_max_iterations
        self.fp_max_iterations = fp_max_iterations
        self.sp_max_iterations = sp_max_iterations
        self.graph = graph

    def __first_phase__(self, m):



        pass

    def __second_phase__(self):
        pass

    def execute(self):
        print("Executing ...")


'''
Pseudo-code :

    First Phase:
    
    current_partitions_df = [...]
    new_partitions_df = []
    
    for each node i:
        k_i = degree of node i
        
        D = select node partition
        k_i_D = compute k_i_D (excluding node i from D)
        
        sum_tot_D = compute sum_tot_D (excluding i from D)
        
        Cs = select node neighbouring partitions
        
        max_delta_Q = -inf
        max_index = -1
        for each neigh_part in node_neigh_partitions:
            k_i_C = compute current k_i_C
            sum_tot_C = compute current sum_tot_C (excluding i from D)
            
            delta_Q = compute delta_Q (final formula)
            if delta_Q > max_delta_Q:
                max_delta_Q = delta_Q
                max_index = -1
                
        if max_delta_Q > 0:
            change partition of node i from D to Cs[max_index]

'''




if __name__ == '__main__':
    session = SparkSession \
        .builder \
        .appName("Testing") \
        .getOrCreate()

    session.sparkContext.setCheckpointDir("../data/checkpoint_dir")

    # sample_size = 100
    # metadata_df = session.read.json("../data/original/arxiv-metadata-oai-snapshot.json").limit(sample_size)
    # g = preprocess_data(metadata_df)
    # write_coauthorship_graph(g, "../data/authors_graph")
    g = read_coauthorship_graph(session, "../data/authors_graph")


    # g.vertices.show()
    # g.edges.show()

    g.vertices.withColumn("community", f.monotonically_increasing_id()).show()



    # louvain_com_det = Louvain()

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

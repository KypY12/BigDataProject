import sys
import time

import pyspark.sql
import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StringType


class Louvain:
    class CurrentGraph:

        def __init__(self, vertices, edges):
            self.vertices = vertices
            self.edges = edges

        def persist(self):
            self.vertices = self.vertices.persist()
            self.edges = self.edges.persist()

            return self

        def unpersist(self):
            self.vertices.unpersist()
            self.edges.unpersist()

    def __init__(self,
                 graph,
                 session,
                 component_name="",
                 max_iterations=-1,
                 fp_max_iterations=-1
                 ):

        self.max_iterations = max_iterations
        self.fp_max_iterations = fp_max_iterations
        self.original_graph = graph

        self.component_name = component_name
        self.communities_save_path = f"../data/louvain_communities_{component_name}"
        self.fp_communities_save_path = f"../data/louvain_communities_{component_name}/first_phase_checkpoint"

        self.session = session

        self.first_phase_communities = session.createDataFrame([], StructType([]))

        # First phase variables
        self.k_i = None

    def setup_checkpoint(self, df, path):
        df.write \
            .option("header", True) \
            .mode("overwrite") \
            .parquet(path)

        df.unpersist()

        return self.session.read.parquet(path).persist()

    def __compute_modularity_terms__(self, current_graph, current_communities):

        single_node_communities = current_communities.groupBy(f.col("community")) \
            .agg(f.count("community").alias("nodes_count")) \
            .where(f.col("nodes_count") == 1)

        single_node_communities = single_node_communities.persist()

        single_node_communities_edges = single_node_communities \
            .join(current_communities.alias("com"),
                  single_node_communities["community"] == current_communities["community"]) \
            .select(f.col("com.id"),
                    f.col("com.community"))

        single_node_communities_edges = single_node_communities_edges.alias("snc1") \
            .join(single_node_communities_edges.alias("snc2"),
                  f.col("snc1.id") == f.col("snc2.id")) \
            .select(f.col("snc1.id").alias("src"),
                    f.col("snc2.id").alias("dst"),
                    f.col("snc1.community").alias("community_src"),
                    f.col("snc2.community").alias("community_dst"),
                    f.lit(0).alias("articles_count"))

        single_node_communities_edges = single_node_communities_edges.persist()

        # Construct an auxiliary table : [community_src, community_dst, src, dst, articles_count]
        comm_aux_df = current_graph.edges \
            .where(f.col("src") != f.col("dst")) \
            .join(current_communities, current_graph.edges["dst"] == current_communities["id"]) \
            .select(f.col("community").alias("community_dst"),
                    f.col("src"),
                    f.col("dst"),
                    f.col("articles_count"))
        comm_aux_df = comm_aux_df \
            .join(current_communities, comm_aux_df["src"] == current_communities["id"]) \
            .select(f.col("community").alias("community_src"),
                    *comm_aux_df.columns)

        comm_aux_df = comm_aux_df.unionByName(single_node_communities_edges)

        comm_aux_df = comm_aux_df.persist()
        single_node_communities_edges.unpersist()

        # Compute all k_i (i is considered here the src node)
        # Needs to be computed once (each iteration's update doesn't change the values in this dataframe)
        if self.k_i is None:
            self.k_i = comm_aux_df \
                .groupBy(f.col("src").alias("i")) \
                .agg(f.sum("articles_count").alias("k_i"))
            self.k_i = self.k_i.persist()

        # Compute all k_i_S and k_i_D (i is considered here the src node)
        k_i_C = comm_aux_df \
            .groupBy([f.col("src").alias("i"),
                      f.col("community_dst").alias("C")]) \
            .agg(f.sum("articles_count").alias("k_i_C"))

        k_i_C = k_i_C.persist()

        # Compute all sum_tot_S and sum_tot_D
        sum_tot_C = comm_aux_df \
            .groupBy(f.col("community_src").alias("C")) \
            .agg(f.sum("articles_count").alias("sum_tot_C"))

        sum_tot_C = sum_tot_C.persist()

        # Compute modularity terms; a table with the following schema:
        # | i | S_i | D_i | k_i | k_i_S | k_i_D | sum_tot_S | sum_tot_D |
        mt = comm_aux_df \
            .select(f.col("src"),
                    f.col("community_src").alias("S_i")) \
            .distinct() \
            .alias("mt")

        comm_aux_df.unpersist()

        mt = mt \
            .join(self.k_i.alias("k_i"),
                  on=f.col("mt.src") == f.col("k_i.i")) \
            .select(f.col("k_i.i"),
                    f.col("mt.S_i"),
                    f.col("k_i.k_i")) \
            .alias("mt")

        mt = mt \
            .join(k_i_C.alias("k_i_S"),
                  on=[f.col("mt.i") == f.col("k_i_S.i"),
                      f.col("mt.S_i") == f.col("k_i_S.C")]) \
            .select(f.col("mt.i"),
                    f.col("mt.S_i"),
                    f.col("mt.k_i"),
                    f.col("k_i_S.k_i_C").alias("k_i_S")) \
            .alias("mt")

        mt = mt \
            .join(k_i_C.alias("k_i_D"),
                  on=f.col("mt.i") == f.col("k_i_D.i")) \
            .select(f.col("mt.i"),
                    f.col("mt.S_i"),
                    f.col("k_i_D.C").alias("D_i"),
                    f.col("mt.k_i"),
                    f.col("mt.k_i_S"),
                    f.col("k_i_D.k_i_C").alias("k_i_D")) \
            .alias("mt")

        k_i_C.unpersist()

        mt = mt \
            .join(sum_tot_C.alias("sum_tot_S"),
                  on=f.col("mt.S_i") == f.col("sum_tot_S.C")) \
            .select(f.col("mt.i"),
                    f.col("mt.S_i"),
                    f.col("mt.D_i"),
                    f.col("mt.k_i"),
                    f.col("mt.k_i_S"),
                    f.col("mt.k_i_D"),
                    f.col("sum_tot_S.sum_tot_C").alias("sum_tot_S")) \
            .alias("mt")

        mt = mt \
            .join(sum_tot_C.alias("sum_tot_D"),
                  on=f.col("mt.D_i") == f.col("sum_tot_D.C")) \
            .select(f.col("mt.i"),
                    f.col("mt.S_i"),
                    f.col("mt.D_i"),
                    f.col("mt.k_i"),
                    f.col("mt.k_i_S"),
                    f.col("mt.k_i_D"),
                    f.col("mt.sum_tot_S"),
                    f.col("sum_tot_D.sum_tot_C").alias("sum_tot_D"))

        sum_tot_C.unpersist()

        mt = mt.persist()

        return mt, single_node_communities

    def __fp_iteration__(self, current_graph, current_communities, two_m, two_m_sq):

        mt, single_node_communities = self.__compute_modularity_terms__(current_graph, current_communities)

        # Compute modularity change (delta Q)
        mc = mt.withColumn("delta_Q",
                           (f.col("k_i_D") - f.col("k_i_S")) / two_m + f.col("k_i") *
                           (2 * (f.col("sum_tot_S") - f.col("sum_tot_D")) - f.col(
                               "k_i")) / two_m_sq)

        mt.unpersist()
        mc = mc.persist()

        # Compute the strictly positive max value of modularity changes of each node i
        positive_max_mc = mc \
            .groupBy("i") \
            .agg(f.max("delta_Q").alias("max_delta_Q")) \
            .where(f.col("max_delta_Q") > 0)

        print("Positive Max Modularity Change checking")

        # If there are no strictly positive modularity changes, then stop the first phase algorithm
        if not positive_max_mc.first():

            print("No Positive Max Modularity Change")

            positive_max_mc.unpersist()
            mc.unpersist()
            single_node_communities.unpersist()

            self.first_phase_communities = current_communities
            new_communities = None

        else:
            positive_max_mc = positive_max_mc.alias("pmmc") \
                .join(mc.alias("mc"),
                      [f.col("pmmc.i") == f.col("mc.i"),
                       f.col("pmmc.max_delta_Q") == f.col("mc.delta_Q")]) \
                .select(f.col("pmmc.i"),
                        f.col("mc.D_i"),
                        f.col("pmmc.max_delta_Q")) \
                .groupBy([f.col("i"),
                          f.col("max_delta_Q")]) \
                .agg(f.min("D_i").alias("D_i"))
            # .dropDuplicates(["i", "max_delta_Q"])

            mc.unpersist()

            updated_communities_df = current_communities.alias("comm") \
                .join(positive_max_mc.alias("pmmc"),
                      f.col("comm.id") == f.col("pmmc.i"),
                      how="left")

            current_communities.unpersist()
            positive_max_mc.unpersist()

            updated_communities_df = updated_communities_df.alias("ucd") \
                .join(single_node_communities.alias("snc"),
                      f.col("ucd.community") == f.col("snc.community"),
                      how="left") \
                .select(f.col("ucd.id"),
                        f.col("ucd.community"),
                        f.col("ucd.D_i"),
                        f.col("ucd.max_delta_Q"),
                        f.col("snc.nodes_count").alias("is_old_comm_single")
                        ) \
                .alias("ucd") \
                .join(single_node_communities.alias("snc"),
                      f.col("ucd.D_i") == f.col("snc.community"),
                      how="left") \
                .select(f.col("ucd.id"),
                        f.col("ucd.community"),
                        f.when((f.col("ucd.is_old_comm_single") == 1) &
                               (f.col("snc.nodes_count") == 1) &
                               (f.col("ucd.community") < f.col("ucd.D_i")),
                               None)
                        .otherwise(f.col("ucd.D_i")).alias("D_i"),
                        f.col("ucd.max_delta_Q"))

            updated_communities_df = updated_communities_df.persist()
            single_node_communities.unpersist()

            new_communities = updated_communities_df \
                .select(f.col("id"),
                        f.when(f.col("D_i").isNotNull(),
                               f.col("D_i"))
                        .otherwise(f.col("community")).alias("community"))

            new_communities = new_communities.persist()
            updated_communities_df.unpersist()

            print("New Communities")
            new_communities.show()

        return new_communities

    def __first_phase__(self, m, current_graph, load_from_path=None):
        print("Executing first phase ...")

        two_m = 2 * m
        two_m_sq = two_m ** 2

        # Assign each node to its own community
        if load_from_path is None:
            current_communities = current_graph.vertices \
                .withColumn("community", f.monotonically_increasing_id()) \
                .persist()
        else:
            current_communities = self.session.read.parquet(load_from_path).persist()

        if self.fp_max_iterations == -1:

            iteration = 0
            while current_communities:

                start = time.perf_counter()

                current_communities = self.__fp_iteration__(current_graph, current_communities, two_m, two_m_sq)
                if current_communities:
                    current_communities = self.setup_checkpoint(current_communities, self.fp_communities_save_path)

                finish = time.perf_counter()

                print(f"First phase - iteration {iteration} -- {finish - start} seconds")
                iteration += 1

        else:

            for iteration in range(self.fp_max_iterations):

                start = time.perf_counter()

                current_communities = self.__fp_iteration__(current_graph, current_communities, two_m, two_m_sq)
                if current_communities:
                    current_communities = self.setup_checkpoint(current_communities, self.fp_communities_save_path)

                finish = time.perf_counter()

                print(f"First phase - iteration {iteration} -- {finish - start} seconds")

                if current_communities is None:
                    break

        self.k_i.unpersist()
        self.k_i = None

    def __second_phase__(self, current_graph):
        print("Executing second phase ...")

        current_communities = self.first_phase_communities

        current_communities = current_communities.withColumn("community_str", f.col("community").cast(StringType()))

        new_e = current_graph.edges \
            .select(f.col("src"),
                    f.col("dst"),
                    f.col("articles_count")) \
            .alias("edges") \
            .join(current_communities.alias("comms1"),
                  f.col("edges.src") == f.col("comms1.id")) \
            .join(current_communities.alias("comms2"),
                  f.col("edges.dst") == f.col("comms2.id")) \
            .select(f.col("src"),
                    f.col("dst"),
                    f.col("articles_count"),
                    f.col("comms1.community_str").alias("src_comm"),
                    f.col("comms2.community_str").alias("dst_comm")) \
            .where(f.col("src_comm") != f.col("dst_comm")) \
            .groupBy([f.col("src_comm").alias("src"),
                      f.col("dst_comm").alias("dst")]) \
            .agg(f.sum("articles_count").alias("articles_count"))

        new_g = None
        if new_e.first():
            new_v = current_communities.select(f.col("community").alias("id"))

            new_g = self.CurrentGraph(new_v, new_e)
            new_g = new_g.persist()

        return new_g

    def execute(self):
        print("Executing ...")

        current_graph = self.original_graph

        iteration = 0

        while current_graph:
            start = time.perf_counter()

            m = current_graph.edges \
                .where(f.col("src") != f.col("dst")) \
                .select(f.sum("articles_count").alias("articles_sum")).first()["articles_sum"]
            m = m / 2

            self.__first_phase__(m, current_graph)

            self.first_phase_communities.write \
                .option("header", True) \
                .mode("overwrite") \
                .json(f"{self.communities_save_path}/iter_{iteration}")

            new_graph = self.__second_phase__(current_graph)

            self.first_phase_communities.unpersist()
            current_graph.unpersist()

            finish = time.perf_counter()
            print(f"Louvain - iteration {iteration} -- {finish - start} seconds")

            iteration += 1
            if (0 < self.max_iterations <= iteration) or new_graph is None:
                break
            else:
                current_graph = new_graph

    def execute_first_phase(self, distinct_save_name, load_from_path=None):
        print("Executing ...")

        current_graph = self.original_graph

        start = time.perf_counter()

        m = current_graph.edges \
            .where(f.col("src") != f.col("dst")) \
            .select(f.sum("articles_count").alias("articles_sum")).first()["articles_sum"]
        m = m / 2

        self.__first_phase__(m, current_graph, load_from_path)

        self.first_phase_communities.write \
            .option("header", True) \
            .mode("overwrite") \
            .json(f"{self.communities_save_path}/iter_{distinct_save_name}")

        self.first_phase_communities.unpersist()

        finish = time.perf_counter()
        print(f"Louvain - first phase - {distinct_save_name} -- {finish - start} seconds")

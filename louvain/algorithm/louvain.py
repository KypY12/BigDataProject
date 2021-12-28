import time

import pyspark.sql
import pyspark.sql.functions as f


class Louvain:

    def __init__(self,
                 graph,
                 alg_max_iterations=-1,
                 fp_max_iterations=-1,
                 sp_max_iterations=-1):
        self.alg_max_iterations = alg_max_iterations
        self.fp_max_iterations = fp_max_iterations
        self.sp_max_iterations = sp_max_iterations
        self.original_graph = graph

        # First phase variables
        self.k_i = None

    def __compute_modularity_terms__(self, current_graph, current_communities):


        single_node_communities = current_communities.groupBy(f.col("community")) \
            .agg(f.count("community").alias("nodes_count")) \
            .where(f.col("nodes_count") == 1)

        single_node_communities.persist()

        print("SINGLE nodes COMM FUNCS")
        print(single_node_communities.count())
        single_node_communities.show()

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

        print("SINGLE edges COMM FUNCS")
        print(single_node_communities_edges.count())
        single_node_communities_edges.show()

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

        print("AUX COMM FUNCS1")
        comm_aux_df = comm_aux_df.unionByName(single_node_communities_edges)
        print("AUX COMM FUNCS2")

        single_node_communities_edges.unpersist()

        comm_aux_df.persist()

        # Compute all k_i (i is considered here the src node)
        # Needs to be computed once (each iteration's update doesn't change the values in this dataframe)
        if self.k_i is None:
            self.k_i = comm_aux_df \
                .groupBy(f.col("src").alias("i")) \
                .agg(f.sum("articles_count").alias("k_i"))
            self.k_i.persist()

        # Compute all k_i_S and k_i_D (i is considered here the src node)
        k_i_C = comm_aux_df \
            .groupBy([f.col("src").alias("i"),
                      f.col("community_dst").alias("C")]) \
            .agg(f.sum("articles_count").alias("k_i_C"))

        # Compute all sum_tot_S and sum_tot_D
        sum_tot_C = comm_aux_df \
            .groupBy(f.col("community_src").alias("C")) \
            .agg(f.sum("articles_count").alias("sum_tot_C"))

        print("compute COMM FUNCS")

        # Compute modularity terms; a table with the following schema:
        # | i | S_i | D_i | k_i | k_i_S | k_i_D | sum_tot_S | sum_tot_D |
        mt = comm_aux_df \
            .select(f.col("src"),
                    f.col("community_src")) \
            .distinct() \
            .alias("mt") \
            .join(self.k_i.alias("k_i"),
                  on=f.col("mt.src") == f.col("k_i.i")) \
            .select(f.col("k_i.i"),
                    f.col("mt.community_src").alias("S_i"),
                    f.col("k_i.k_i")) \
            .alias("mt") \
            .join(k_i_C.alias("k_i_S"),
                  on=[f.col("mt.i") == f.col("k_i_S.i"),
                      f.col("mt.S_i") == f.col("k_i_S.C")]) \
            .select(f.col("mt.i"),
                    f.col("mt.S_i"),
                    f.col("mt.k_i"),
                    f.col("k_i_S.k_i_C").alias("k_i_S")) \
            .alias("mt") \
            .join(k_i_C.alias("k_i_D"),
                  on=f.col("mt.i") == f.col("k_i_D.i")) \
            .select(f.col("mt.i"),
                    f.col("mt.S_i"),
                    f.col("k_i_D.C").alias("D_i"),
                    f.col("mt.k_i"),
                    f.col("mt.k_i_S"),
                    f.col("k_i_D.k_i_C").alias("k_i_D")) \
            .alias("mt") \
            .join(sum_tot_C.alias("sum_tot_S"),
                  on=f.col("mt.S_i") == f.col("sum_tot_S.C")) \
            .select(f.col("mt.i"),
                    f.col("mt.S_i"),
                    f.col("mt.D_i"),
                    f.col("mt.k_i"),
                    f.col("mt.k_i_S"),
                    f.col("mt.k_i_D"),
                    f.col("sum_tot_S.sum_tot_C").alias("sum_tot_S")) \
            .alias("mt") \
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


        print("MT1 COMM FUNCS")

        comm_aux_df.unpersist()
        k_i_C.unpersist()
        sum_tot_C.unpersist()

        print("MT2 COMM FUNCS")


        return mt, single_node_communities

    def __fp_iteration__(self, current_graph, current_communities, two_m, two_m_sq):

        mt, single_node_communities = self.__compute_modularity_terms__(current_graph, current_communities)

        # Compute modularity change (delta Q)
        mc = mt.withColumn("delta_Q",
                           (f.col("k_i_D") - f.col("k_i_S")) / two_m + f.col("k_i") *
                           (2 * (f.col("sum_tot_S") - f.col("sum_tot_D")) - f.col(
                               "k_i")) / two_m_sq)

        print("MC1 finished")
        mt.unpersist()
        print("MC2 finished")

        # mc.show()

        # Compute the strictly positive max value of modularity changes of each node i
        positive_max_mc = mc \
            .groupBy("i") \
            .agg(f.max("delta_Q").alias("max_delta_Q")) \
            .where(f.col("max_delta_Q") > 0)

        print("PMMC1 finished")

        # If there are no strictly positive modularity changes, then stop the first phase algorithm
        if not positive_max_mc.first():
            positive_max_mc.unpersist()
            mc.unpersist()
            current_communities.unpersist()
            single_node_communities.unpersist()

            return False

        positive_max_mc = positive_max_mc.alias("pmmc") \
            .join(mc.alias("mc"),
                  [f.col("pmmc.i") == f.col("mc.i"),
                   f.col("pmmc.max_delta_Q") == f.col("mc.delta_Q")]) \
            .select(f.col("pmmc.i"),
                    f.col("mc.D_i"),
                    f.col("pmmc.max_delta_Q")) \
            .dropDuplicates(["i", "max_delta_Q"])

        print("PMMC2 finished")

        mc.unpersist()

        updated_communities_df = current_communities.alias("comm") \
            .join(positive_max_mc.alias("pmmc"),
                  f.col("comm.id") == f.col("pmmc.i"),
                  how="left")

        print("UCD1 finished")

        current_communities.unpersist()
        positive_max_mc.unpersist()

        updated_communities_df = updated_communities_df.alias("ucd") \
            .join(single_node_communities.alias("snc"),
                  f.col("ucd.community") == f.col("snc.community"),
                  how="left") \
            .select(f.col("ucd.id"),
                    f.col("ucd.community"),
                    f.col("ucd.i"),
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
                    f.col("ucd.i"),
                    f.when((f.col("ucd.is_old_comm_single") == 1) &
                           (f.col("snc.nodes_count") == 1) &
                           (f.col("ucd.community") < f.col("ucd.D_i")),
                           None)
                    .otherwise(f.col("ucd.D_i")).alias("D_i"),
                    f.col("ucd.max_delta_Q"))

        print("UCD2 finished")

        single_node_communities.unpersist()

        new_communities = updated_communities_df \
            .select(f.col("id"),
                    f.when(f.col("D_i").isNotNull(),
                           f.col("D_i"))
                    .otherwise(f.col("community")).alias("community"))

        print("NEW COMM finished")

        updated_communities_df.unpersist()

        new_communities.persist()

        return new_communities

    def __first_phase__(self, m, current_graph):

        two_m = 2 * m
        two_m_sq = two_m ** 2

        # Assign each node to its own community
        current_communities = current_graph.vertices.withColumn("community", f.monotonically_increasing_id())
        current_communities.persist()

        if self.fp_max_iterations == -1:

            start = time.perf_counter()
            current_communities = self.__fp_iteration__(current_graph, current_communities, two_m, two_m_sq)
            finish = time.perf_counter()

            iteration = 0
            print(f"Iteration {iteration} -- {finish-start} seconds")

            while current_communities:
                start = time.perf_counter()
                current_communities = self.__fp_iteration__(current_graph, current_communities, two_m, two_m_sq)
                finish = time.perf_counter()

                iteration += 1
                print(f"Iteration {iteration} -- {finish-start} seconds")

        else:

            for iteration in range(self.fp_max_iterations):
                start = time.perf_counter()
                current_communities = self.__fp_iteration__(current_graph, current_communities, two_m, two_m_sq)
                finish = time.perf_counter()

                if not current_communities:
                    print(f"Iteration: {iteration} -- {finish-start} seconds")
                    break
                elif iteration == self.fp_max_iterations - 1:
                    current_communities.unpersist()

                print(f"Iteration {iteration} -- {finish-start} seconds")

        self.k_i.unpersist()
        self.k_i = None

    def __second_phase__(self):
        pass

    def execute(self):
        print("Executing ...")

        current_graph = self.original_graph

        m = current_graph.edges \
            .where(f.col("src") != f.col("dst")) \
            .select(f.sum("articles_count").alias("articles_sum")).first()["articles_sum"]
        m = m / 2

        self.__first_phase__(m, current_graph)


'''
Pseudo-code :

    First Phase:

    current_partitions_df = [...]
    new_partitions_df = []

    for each node i:
        k_i = sum of weights of node i

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

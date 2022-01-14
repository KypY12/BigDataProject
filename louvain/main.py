from pyspark.sql import SparkSession
import pyspark.sql.functions as f

session = SparkSession \
    .builder \
    .appName("Louvain run") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()
# .config("spark.driver.memory", "4g") \

from algorithm.louvain import Louvain
from processing.preprocess import preprocess_data, write_coauthorship_graph, read_coauthorship_graph


def testing_first_phase(g):
    m = g.edges \
        .where(f.col("src") != f.col("dst")) \
        .select(f.sum("articles_count").alias("articles_sum")).first()["articles_sum"]

    m = m / 2

    communities = g.vertices.withColumn("community", f.monotonically_increasing_id())
    # communities.show()

    single_node_communities = communities.groupBy(f.col("community")) \
        .agg(f.count("community").alias("nodes_count")) \
        .where(f.col("nodes_count") == 1)

    # single_node_communities.show()

    single_node_communities_edges = single_node_communities \
        .join(communities.alias("com"), single_node_communities["community"] == communities["community"]) \
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

    # single_node_communities_edges.show()

    comm_aux_df = g.edges.where(f.col("src") != f.col("dst")) \
        .join(communities, g.edges["dst"] == communities["id"]) \
        .select(f.col("community").alias("community_dst"),
                f.col("src"),
                f.col("dst"),
                f.col("articles_count"))
    comm_aux_df = comm_aux_df \
        .join(communities, comm_aux_df["src"] == communities["id"]) \
        .select(f.col("community").alias("community_src"),
                *comm_aux_df.columns)

    comm_aux_df = comm_aux_df.unionByName(single_node_communities_edges)
    # comm_aux_df.show()

    k_i = comm_aux_df \
        .groupBy(f.col("src").alias("i")) \
        .agg(f.sum("articles_count").alias("k_i"))
    # k_i.show()

    k_i_C = comm_aux_df \
        .groupBy([f.col("src").alias("i"),
                  f.col("community_dst").alias("C")]) \
        .agg(f.sum("articles_count").alias("k_i_C"))
    # k_i_C.show()

    # [community_src, community_dst, src, dst, articles_count]

    # comm_aux_df.join(k_i_C, [comm_aux_df["src"] == k_i_C["i"],
    #                          (comm_aux_df["community_dst"] == k_i_C["C"]) |
    #                          (comm_aux_df["community_src"] == k_i_C["C"])]).show()
    #
    # comm_aux_df.join(k_i_C, [comm_aux_df["src"] == k_i_C["i"],
    #                          comm_aux_df["community_src"] == k_i_C["C"]]).show()

    # comm_aux_df.join(k_i_C, comm_aux_df["src"] == k_i_C["i"])\
    #     .where(f.col("community_src") == f.col("C")).show()

    sum_tot_C = comm_aux_df \
        .groupBy(f.col("community_src").alias("C")) \
        .agg(f.sum("articles_count").alias("sum_tot_C")).orderBy("sum_tot_C", ascending=False)
    # sum_tot_C.show()

    # =================================================================
    mt = comm_aux_df \
        .select(f.col("src"),
                f.col("community_src")) \
        .distinct() \
        .alias("mt") \
        .join(k_i.alias("k_i"),
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

    # | i | S_i | D_i | k_i | k_i_S | k_i_D | sum_tot_S | sum_tot_D |
    # mt.show()

    two_m = 2 * m
    two_m_sq = two_m ** 2

    mc = mt.withColumn("delta_Q",
                       (f.col("k_i_D") - f.col("k_i_S")) / two_m + f.col("k_i") *
                       (2 * (f.col("sum_tot_S") - f.col("sum_tot_D")) - f.col(
                           "k_i")) / two_m_sq)
    # mc.show()

    positive_max_mc = mc \
        .groupBy("i") \
        .agg(f.max("delta_Q").alias("max_delta_Q")) \
        .where(f.col("max_delta_Q") > 0)

    # positive_max_mc.show()

    positive_max_mc = positive_max_mc.alias("pmmc") \
        .join(mc.alias("mc"),
              [f.col("pmmc.i") == f.col("mc.i"),
               f.col("pmmc.max_delta_Q") == f.col("mc.delta_Q")]) \
        .select(f.col("pmmc.i"),
                f.col("mc.D_i"),
                f.col("pmmc.max_delta_Q")) \
        .dropDuplicates(["i", "max_delta_Q"])

    # positive_max_mc.show()

    updated_communities_df = communities.alias("comm") \
        .join(positive_max_mc.alias("pmmc"),
              f.col("comm.id") == f.col("pmmc.i"),
              how="left")

    # updated_communities_df.orderBy("id").show()

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
                       (f.col("ucd.community") < f.col("ucd.D_i")), None)
                .otherwise(f.col("ucd.D_i")).alias("D_i"),
                f.col("ucd.max_delta_Q"))

    # updated_communities_df.show(20)

    updated_communities_df = updated_communities_df \
        .select(f.col("id"),
                f.when(f.col("D_i").isNotNull(), f.col("D_i"))
                .otherwise(f.col("community")).alias("community")) \
        .orderBy("community")

    # updated_communities_df.show(210)

    print(communities.count())
    print(updated_communities_df.select("id").distinct().count())

    # node_k_i_C = k_i_C.where(f.col("i") == "Stroeer Alexander ")
    # node_k_i_C.show()
    #
    # elements = list(node_k_i_C.select('C').toPandas()['C'])
    # print(elements)
    #
    # sth = sum_tot_C.where(f.col("C").isin(elements))
    # sth.show()

    # k_i_S = node_k_i_C.where(f.col("C") == node_community).select("C").toPandas()

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


if __name__ == '__main__':
    session.sparkContext.setCheckpointDir("../data/louvain_checkpoints_dir")

    # sample_size = 500
    # metadata_df = session.read.json("../data/original/arxiv-metadata-oai-snapshot.json").limit(sample_size)
    # metadata_df = session.read.json("../data/original/arxiv-metadata-oai-snapshot.json").limit(1_000_000)
    # metadata_df = session.read.json("../data/original/arxiv-metadata-oai-snapshot.json")
    # metadata_df.show(20)
    # g = preprocess_data(metadata_df)

    # write_coauthorship_graph(g, "../data/authors_graph")

    g = read_coauthorship_graph(session, "../data")

    g.vertices.show()
    g.edges.show()

    louvain_alg = Louvain(g, session)

    louvain_alg.execute()

    # testing_first_phase(g)

    g.unpersist()

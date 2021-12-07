from pyspark.sql import SparkSession


# Fix to make it work on windows:
# - create a variable PYSPARK_SUBMIT_ARGS and add (without commas):
# "--packages graphframes:graphframes:0.8.2-spark3.2-s_2.12 pyspark-shell"
# OR
# import os
# os.environ["PYSPARK_SUBMIT_ARGS"] = (
#     "--packages graphframes:graphframes:0.8.2-spark3.2-s_2.12 pyspark-shell"
# )


if __name__ == '__main__':

    session = SparkSession \
        .builder \
        .appName("Testing") \
        .getOrCreate()

    try:
        import graphframes as gf
    except:
        print("error")

    # Create a Vertex DataFrame with unique ID column "id"
    v = session.createDataFrame([
        ("a", "Alice", 34),
        ("b", "Bob", 36),
        ("c", "Charlie", 30),
    ], ["id", "name", "age"])
    # Create an Edge DataFrame with "src" and "dst" columns
    e = session.createDataFrame([
        ("a", "b", "friend"),
        ("b", "c", "follow"),
        ("c", "b", "follow"),
    ], ["src", "dst", "relationship"])

    # Create a GraphFrame
    g = gf.GraphFrame(v, e)

    # Query: Get in-degree of each vertex.
    g.inDegrees.show()

    # Query: Count the number of "follow" connections in the graph.
    g.edges.filter("relationship = 'follow'").count()

    # Run PageRank algorithm, and show results.
    results = g.pageRank(resetProbability=0.01, maxIter=20)
    results.vertices.select("id", "pagerank").show()

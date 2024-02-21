from neo4j import GraphDatabase
from graphdatascience import GraphDataScience

URI = "bolt://localhost:7687"
AUTH = ("neo4j", "changeme")
DATABASE = "neo4j"

with GraphDatabase.driver(URI, auth=AUTH) as driver:
    try:
        gds = GraphDataScience(URI, auth=AUTH)
        gds.run_cypher(
        """
        MATCH (p1:Party)-[t:TRANSACTION]->(p2:Party)
            WHERE t.tran_timestamp >= datetime("2020-10-01")
                AND t.tran_timestamp < datetime("2020-11-01")
            RETURN p1, t, p2
        """
        )
        G, project_result = gds.graph.project("transaction_graph", "Party", {"TRANSACTION": {"properties": ["tran_id","tx_type","base_amt","tran_timestamp"]}})

        node2vec_result = gds.node2vec.write(
            G,                                #  Graph object
            featureProperties=["community"],  #  Configuration parameters
            embeddingDimension=10,
            walkLength=80,
            inOutFactor=1,
            returnFactor=1,
            relationshipWeightProperty="base_amt",
            writeProperty="node2vec"
        )
        assert node2vec_result["nodePropertiesWritten"] == G.node_count()

    except Exception as e:
        print(e)
        # further logging/processing



# MLOps job to update the Graph Embeddings Feature Group

# Import Libraries
import datetime
import neo4j
from neo4j import GraphDatabase
from graphdatascience import GraphDataScience
import pandas as pd
import numpy as np

URI = "neo4j+s://20916ac8.databases.neo4j.io"
AUTH = ("neo4j", "0FYrU2Ye5qnniYGCF9fd4zYeM-rBjqwkZQWTFPZOePQ")
DATABASE = "neo4j"

with GraphDatabase.driver(URI, auth=AUTH) as driver:
    with driver.session(database=DATABASE) as session:
        result = session.run("""
        Match (n) call {with n detach delete n } in transactions of 10000 rows
        """)
        print(result.consume().counters)

with GraphDatabase.driver(URI, auth=AUTH) as driver:
    with driver.session(database=DATABASE) as session:
            result = session.run("""
                load csv with headers from "https://repo.hops.works/master/hopsworks-tutorials/data/aml/party.csv" as parties
                create (p:Party)
                set p = parties
            """)
            # print(result.consume().counters)

with GraphDatabase.driver(URI, auth=AUTH) as driver:
    with driver.session(database=DATABASE) as session:
            result = session.run("""
                LOAD CSV WITH HEADERS FROM "https://repo.hops.works/master/hopsworks-tutorials/data/aml/transactions.csv" AS Transaction
                    MATCH (startNode:Party)
                    WHERE startNode.partyId = Transaction.src
                    CALL {
                        WITH Transaction, startNode
                        MATCH (endNode:Party)
                        WHERE endNode.partyId = Transaction.dst
                        CREATE (startNode)-[rel:TRANSACTION {tran_id: Transaction.tran_id, tx_type: Transaction.tx_type, base_amt: Transaction.base_amt, tran_timestamp: datetime(Transaction.tran_timestamp)}]->(endNode)
                    } IN TRANSACTIONS OF 2500 ROWS;
            """)
            # print(result.consume().counters)


transactions_df = pd.read_csv("https://repo.hops.works/master/hopsworks-tutorials/data/aml/transactions.csv", parse_dates = ['tran_timestamp'])
transactions_df.head(5)
alert_transactions = pd.read_csv("https://repo.hops.works/master/hopsworks-tutorials/data/aml/alert_transactions.csv")
alert_transactions.head()
party = pd.read_csv("https://repo.hops.works/master/hopsworks-tutorials/data/aml/party.csv")
# print("Datasets loaded!")

with GraphDatabase.driver(URI, auth=AUTH) as driver:
    try:
        gds = GraphDataScience(URI, auth=AUTH, database=DATABASE)

        # Determine months of transactions
        start_date = transactions_df['tran_timestamp'].min().to_pydatetime().replace(tzinfo=None)
        end_date = transactions_df['tran_timestamp'].max().to_pydatetime().replace(tzinfo=None)

        # For each month of transactions
        while start_date <= end_date:
            last_day_of_month = datetime.datetime(start_date.year, start_date.month, 1) + datetime.timedelta(days=32)
            end_date_of_month = last_day_of_month - datetime.timedelta(days=last_day_of_month.day)

            # Convert dates as milliseconds
            start = float(start_date.timestamp() / 10 ** 9)
            end = float(end_date_of_month.timestamp() / 10 ** 9)

            # Retrieve transactions within the month
            gds.run_cypher(
            f"MATCH (p1:Party)-[t:TRANSACTION]->(p2:Party) WHERE t.tran_timestamp >= {start} AND t.tran_timestamp < {end} RETURN p1, t, p2")

            # Create a temporary graph
            G, project_result = gds.graph.project("transaction_graph", "Party", "TRANSACTION")

            # Use the temporary graph to compute embeddings and save them as a property in the Neo4j database
            node2vec_result = gds.node2vec.write(
                G,                                #  Graph object
                embeddingDimension=16,
                walkLength=80,
                inOutFactor=1,
                returnFactor=1,
                writeProperty="node2vec"
            )

            # Increment to next month
            start_date = end_date_of_month + datetime.timedelta(days=1)

            # Remove the current monthly graph to generate a graph for the following month in the subsequent iteration
            gds.run_cypher(
                """
                CALL gds.graph.drop('transaction_graph') YIELD graphName
                """
            )

    except Exception as e:
        print(e)

#Connect to the Hopsworks feature store
import hopsworks
project = hopsworks.login()
fs = project.get_feature_store()

# Define Expectation Suite - no use of HSFS
import great_expectations as ge
# from pprint import pprint
import json

expectation_suite = ge.core.ExpectationSuite(expectation_suite_name="aml_project_validations")
# pprint(expectation_suite.to_json_dict(), indent=2)

expectation_suite.add_expectation(
  ge.core.ExpectationConfiguration(
  expectation_type="expect_column_max_to_be_between",
  kwargs={"column": "monthly_in_count", "min_value": 0, "max_value": 10000000}) 
)


# Connecting to Neo4j, getting the embeddings, and putting them into a data frame

with GraphDatabase.driver(URI, auth=AUTH) as driver:
    graph_embeddings_df = driver.execute_query(
        """MATCH (p:Party)-[t:TRANSACTION]->(:Party) 
            return 
            p.partyId as id, 
            p.node2vec as party_graph_embedding, 
            datetime(t.tran_timestamp).epochmillis as tran_timestamp""",
        database_=DATABASE,
        result_transformer_=neo4j.Result.to_df
    )
    
    # print(type(graph_embeddings_df))  # <class 'pandas.core.frame.DataFrame'>
    # print(graph_embeddings_df.head())

# Increase data size to store embeddings
from hsfs import engine
features = engine.get_instance().parse_schema_feature_group(graph_embeddings_df)
for f in features:
    if f.type == "array<double>" or f.type == "array<float>":
        f.online_type = "VARBINARY(20000)"

# Define Feature Group
graph_embeddings_fg = fs.get_or_create_feature_group(name="graph_embeddings",
                                       version=9,
                                       primary_key=["id"],
                                       description="node embeddings from transactions graph",
                                       event_time = 'tran_timestamp',     
                                       online_enabled=True,
                                       features=features,
                                       statistics_config={"enabled": False, "histograms": False, "correlations": False, "exact_uniqueness": False}
                                       )

# Insert data frame into Feature Group
graph_embeddings_fg.insert(graph_embeddings_df)
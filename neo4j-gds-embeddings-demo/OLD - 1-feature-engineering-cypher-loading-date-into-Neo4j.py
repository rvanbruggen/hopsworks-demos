# Importing Transaction data into Neo4j 
# (p1:Party)-[t:TRANSACTION]->(p2:Party)

from neo4j import GraphDatabase
from graphdatascience import GraphDataScience

URI = "bolt://localhost:7687"
AUTH = ("neo4j", "changeme")
DATABASE = "neo4j"

with GraphDatabase.driver(URI, auth=AUTH) as driver:
    try:
        driver.execute_query("create text index party_id_index for (p:Party) on (p.partyId)", database_=DATABASE)
        driver.execute_query("create text index party_type_index for (p:Party) on (p.partyType)", database_=DATABASE)
        driver.execute_query("create text index transaction_id_index for ()-[r:TRANSACTION]-() ON r.tran_id", database_=DATABASE)
        driver.execute_query("create range index transaction_timestamp_index for ()-[r:TRANSACTION]-() ON r.tran_timestamp", database_=DATABASE)


        with driver.session(database=DATABASE) as session:
            result = session.run("""
                load csv with headers from "https://repo.hops.works/master/hopsworks-tutorials/data/aml/party.csv" as parties
                create (p:Party)
                set p = parties
            """)
        print(result.consume().counters)

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
        print(result.consume().counters)

    except Exception as e:
        print(e)
        # further logging/processing



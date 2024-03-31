//CONSTRAINTS
CREATE CONSTRAINT party_id_constraint FOR (p:Party) REQUIRE p.partyId IS UNIQUE;
CREATE TEXT INDEX party_type_index FOR (p:Party) ON (p.partyType);
CREATE CONSTRAINT transaction_id_constraint FOR ()-[r:TRANSACTION]-() REQUIRE r.tran_id is UNIQUE;
CREATE TEXT INDEX transaction_timestamp_index FOR ()-[r:TRANSACTION]-() ON r.tran_timestamp;

//LOAD PARTIES
load csv with headers from "https://repo.hops.works/master/hopsworks-tutorials/data/aml/party.csv" as parties
    create (p:Party)
    set p = parties;

//LOAD TRANSACTIONS
:auto LOAD CSV WITH HEADERS FROM "https://repo.hops.works/master/hopsworks-tutorials/data/aml/transactions.csv" AS Transaction
    MATCH (startNode:Party)
        WHERE startNode.partyId = Transaction.src
        CALL {
            WITH Transaction, startNode
                MATCH (endNode:Party)
                WHERE endNode.partyId = Transaction.dst
                CREATE (startNode)-[rel:TRANSACTION {tran_id:  Transaction.tran_id, tx_type: Transaction.tx_type, base_amt: Transaction.base_amt, tran_timestamp: datetime(Transaction.tran_timestamp)}]->(endNode)
        } IN TRANSACTIONS OF 2500 ROWS;

//LOAD GDS GRAPH PROJECTION
MATCH (p1:Party)-[t:TRANSACTION]->(p2:Party) WHERE t.tran_timestamp >= datetime("2021-10-01") AND t.tran_timestamp < datetime("2021-11-01")
WITH gds.graph.project("transaction_graph", p1,p2) AS G
RETURN G.graphName as graph, G.nodeCount as nodes, G.relationshipCount as rels

//CALCULATE EMBEDDINGS
call gds.node2vec.write("transaction_graph",
    {embeddingDimension: 16,
        walkLength:80,
        inOutFactor:1,
        returnFactor:1,
        writeProperty:"node2vec"})


//DROP PROJECTION
CALL gds.graph.drop('transaction_graph') YIELD graphName 


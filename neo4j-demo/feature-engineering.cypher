// Check if we can access the transactions.csv file
load csv from "https://repo.hops.works/master/hopsworks-tutorials/data/aml/transactions.csv" as transactions
return transactions
limit 5;

// Check if we can access the alerts.csv file
load csv from "https://repo.hops.works/master/hopsworks-tutorials/data/aml/alert_transactions.csv" as alerts
return alerts
limit 5;

// Check if we can access the parties.csv file
load csv with headers from "https://repo.hops.works/master/hopsworks-tutorials/data/aml/party.csv" as parties
return parties
limit 5;

// Prepare Neo4j with appropriate indexes
create text index party_id_index for (p:Party) on (p.partyId);
create text index party_type_index for (p:Party) on (p.partyType);
create text index transaction_id_index for ()-[r:TRANSACTION]-() ON r.tran_id;

// Create Parties in Neo4j
load csv with headers from "https://repo.hops.works/master/hopsworks-tutorials/data/aml/party.csv" as parties
create (p:Party)
    set p = parties;


// Create Transactions in Neo4j
:auto load csv with headers from "https://repo.hops.works/master/hopsworks-tutorials/data/aml/transactions.csv" as transactions
call {
    WITH transactions
    MATCH (startNode:Party), (endNode:Party)
    WHERE startNode.partyId = transactions.src AND endNode.partyId = transactions.dst
    CREATE (startNode)-[rel:TRANSACTION {tran_id: transactions.tran_id, tx_type: transactions.tx_type, base_amt: transactions.base_amt, tran_timestamp: datetime(transactions.tran_timestamp)}]->(endNode)
} in transactions of 2500 rows;



// Alternative way to create transactions in Neo4j
:auto load csv with headers from "https://repo.hops.works/master/hopsworks-tutorials/data/aml/transactions.csv" as transactions
call {
    WITH transactions
    CREATE (t:Transaction)
    SET t = transactions
} in transactions of 25000 rows;

:auto MATCH (t:Transaction), (startNode:Party)
WHERE startNode.partyId = t.src
call {
    WITH t, startNode
    MATCH (endNode:Party)
    WHERE endNode.partyId = t.dst
    CREATE (startNode)-[rel:TRANSACTION {tran_id: t.tran_id, tx_type: t.tx_type, base_amt: t.base_amt, tran_timestamp: datetime(t.tran_timestamp)}]->(endNode)
} in transactions of 2500 rows;


// Alternative way to create transactions in Neo4j
:auto LOAD CSV WITH HEADERS FROM "https://repo.hops.works/master/hopsworks-tutorials/data/aml/transactions.csv" AS Transaction
    MATCH (startNode:Party)
        WHERE startNode.partyId = Transaction.src
        CALL {
            WITH Transaction, startNode
            MATCH (endNode:Party)
            WHERE endNode.partyId = Transaction.dst
            CREATE (startNode)-[rel:TRANSACTION {tran_id: Transaction.tran_id, tx_type: Transaction.tx_type, base_amt: Transaction.base_amt, tran_timestamp: datetime(Transaction.tran_timestamp)}]->(endNode)
        } IN TRANSACTIONS OF 2500 ROWS;







// Alternative way to create transactions in Neo4j
// :auto load csv with headers from "https://repo.hops.works/master/hopsworks-tutorials/data/aml/transactions.csv" as transactions
// call {
//     WITH transactions
//     MATCH (startNode:Party) WHERE startNode.partyId = transactions.src
//     WITH startNode, transactions
//         MATCH (endNode:Party) WHERE endNode.partyId = transactions.dst
//         CREATE (startNode)-[rel:TRANSACTION {tran_id: transactions.tran_id, tx_type: transactions.tx_type, base_amt: transactions.base_amt, tran_timestamp: datetime(transactions.tran_timestamp)}]->(endNode)
// } in transactions of 2500 rows;
// return "Success!";

// Slower way to add the transactions
// :auto MATCH (t:Transaction)
// call {
//     WITH t
//     MATCH (endNode:Party), (startNode:Party)
//     WHERE endNode.partyId = t.dst AND WHERE startNode.partyId = t.src
//     CREATE (startNode)-[rel:TRANSACTION {tran_id: t.tran_id, tx_type: t.tx_type, base_amt: t.base_amt, tran_timestamp: datetime(t.tran_timestamp)}]->(endNode)
// } in transactions of 2500 rows;


// Optional storage of Alerts on Transactions
:auto load csv with headers from "https://repo.hops.works/master/hopsworks-tutorials/data/aml/alert_transactions.csv" as alerts
call {
    with alerts
    match (a:Party)-[t:TRANSACTION]->(b:Party)
    where t.tran_id = alerts.tran_id
    set t.alert_id = alerts.alert_id
    set t.alert_type = alerts.alert_type
    set t.is_sar = alerts.is_sar
} in transactions of 25 rows;
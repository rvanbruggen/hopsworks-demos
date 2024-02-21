# Adding the Neo4j graph embeddings to the Hopsworks Feature Store as a feature group

# Doing all the imports
import datetime
import pandas as pd
import numpy as np
import neo4j
from neo4j import GraphDatabase
from graphdatascience import GraphDataScience
from tqdm import tqdm

import hopsworks
# Connecting to Hopsworks Serverless - need to choose project
project = hopsworks.login()
fs = project.get_feature_store()

# Setting up Neo4j connectivity
URI = "bolt://localhost:7687"
AUTH = ("neo4j", "changeme")
DATABASE = "neo4j"

# Connecting to Neo4j, getting the embeddings, and putting them into a dataframe
with GraphDatabase.driver(URI, auth=AUTH) as driver:
    graph_embeddings_df = driver.execute_query(
        "MATCH (p:Party)-[t:TRANSACTION]->(:Party) return p.partyId as party_id, p.partyType as party_type, p.node2vec as party_graph_embedding, t.tran_timestamp.epochmillis as timestamp",
        database_="neo4j",
        result_transformer_=neo4j.Result.to_df
    )
    print(type(graph_embeddings_df))  # <class 'pandas.core.frame.DataFrame'>

# Creating the features
from hsfs import engine
features = engine.get_instance().parse_schema_feature_group(graph_embeddings_df)

# Creating the feature group in Hopsworks Serverless App
graph_embeddings_fg = fs.get_or_create_feature_group(name="graph_embeddings",
                                       version=1,
                                       primary_key=["party_id"],
                                       description="node embeddings from transactions graph",
                                       event_time = 'timestamp',     
                                       online_enabled=True,
                                       features=features,
                                       statistics_config={"enabled": False, "histograms": False, "correlations": False, "exact_uniqueness": False}
                                       )
graph_embeddings_fg.insert(graph_embeddings_df)
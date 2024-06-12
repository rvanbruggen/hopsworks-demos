import os
import numpy as np
import pandas as pd
import hsfs
import joblib


class Predict(object):

    def __init__(self):
        """ Initializes the serving state, reads a trained model"""        
        # get feature store handle
        fs_conn = hsfs.connection()
        self.fs = fs_conn.get_feature_store()
        
        # get feature view
        self.fv = self.fs.get_feature_view("beervolume_fv", 1)
        
        # initialize serving
        self.fv.init_serving(1)

        # load the trained model
        self.model = joblib.load(os.environ["ARTIFACT_FILES_PATH"] + "/xgboost_beervolume_model.pkl")
        print("Initialization of Beervolume model Complete")

    
    def predict(self, id_value):
        """ Serves a Beervolume prediction request usign a trained model"""
        # Retrieve feature vectors
        feature_vector = self.fv.get_feature_vector(
            entry = {'id': id_value[0]}
        )
        return self.model.predict(np.asarray(feature_vector[1:]).reshape(1, -1)).tolist()

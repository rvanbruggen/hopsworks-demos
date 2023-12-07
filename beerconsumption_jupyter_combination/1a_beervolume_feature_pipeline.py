import hopsworks
from features.beervolume import generate_today
import warnings
warnings.filterwarnings('ignore')

# Generate beervolume data form today
generated_data_today = generate_today()

# Connect to the Feature Store
project = hopsworks.login()
fs = project.get_feature_store() 

# Retrieve Beervolume Feature Group
beervolume_fg = fs.get_feature_group(
    name='beervolume',
    version=1,
)
# Insert generated data for today into Beervolume Feature Group
beervolume_fg.insert(generated_data_today)

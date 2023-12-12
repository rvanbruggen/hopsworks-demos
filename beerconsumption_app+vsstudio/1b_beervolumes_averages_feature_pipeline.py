from datetime import datetime, timedelta
from averages import calculate_second_order_features
import hopsworks
import warnings
warnings.filterwarnings('ignore')

# Connect to the Feature Store
project = hopsworks.login()
fs = project.get_feature_store() 

# Retrieve Averages Feature Group
beervolume_averages_fg = fs.get_feature_group(
    name='beervolume_averages',
    version=1,
)
# Retrieve Beer Volume Feature Group
beervolume_fg = fs.get_feature_group(
    name='beervolume',
    version=1,
)
# Get today's date
today = datetime.today()

# Calculate the date 30 days ago
thirty_days_ago = (today - timedelta(days=31)).strftime("%Y-%m-%d")

# Read price data for 30 days ago
month_price_data = beervolume_fg.filter(beervolume_fg.date >= thirty_days_ago).read()

# Calculate second order features
beervolume_averages_df = calculate_second_order_features(month_beervolume_data)

# Get calculated second order features only for today
averages_today = beervolume_averages_df[beervolume_averages_df.date == today.strftime("%Y-%m-%d")]

# Insert second order features for today into Averages Feature Group
beervolume_averages_fg.insert(averages_today)

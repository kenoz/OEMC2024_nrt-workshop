import json
import datetime

import pandas as pd
import numpy as np
import rioxarray
from rasterio import features
import fiona

from nrt import data


# Import interpretation result as pandas dataframe
# Convert dataframe of temporal segments to dataframe of disturbance date/no-disturbance
# Join to feature collection
# Rasterize feature collection

class Segment:
    def __init__(self, begin, end, label):
        self.begin = begin
        self.end = end
        self.label = label

    # Define __lt__ to allow sorting by the 'begin' field
    def __lt__(self, other):
        return self.begin < other.begin

    def __repr__(self):
        return f"Segment(begin={self.begin}, end={self.end}, label='{self.label}')"



def analyze_segmentation(segments):
    was_forest = any(segment.label == 'Stable tree cover' for segment in segments)
    is_stable = True
    disturbance_date = None
    stable_found = False
    for i, segment in enumerate(segments[:-1]):
        if segment.label == 'Stable tree cover':
            stable_found = True
        if stable_found and segments[i + 1].label in ['Non-treed', 'Dieback']:
            is_stable = False
            disturbance_date = segments[i + 1].begin
            break
    return {
        'forest': was_forest,
        'is_stable': is_stable,
        'disturbance_date': disturbance_date
    }

# Group dataframe by feature_id and analyze segments for each group
def process_feature_group(group):
    # Create list of Segment instances
    segments = [Segment(row['begin'], row['end'], row['label']) for _, row in group.iterrows()]
    # Sort the segments by 'begin' date
    sorted_segments = sorted(segments)
    # Analyze stability and disturbance
    return analyze_segmentation(sorted_segments)




df = data.germany_temporal_segments()
fc, meta = data.germany_sample_points(return_meta=True)
stability_df = df.groupby('feature_id').apply(process_feature_group).apply(pd.Series).reset_index()

print(stability_df)

# TODO: Old school way, this won't work anymore in fiona>=2.0 as features will become immutable
for feature in fc:
    idx = feature['properties']['fid']
    # Filter the dataframe to get the row with the matching feature_id
    stability_row = stability_df[stability_df['feature_id'] == idx]
    # If a match is found, update the properties with the disturbance_date
    if not stability_row.empty:
        feature['properties']['forest'] = stability_row['forest'].values[0].item()
        disturbance_date = stability_row['disturbance_date'].values[0]
        # Convert NaN to 0 on the fly
        feature['properties']['disturbance_date'] = 0 if np.isnan(disturbance_date) else disturbance_date

fc_ = filter(lambda x: x['properties']['forest'], fc)
# fc__ = filter(lambda x: x['properties']['disturbance_date'] > (datetime.datetime(2020,1,1) - datetime.datetime(1970,1,1)).days, fc_)

"""
# Rasterize to match germany_zarr
cube = data.germany_zarr()
y_true_2d = features.rasterize(shapes=[(feat['geometry'], feat['properties']['disturbance_date']) for feat in fc_],
                               out_shape=cube.rio.shape,
                               fill=-1,
                               transform=cube.rio.transform(),
                               dtype=np.int16)
print(y_true_2d)
mask = np.where(y_true_2d == -1, 0, 1)
print(mask)
"""

meta['schema']['properties'].update(forest='bool', disturbance_date='int')
with fiona.open('/home/loic/git/nrt-workshop/data/fc.fgb', 'w', **meta) as con:
    con.writerecords(fc_)

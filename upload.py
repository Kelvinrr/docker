import os
import tarfile
import glob

import plio
import libpysat
import pandas as pd
import geopandas as gpd
import numpy as np

from sqlalchemy import create_engine
from geoalchemy2 import Geometry, WKTElement
from shapely.geometry import Point

engine = create_engine('postgresql://postgres@localhost:55432')
connection = engine.connect()

files = glob.glob('*.sl2')
for f in files:
    tar = tarfile.open(f)
    # Extract the spc and the ctg files
    list(map(tar.extract, [m for m in tar.getmembers() if '.spc' in m.name]))
files = glob.glob('*.spc')

columns = ['INCIDENCE_ANGLE', 'EMISSION_ANGLE', 'CENTER_LONGITUDE', 'CENTER_LATITUDE', 'PRODUCT_ID']

data_path = '/Users/krodriguez-pr/repos/kaguya_spectral_profiler/sampledata'
files = glob.glob(os.path.join(data_path,'*.spc'))

print('uploading {} files'.format(len(files)))

ref1_dict = {}
ref2_dict = {}
d= {}
for f in files:
    s = libpysat.data.spectra.Spectra.from_file(f)
    meta = s.xs('REF1', level=1, axis=1).loc[columns]
    ref1s = s.xs('REF1', level=1, axis=1).data
    ref2s = s.xs('REF2', level=1, axis=1).data

    # continuum correct here
    res1, denom = ref1s.continuum_correct(nodes=[512.6, 1547.7, 2404.2],correction_nodes=[512.6, 1547.7, 2587.9])
    res2, denom = ref2s.continuum_correct(nodes=[512.6, 1547.7, 2404.2],correction_nodes=[512.6, 1547.7, 2587.9])

    ref1_dict[s.loc['PRODUCT_ID'].iloc[0]] = res1
    ref2_dict[s.loc['PRODUCT_ID'].iloc[0]] = res2
    d[s.loc['PRODUCT_ID'].iloc[0]] = meta

# populate filelookups
product_ids = str(set(d.keys())).replace('\'','')
fileid_lookup = pd.read_sql('select file_id, product_id from filemetadata'.format(), connection).to_dict(orient='list')
fileid_lookup = dict(zip(fileid_lookup['product_id'], fileid_lookup['file_id']))

# populate filenamedata
records = []
for key in d.keys():
    # Add file ID to table
    if key in fileid_lookup.keys():
        print('{} is a duplicate'.format(key))
        continue
    file_record = {
        'filepath' : os.path.join(data_path, key),
        'product_id' : key
    }
    records.append(file_record)

pd.DataFrame.from_dict(records).to_sql('filemetadata', engine, if_exists='append', index=False)

# populate filelookups
product_ids = str(set(d.keys())).replace('\'','')
fileid_lookup = pd.read_sql('select file_id, product_id from filemetadata', connection).to_dict(orient='list')
fileid_lookup = dict(zip(fileid_lookup['product_id'], fileid_lookup['file_id']))

print('Uploading filelookups')
i=0
# convert to file_ids for simplicity
for key in d.keys():
    print('Uploading {} out of {}'.format(i, len(d.keys())))

    original_df = d[key]
    df = gpd.GeoDataFrame()
    df['file_id'] = pd.Series([fileid_lookup[key]]*original_df.shape[1])
    df['observation_id'] = original_df.columns
    df['emission'] = original_df.loc['EMISSION_ANGLE']
    df['incidence'] = original_df.loc['INCIDENCE_ANGLE']

    # we want lon, lat
    def to_point(rec):
        lon = rec['CENTER_LONGITUDE']
        lat = rec['CENTER_LATITUDE']
        rec['location'] = WKTElement(Point(lon, lat).wkt, srid=930100)
        return rec

    original_df = original_df.apply(to_point, axis=0)
    df['location'] = original_df.loc['location']
    try:
        df.to_sql('filelookups', engine, if_exists='append', index=False, dtype={'location': Geometry('POINT', srid=930100)})
    except:
        pass

print('Uploading REF1')
i = 0
for key in ref1_dict.keys():
    print('Uploading {} out of {}'.format(i, len(d.keys())))
    original_df = ref1_dict[key].T
    df = pd.DataFrame()
    df['file_id'] = pd.Series([fileid_lookup[key]]*original_df.shape[0])
    df['observation_id'] = original_df.index
    df['spectra'] = pd.Series(np.asarray(original_df).tolist())
    df.to_sql('ref1', engine, if_exists='append', index=False)


print('Uploading REF2')
i = 0
for key in ref2_dict.keys():
    print('Uploading {} out of {}'.format(i, len(d.keys())))
    original_df = ref1_dict[key].T
    df = pd.DataFrame()
    df['file_id'] = pd.Series([fileid_lookup[key]]*original_df.shape[0])
    df['observation_id'] = original_df.index
    df['spectra'] = pd.Series(np.asarray(original_df).tolist())
    df.to_sql('ref2', engine, if_exists='append', index=False)

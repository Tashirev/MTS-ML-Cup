import sys
import os
import warnings
os.environ['OPENBLAS_NUM_THREADS'] = '1'
warnings.filterwarnings('ignore')


import cudf
from tqdm import tqdm

print('start read csv')
data_urls_description = cudf.read_csv('urls_2_domains_description.csv')[['url','description']]
data_urls_description = data_urls_description.set_index('url')['description']

print(data_urls_description)
print('start read pqt')
data_url_only = cudf.read_parquet(f'data_url_only_XX_2_domains.pqt', engine='pyarrow')

print(data_url_only)
print('start replace')


for column in tqdm(data_url_only.columns[1:]):  # проход по всем колонкам в data_url_only

    data_url_only[column] = data_url_only[column].map(data_urls_description, na_action='ignore')


data_url_only.to_parquet('data_description_only_XX_2_domains.pqt')
print('THE END')

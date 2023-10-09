import pandas as pd
import numpy as np

root_dir = '/home/timur/Загрузки'

file_path_from_db = f'{root_dir}/select _ from reference_inn wher (3).xlsx'
file_path_from_marketing = f'{root_dir}/Test_inn.xlsx'

file_from_db = pd.read_excel(file_path_from_db)
file_from_marketing = pd.read_excel(file_path_from_marketing)

np_difference = np.setdiff1d(file_from_marketing['company_name'], file_from_db['company_name'])
df_difference = pd.DataFrame(data=np_difference)
df_difference.to_csv(f'{root_dir}/difference.csv', index=False)
import pandas as pd
import numpy as np

root_dir = '/home/timur/sambashare/reference/reference_inn_by_api/done'

file_path_from_db = f'/home/timur/Загрузки/select _ from reference_inn wher (2).xlsx'
file_path_from_marketing = f'{root_dir}/ИНН_all_enr_07_2023_3382 строк_2_12.09.23.xlsx'

file_from_db = pd.read_excel(file_path_from_db)
file_from_marketing = pd.read_excel(file_path_from_marketing)

np_difference = np.setdiff1d(file_from_marketing['company_name'], file_from_db['company_name'])
df_difference = pd.DataFrame(data=np_difference)
df_difference.to_csv(f'{root_dir}/difference.csv', index=False)
import pandas as pd

root_dir = '/home/timur/Загрузки'
file_path_from_db = f'{root_dir}/select company_name, company_inn(1).xlsx'
file_path_from_marketing = f'{root_dir}/reference_inn_all_production.xlsx'

file_from_db = pd.read_excel(file_path_from_db).sort_values(by=['company_name'], ignore_index=True)
file_from_marketing = pd.read_excel(file_path_from_marketing).sort_values(by=['company_name'], ignore_index=True)

df_difference = pd.DataFrame()

index_db: int = 0
for index, row in file_from_marketing.iterrows():
    value: str = file_from_db.loc[index_db, "company_name"]
    if row["company_name"] == value:
        index_db += 1
    else:
        df_difference = df_difference.append(row, ignore_index=True)
        # df_difference = pd.concat([df_difference, row], ignore_index=True)

df_difference.to_excel(f'{root_dir}/difference.xlsx', index=False)

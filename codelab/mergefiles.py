import os
import pandas as pd

#path = '/opt/airflow/localfiles'
path = '../locafiles/'
if os.path.isdir(path):
    os.chdir(path)
else:
    os.mkdir(path)
    os.chdir(path)

print(path)

file_list = [path + f for f in os.listdir(path) if f.startswith('resultados')]
print(file_list)

csv_list = []
for file in sorted(file_list):
    csv_list.append(pd.read_csv(file).assign(file_name = os.path.basename(file)))
    print(csv_list)

csv_merged = pd.concat(csv_list, ignore_index=True)
print(csv_merged)

csv_merged.to_csv(path + 'Resultados_Consolidados.csv', index=False)
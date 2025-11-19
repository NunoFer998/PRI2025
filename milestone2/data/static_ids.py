import pandas as pd
import hashlib

def generate_unique_id(row):
    unique_string = (
        str(row['name']) + 
        str(row['symptoms']) + 
        str(row['description']) + 
        str(row['treatments'])
    )
    return hashlib.md5(unique_string.encode('utf-8')).hexdigest()

filename = 'merged_disease_symptom_list.csv'  
df = pd.read_csv(filename)

df['id'] = df.apply(generate_unique_id, axis=1)

cols = list(df.columns)
cols.insert(0, cols.pop(cols.index('id')))
df = df[cols]

output_filename = 'dataset_static_ids.csv'
df.to_csv(output_filename, index=False)

print(f"Created '{output_filename}' with static IDs.")
print(f"First row example:\n{df.iloc[0]}")
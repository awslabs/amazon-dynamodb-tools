import pandas as pd

# Assuming the JSON data is stored in a file called 'persons.json'
df = pd.read_json('persons.json')
df.to_parquet('persons.parquet')

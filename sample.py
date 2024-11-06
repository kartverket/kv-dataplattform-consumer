from typing import List
import pandas as pd
from deltalake import write_deltalake
from kv_dataplatform_consumer.crypto_utils import generate_symmetric_key, symmetric_encrypt_data

def write_table(path: str, data: dict, cols_to_encrypt: List[str]) -> bytes:
    key = generate_symmetric_key()

    # "Initial dump"
    df = pd.DataFrame(data)

    delta_table_path = f"./{path}"
    for col in cols_to_encrypt:
        print(col)
        encrypted_cities = df[col].apply(lambda col_data: symmetric_encrypt_data(col_data, key))
        df[f'{col}_enc'] = encrypted_cities.apply(lambda x: x[0])
        df[f'{col}_nonce'] = encrypted_cities.apply(lambda x: x[1])

        df = df.drop(columns=[col])

    write_deltalake(delta_table_path, df, mode='overwrite')
    print(f"Delta table written to {delta_table_path}")

    return key
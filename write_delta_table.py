from typing import Dict, List
import pandas as pd
from deltalake import write_deltalake
from kv_dataplatform_consumer.crypto_utils import symmetric_encrypt_data
import uuid

def write_table(path: str, data: dict, key_id: str, key: bytes, cols_to_encrypt: List[str]) -> dict:
    # "Initial dump"
    df = pd.DataFrame(data)
    df["key_id"] = df[cols_to_encrypt[0]].apply(lambda _: key_id)

    nonces: Dict[bytes, bool] = { }

    delta_table_path = f"./{path}"
    for col in cols_to_encrypt:
        def handle_nonce(x: bytes) -> bytes:
            if nonces.get(x, False) is True:
                raise ValueError("SECURITY ERROR: Should not have equal nonces")
            nonces[x] = True
            return x

        encrypted_cities = df[col].apply(lambda col_data: symmetric_encrypt_data(col_data, key))
        df[f'{col}_enc'] = encrypted_cities.apply(lambda x: x[0])
        df[f'{col}_nonce'] = encrypted_cities.apply(lambda x: handle_nonce(x[1]))
        df = df.drop(columns=[col])
    

    write_deltalake(delta_table_path, df, mode='overwrite')
    print(f"Delta table written to {delta_table_path}")

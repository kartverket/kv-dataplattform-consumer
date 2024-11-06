import logging
import pandas as pd
from kv_dataplatform_consumer.crypto_utils import symmetric_decrypt_data, generate_public_private_key, asymmetric_decrypt_symmetric_key

ENC_SUFFIX = "_enc"
NONCE_SUFFIX = "_nonce"

def consume_pii_table(df_table: pd.DataFrame, key_enc: bytes, asymmetric_private_key: bytes) -> pd.DataFrame:
    key = asymmetric_decrypt_symmetric_key(key_enc, rsa_private_key=asymmetric_private_key)
    encrypted_columns = [col for col in df_table.columns if col.endswith(ENC_SUFFIX)]

    for enc_col in encrypted_columns:
        logging.info(enc_col)
        col_name = enc_col[:-len(ENC_SUFFIX)]
        nonce_col = col_name + NONCE_SUFFIX

        if nonce_col in df_table.columns:
            df_table[col_name] = df_table.apply(
                lambda row: symmetric_decrypt_data(row[enc_col], key, row[nonce_col]),
                axis=1
            )
            
            logging.info(f"Decrypted column '{col_name}': {df_table[col_name].tolist()}")
            df_table.drop(columns=[enc_col, nonce_col], inplace=True)
    
    return df_table

def consume_table_from_share(share_key_path: str, share_identifier: str, schema: str, table: str, asymmetric_private_key: str) -> pd.DataFrame:
    pass

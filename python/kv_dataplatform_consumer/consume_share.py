import logging
import pandas as pd
from kv_dataplatform_consumer.crypto_utils import symmetric_decrypt_data, asymmetric_decrypt_symmetric_key
import delta_sharing
import base64

ENC_SUFFIX = "_enc"
NONCE_SUFFIX = "_nonce"

def consume_pii_table(df_table: pd.DataFrame, df_key_table: pd.DataFrame, asymmetric_private_key: bytes) -> pd.DataFrame:
    df_table["key_id"] = df_table["key_id"].astype(str)
    df_key_table["key_id"] = df_key_table["key_id"].astype(str)
    joined_df = df_table.join(df_key_table.set_index("key_id"), on="key_id", how="left")
    key_enc = joined_df["key"].to_list()[0]
    key_dec = asymmetric_decrypt_symmetric_key(key_enc, asymmetric_private_key)

    encrypted_columns = [col for col in df_table.columns if col.endswith(ENC_SUFFIX)]

    for enc_col in encrypted_columns:
        col_name = enc_col[:-len(ENC_SUFFIX)]
        nonce_col = col_name + NONCE_SUFFIX

        if nonce_col in joined_df.columns:
            joined_df[col_name] = joined_df.apply(
                lambda row: symmetric_decrypt_data(row[enc_col], key_dec, row[nonce_col]),
                axis=1
            )
            
            logging.info(f"Decrypted column '{col_name}': {joined_df[col_name].tolist()}")
            joined_df.drop(columns=[enc_col, nonce_col], inplace=True)
    joined_df.drop(columns=["key_id", "key"], inplace=True)
    return joined_df

def consume_table_from_share(share_key_path: str, share_name: str, schema: str, table: str, asymmetric_private_key: bytes) -> pd.DataFrame:
    table_url = f"{share_key_path}#{share_name}.{schema}.{table}"
    table_url_keys = f"{share_key_path}#{share_name}.__keys__{schema}.{table}"

    df_table = delta_sharing.load_as_pandas(table_url)
    df_table_keys = delta_sharing.load_as_pandas(table_url_keys)
    df_table_keys["key"] = df_table_keys["key"].apply(lambda key_enc: base64.standard_b64decode(key_enc))
    
    return consume_pii_table(df_table, df_table_keys, asymmetric_private_key)

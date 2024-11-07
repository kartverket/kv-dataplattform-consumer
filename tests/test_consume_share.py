import uuid
from deltalake import DeltaTable
from kv_dataplatform_consumer.consume_share import consume_pii_table
from kv_dataplatform_consumer.crypto_utils import generate_public_private_key, asymmetric_encrypt_symmetric_key, generate_symmetric_key
import pandas as pd
import logging

from write_delta_table import write_table

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')

data = {
    "id": [1, 2, 3, 4],
    "name": ["Alice", "Bob", "Charlie", "David"],
    "age": [24, 27, 22, 32],
    "city": ["New York", "San Francisco", "Chicago", "Austin"]
}

def test_that_encrypted_then_decrypted_data_from_consume_pii_table_returns_original_column_for_a_recipient():
    # Arrange
    recipient_keys = generate_public_private_key()
    path = "./sample_delta_table"
    keys = { }
    key_id = str(uuid.uuid4())
    logging.info(key_id)
    key = generate_symmetric_key()
    keys[key_id] = key
    write_table(path, data, key_id, key, ["city"])

    # Would write keys_df to a table in a keys schema, named the same as the table
    keys_df = pd.DataFrame([(key_id, asymmetric_encrypt_symmetric_key(key, recipient_keys["public_key"])) for (key_id, key) in keys.items()], columns=["key_id", "key"])
    table_df = DeltaTable(path).to_pandas()

    # Act
    decrypted_df = consume_pii_table(table_df, keys_df, recipient_keys["private_key"])

    # Assert
    elems = decrypted_df["city"].tolist()
    assert all(x == y for x,y in zip(elems, data["city"]))

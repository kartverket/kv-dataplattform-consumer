import uuid
from deltalake import DeltaTable
from kv_dataplatform_consumer.consume_share import (
    consume_pii_table,
    consume_table_from_share,
)
from kv_dataplatform_consumer.crypto_utils import (
    generate_public_private_key,
    asymmetric_encrypt_symmetric_key,
    generate_symmetric_key,
)
import pandas as pd
import logging
from delta_sharing import delta_sharing
import base64

from write_delta_table import write_table

logging.basicConfig(level=logging.DEBUG, format="%(levelname)s: %(message)s")

data = {
    "id": [1, 2, 3, 4],
    "name": ["Alice", "Bob", "Charlie", "David"],
    "age": [24, 27, 22, 32],
    "city": ["New York", "San Francisco", "Chicago", "Austin"],
}

share_private_key = bytes(
    "-----BEGIN RSA PRIVATE KEY-----\n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\n-----END RSA PRIVATE KEY-----",
    "utf-8",
)


def test_that_encrypted_then_decrypted_data_from_consume_pii_table_returns_original_column_for_a_recipient():
    # Arrange
    recipient_keys = generate_public_private_key()
    path = "./sample_delta_table"
    keys = {}
    key_id = str(uuid.uuid4())
    logging.info(key_id)
    key = generate_symmetric_key()
    keys[key_id] = key
    write_table(path, data, key_id, key, ["city"])

    # Would write keys_df to a table inside the schema __keys__<schema-name>, named the same as the encrypted table
    keys_df = pd.DataFrame(
        [
            (
                key_id,
                asymmetric_encrypt_symmetric_key(key, recipient_keys["public_key"]),
            )
            for (key_id, key) in keys.items()
        ],
        columns=["key_id", "key"],
    )
    table_df = DeltaTable(path).to_pandas()

    # Act
    decrypted_df = consume_pii_table(table_df, keys_df, recipient_keys["private_key"])

    # Assert
    elems = decrypted_df["city"].tolist()
    assert all(x == y for x, y in zip(elems, data["city"]))


def test_that_table_from_a_share_returns_valid_data_given_a_private_key():
    # Arrange
    share_key_path = "./config.share"
    client = delta_sharing.SharingClient(share_key_path)
    tables = client.list_all_tables()
    first_nonkey_table = list(
        filter(
            lambda table: not (
                "information_schema" in table.name or table.name.startswith("__keys__")
            ),
            tables,
        )
    )[0]

    # Act
    decrypted_df = consume_table_from_share(
        share_key_path,
        first_nonkey_table.share,
        first_nonkey_table.schema,
        first_nonkey_table.name,
        share_private_key,
    )
    logging.info("Decrypted DF:")
    logging.info(decrypted_df.to_string())

    # Assert
    elems = decrypted_df["city"].tolist()
    assert all(x == y for x, y in zip(elems, data["city"]))

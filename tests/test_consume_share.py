from deltalake import DeltaTable
from kv_dataplatform_consumer.consume_share import consume_pii_table
import logging

from sample import write_table

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')

data = {
    "id": [1, 2, 3, 4],
    "name": ["Alice", "Bob", "Charlie", "David"],
    "age": [24, 27, 22, 32],
    "city": ["New York", "San Francisco", "Chicago", "Austin"]
}

def test_that_decrypted_data_returns_original_column():
    path = "./sample_delta_table"
    key = write_table(path, data, ["city"])
    table_df = DeltaTable(path).to_pandas()
    logging.info(table_df.to_string())
    decrypted_df = consume_pii_table(table_df, key)

    elems = decrypted_df["city"].tolist()

    assert all(x == y for x,y in zip(elems, data["city"]))


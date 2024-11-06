from kv_dataplatform_consumer.crypto_utils import generate_symmetric_key, symmetric_encrypt_data, symmetric_decrypt_data
from kv_dataplatform_consumer.crypto_utils import generate_public_private_key, asymmetric_encrypt_symmetric_key, asymmetric_decrypt_symmetric_key
import logging

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')

def test_that_encrypted_then_decrypted_data_returns_original_symmetric():  
    sample_data = "12345678901"
    aes_key = generate_symmetric_key()

    (sample_data_enc, nonce) = symmetric_encrypt_data(sample_data, aes_key)
    sample_data_dec = symmetric_decrypt_data(sample_data_enc, aes_key, nonce)

    assert sample_data == sample_data_dec

def test_that_encrypted_then_decrypted_data_returns_original_asymmetric():
    rsa_key_pair = generate_public_private_key()
    aes_key_data = generate_symmetric_key()

    aes_key_enc = asymmetric_encrypt_symmetric_key(aes_key_data, rsa_key_pair["public_key"])
    sample_data_dec = asymmetric_decrypt_symmetric_key(aes_key_enc, rsa_key_pair["private_key"])

    assert aes_key_data == sample_data_dec

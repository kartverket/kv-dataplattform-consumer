from typing import Tuple

# AES (ChaCha20-Poly1305, AEAD)
from Crypto.Cipher import ChaCha20_Poly1305
from Crypto.Random import get_random_bytes
from Crypto.Util.Padding import pad, unpad

# RSA
from Crypto.Cipher import PKCS1_OAEP
from Crypto.PublicKey import RSA
from base64 import b64encode, b64decode

AES_DEFAULT_BYTES_KEY_SIZE_BYTES = 32 # Note: Mandatory for AEAD mode
AES_DEFAULT_NONCE_SIZE_BYTES = 12 # Note: Mandatory for AEAD mode

################################################
###  SYMMETRIC ENCRYPTION / DECRYPTION AES   ###
################################################
"""
🚨 Do not use ECB, unless you only encrypt one block
💁 One can use CTR mode, but does not guarantee integrity of data
✅ Prefer to use ChaCha20-Poly1305 (AEAD) mode, to give encryption and integrity/authentication of ciphertext in one algorithm

Generalte note: 
- When generating IV/nonce, it MUST be unique every single time, when using a key over multiple inputs. Could be catastrophic implications if not. 
- Data also need to be less than (2^31) - 1 bytes, to avoid overflow.
- Use UTF-8
"""
def generate_symmetric_key() -> bytes:
    aes_key = get_random_bytes(AES_DEFAULT_BYTES_KEY_SIZE_BYTES)
    return aes_key

def symmetric_encrypt_data(raw_data: str, aes_key: bytes) -> Tuple[str, str]:
    nonce_bytes = get_random_bytes(AES_DEFAULT_NONCE_SIZE_BYTES)
    raw = pad(raw_data.encode(), AES_DEFAULT_BYTES_KEY_SIZE_BYTES)
    cipher = ChaCha20_Poly1305.new(key=aes_key, nonce=nonce_bytes)
    ciphertext_bytes, _ = cipher.encrypt_and_digest(raw)
    
    ciphertext_str = b64encode(ciphertext_bytes).decode('utf-8')
    nonce_str = b64encode(nonce_bytes).decode('utf-8')
    return ciphertext_str, nonce_str

def symmetric_decrypt_data(enc: str, aes_key: bytes, aes_nonce: str) -> str:
    enc = b64decode(enc)
    nonce = b64decode(aes_nonce)
    cipher = ChaCha20_Poly1305.new(key=aes_key, nonce=nonce)
    decrypted_data = unpad(cipher.decrypt(enc), AES_DEFAULT_BYTES_KEY_SIZE_BYTES).decode('utf-8')
    return decrypted_data


#################################################
### -- ASYMMETRIC ENCRYPTION / DECRYPTION RSA ###
#################################################
def asymmetric_encrypt_symmetric_key(symmetric_key: str, rsa_public_key: str):
    key = RSA.importKey(rsa_public_key)
    cipher = PKCS1_OAEP.new(key)
    ciphertext = cipher.encrypt(symmetric_key)
    return ciphertext

def asymmetric_decrypt_symmetric_key(symmetric_key_enc: str, rsa_private_key: str):
    key = RSA.importKey(rsa_private_key)
    cipher = PKCS1_OAEP.new(key)
    message = cipher.decrypt(symmetric_key_enc)
    return message

def generate_public_private_key(): 
    # Should use key of size >= 4096 bits (Should be safe until year ~2048)
    # The pros (security) of a bigger key outweighs the cons (overhead of key generation)
    generated_key = RSA.generate(4096)

    private_key_pem = generated_key.export_key()
    public_key_pem = generated_key.publickey().export_key()

    private_key_str = private_key_pem.decode('utf-8')
    public_key_str = public_key_pem.decode('utf-8')

    return {
        "public_key": public_key_str,
        "private_key": private_key_str
    }

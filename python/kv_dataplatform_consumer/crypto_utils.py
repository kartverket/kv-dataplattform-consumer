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
    cipher = ChaCha20_Poly1305.new(key=aes_key, nonce=nonce_bytes)
    
    ciphertext, tag = cipher.encrypt_and_digest(raw_data.encode("utf-8"))

    # Java stores ciphertext+tag together
    ciphertext_with_tag = ciphertext + tag
    
    ciphertext_b64 = b64encode(ciphertext_with_tag).decode("utf-8")
    nonce_b64 = b64encode(nonce_bytes).decode("utf-8")
    
    return ciphertext_b64, nonce_b64

def symmetric_decrypt_data(ciphertext_b64: str, aes_key: bytes, nonce_b64: str) -> str:
    data = b64decode(ciphertext_b64)
    nonce = b64decode(nonce_b64)

    # Split out the last 16 bytes for tag (ChaCha20-Poly1305 tag size)
    ciphertext, tag = data[:-16], data[-16:]

    cipher = ChaCha20_Poly1305.new(key=aes_key, nonce=nonce)
    plaintext = cipher.decrypt_and_verify(ciphertext, tag)
    
    return plaintext.decode("utf-8")



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

package no.kartverket.kv;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.crypto.spec.IvParameterSpec;
import java.security.*;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;

/**
 * A utility class providing symmetric encryption/decryption functionality
 * using ChaCha20-Poly1305.
 */
public class EncryptionUtils {

    // ChaCha20-Poly1305 parameters
    private static final int NONCE_SIZE = 12; // 12-byte nonce for ChaCha20-Poly1305
    private static final int KEY_SIZE_BYTES = 32; // 256-bit key

    /**
     * Create a SecretKey object from the given raw bytes.
     *
     * @param keyBytes a 32-byte (256-bit) array for ChaCha20
     * @return a SecretKey suitable for ChaCha20
     */
    public static SecretKey createSecretKeyFromBytes(byte[] keyBytes) {
        return new SecretKeySpec(keyBytes, "ChaCha20");
    }

    /**
     * Encrypt plaintext data using ChaCha20-Poly1305.
     *
     * @param rawData   the plaintext to encrypt
     * @param secretKey the ChaCha20 secret key
     * @return an array with two Base64-encoded strings:
     *         index 0: ciphertext
     *         index 1: nonce
     * @throws Exception if the encryption fails
     */
    public static String[] symmetricEncryptData(String rawData, SecretKey secretKey) throws Exception {
        // Generate a random nonce
        byte[] nonce = new byte[NONCE_SIZE];
        new SecureRandom().nextBytes(nonce);

        // Initialize Cipher for ChaCha20-Poly1305 encryption
        Cipher cipher = Cipher.getInstance("ChaCha20-Poly1305");
        IvParameterSpec ivSpec = new IvParameterSpec(nonce);
        cipher.init(Cipher.ENCRYPT_MODE, secretKey, ivSpec);

        // Convert plaintext to bytes
        byte[] rawDataBytes = rawData.getBytes("UTF-8");

        // Encrypt
        byte[] encryptedData = cipher.doFinal(rawDataBytes);

        // Base64-encode the ciphertext and nonce
        String ciphertextB64 = Base64.getEncoder().encodeToString(encryptedData);
        String nonceB64 = Base64.getEncoder().encodeToString(nonce);

        return new String[] { ciphertextB64, nonceB64 };
    }

    /**
     * Decrypt ciphertext data using ChaCha20-Poly1305.
     *
     * @param ciphertextB64 the Base64-encoded ciphertext
     * @param secretKey     the ChaCha20 secret key
     * @param nonceB64      the Base64-encoded nonce
     * @return the decrypted plaintext
     * @throws Exception if the decryption fails
     */
    public static String symmetricDecryptData(String ciphertextB64, SecretKey secretKey, String nonceB64)
            throws Exception {
        byte[] encryptedData = Base64.getDecoder().decode(ciphertextB64);
        byte[] nonce = Base64.getDecoder().decode(nonceB64);

        // Initialize for decryption
        Cipher cipher = Cipher.getInstance("ChaCha20-Poly1305");
        IvParameterSpec ivSpec = new IvParameterSpec(nonce);
        cipher.init(Cipher.DECRYPT_MODE, secretKey, ivSpec);

        // Decrypt
        byte[] decryptedBytes = cipher.doFinal(encryptedData);
        return new String(decryptedBytes, "UTF-8");
    }

    /**
     * Convenience method to generate a random 32-byte key.
     *
     * @return a new random 256-bit key
     */
    public static byte[] generateRandomKeyBytes() {
        byte[] keyBytes = new byte[KEY_SIZE_BYTES];
        new SecureRandom().nextBytes(keyBytes);
        return keyBytes;
    }

    /**
     * Encrypts (wraps) the given symmetric key using the provided RSA public key.
     *
     * @param symmetricKey the symmetric key bytes to encrypt (e.g. 32 bytes for
     *                     ChaCha20)
     * @param publicKey    the RSA public key for encryption
     * @return a Base64-encoded string of the encrypted symmetric key
     * @throws Exception if encryption fails
     */
    /**
     * Encrypts (wraps) the given symmetric key using the provided RSA public key.
     *
     * @param symmetricKey the symmetric key bytes to encrypt (e.g. 32 bytes for
     *                     ChaCha20)
     * @param publicKey    the RSA public key for encryption
     * @return a Base64-encoded string of the encrypted symmetric key
     * @throws Exception if encryption fails
     */
    public static String asymmetricEncryptSymmetricKey(byte[] symmetricKey, PublicKey publicKey) throws Exception {
        // Initialize cipher with RSA and OAEP padding (using SHA-1 by default)
        Cipher cipher = Cipher.getInstance("RSA/ECB/OAEPWithSHA-1AndMGF1Padding");
        cipher.init(Cipher.ENCRYPT_MODE, publicKey);

        // Encrypt the symmetric key
        byte[] encryptedKey = cipher.doFinal(symmetricKey);
        return Base64.getEncoder().encodeToString(encryptedKey);
    }

    /**
     * Decrypts (unwraps) the given encrypted symmetric key using the provided RSA
     * private key.
     *
     * @param encryptedSymmetricKeyB64 Base64-encoded encrypted symmetric key
     * @param privateKey               the RSA private key for decryption
     * @return the decrypted symmetric key as a byte array
     * @throws Exception if decryption fails
     */
    public static byte[] asymmetricDecryptSymmetricKey(String encryptedSymmetricKeyB64, PrivateKey privateKey)
            throws Exception {
        byte[] encryptedKey = Base64.getDecoder().decode(encryptedSymmetricKeyB64);
        Cipher cipher = Cipher.getInstance("RSA/ECB/OAEPWithSHA-1AndMGF1Padding");
        cipher.init(Cipher.DECRYPT_MODE, privateKey);
        return cipher.doFinal(encryptedKey);
    }

    /**
     * Convenience method to generate an RSA key pair.
     *
     * @param keySize the size of the RSA key (e.g., 2048)
     * @return a KeyPair containing the generated RSA public and private keys
     * @throws Exception if key generation fails
     */
    public static KeyPair generateRSAKeyPair(int keySize) throws Exception {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        System.out.println(kpg);
        kpg.initialize(4096);
        return kpg.generateKeyPair();
    }

    /**
     * Loads an RSA public key from a PEM-formatted string.
     *
     * @param pem the PEM-formatted public key string
     * @return the PublicKey object
     * @throws Exception if key loading fails
     */
    public static PublicKey loadPublicKey(String pem) throws Exception {
        String publicKeyPEM = pem
                .replace("-----BEGIN PUBLIC KEY-----", "")
                .replace("-----END PUBLIC KEY-----", "")
                .replaceAll("\\s", "");
        byte[] decoded = Base64.getDecoder().decode(publicKeyPEM);
        X509EncodedKeySpec keySpec = new X509EncodedKeySpec(decoded);
        KeyFactory kf = KeyFactory.getInstance("RSA");
        return kf.generatePublic(keySpec);
    }

    /**
     * Loads an RSA private key from a PEM-formatted string.
     *
     * @param pem the PEM-formatted private key string
     * @return the PrivateKey object
     * @throws Exception if key loading fails
     */
    public static PrivateKey loadPrivateKey(String pem) throws Exception {
        String privateKeyPEM = pem
                .replace("-----BEGIN PRIVATE KEY-----", "")
                .replace("-----END PRIVATE KEY-----", "")
                .replaceAll("\\s", "");
        byte[] decoded = Base64.getDecoder().decode(privateKeyPEM);
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(decoded);
        KeyFactory kf = KeyFactory.getInstance("RSA");
        return kf.generatePrivate(keySpec);
    }
}

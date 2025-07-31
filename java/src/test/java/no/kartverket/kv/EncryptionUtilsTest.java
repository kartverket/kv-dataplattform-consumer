package no.kartverket.kv;

import org.junit.Test;
import static org.junit.Assert.*;

import javax.crypto.SecretKey;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;

public class EncryptionUtilsTest {

    @Test
    public void testSymmetricEncryptDecrypt() throws Exception {
        // Generate key
        byte[] keyBytes = EncryptionUtils.generateRandomKeyBytes();
        SecretKey secretKey = EncryptionUtils.createSecretKeyFromBytes(keyBytes);

        // Sample data
        String rawData = "R1yyJ9FzMFLEnJiToø341\n" +
                "LekapSLfU07æFj2wAChhz\n" +
                "jcZpRH5nNå6q8zM3xiR76\n";

        // Encrypt
        String[] encryptedResult = EncryptionUtils.symmetricEncryptData(rawData, secretKey);
        String ciphertextB64 = encryptedResult[0];
        String nonceB64 = encryptedResult[1];

        // Decrypt
        String decryptedData = EncryptionUtils.symmetricDecryptData(ciphertextB64, secretKey, nonceB64);

        // Verify
        assertEquals(rawData, decryptedData);
    }

    @Test
    public void testAsymmetricEncryptDecryptSymmetricKey() throws Exception {
        // Generate a symmetric key using the EncryptionUtils method
        byte[] symmetricKey = EncryptionUtils.generateRandomKeyBytes();

        // Generate an RSA key pair (2048-bit)
        KeyPair keyPair = EncryptionUtils.generateRSAKeyPair(2048);
        PublicKey publicKey = keyPair.getPublic();
        System.out.println("Generated public key" + publicKey.getEncoded());
        PrivateKey privateKey = keyPair.getPrivate();

        // Encrypt the symmetric key using the RSA public key
        String encryptedSymmetricKeyB64 = EncryptionUtils.asymmetricEncryptSymmetricKey(symmetricKey, publicKey);
        // Decrypt the symmetric key using the RSA private key
        byte[] decryptedSymmetricKey = EncryptionUtils.asymmetricDecryptSymmetricKey(encryptedSymmetricKeyB64,
                privateKey);
        // Verify that the original symmetric key matches the decrypted key
        assertArrayEquals(symmetricKey, decryptedSymmetricKey);
    }
}

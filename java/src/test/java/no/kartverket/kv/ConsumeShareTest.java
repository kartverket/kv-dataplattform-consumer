package no.kartverket.kv;

import org.junit.Test;
import static org.junit.Assert.*;

import javax.crypto.SecretKey;

import java.io.FileWriter;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Base64;

public class ConsumeShareTest {

    @Test
    public void testEncryptedThenDecryptedDataReturnsOriginalColumnForRecipient() throws Exception {
        String[] originalData = { "Old York", "San Francisco", "Pineapple", "Legolazer 3000" };

        KeyPair recipientKeys = EncryptionUtils.generateRSAKeyPair(2048);
        PublicKey publicKey = recipientKeys.getPublic();
        PrivateKey privateKey = recipientKeys.getPrivate();

        byte[] symmetricKeyBytes = EncryptionUtils.generateRandomKeyBytes();
        SecretKey symmetricKey = EncryptionUtils.createSecretKeyFromBytes(symmetricKeyBytes);

        // Encrypt the sample data with symmetric key
        String[] encryptedData = new String[originalData.length];
        String[] nonces = new String[originalData.length];
        for (int i = 0; i < originalData.length; i++) {
            String[] result = EncryptionUtils.symmetricEncryptData(originalData[i], symmetricKey);
            encryptedData[i] = result[0];
            nonces[i] = result[1];
        }

        // Encrypt the symmetric key with recipient's public key
        String encryptedSymmetricKey = EncryptionUtils.asymmetricEncryptSymmetricKey(symmetricKeyBytes, publicKey);

        String privateKeyPem = "-----BEGIN PRIVATE KEY-----\n"
                + Base64.getMimeEncoder(64, "\n".getBytes()).encodeToString(privateKey.getEncoded())
                + "\n-----END PRIVATE KEY-----";

        // Write encrypted test data and keys to files to test decryption with Python
        new java.io.File("../shared_test_data").mkdirs();
        try (FileWriter file = new FileWriter("../shared_test_data/encryption_test.json")) {
            file.write("{\n");

            file.write("  \"original_data\": [\n");
            for (int i = 0; i < originalData.length; i++) {
                file.write("    \"" + originalData[i] + "\"");
                if (i < originalData.length - 1)
                    file.write(",");
                file.write("\n");
            }
            file.write("  ],\n");

            file.write("  \"encrypted_data\": [\n");
            for (int i = 0; i < encryptedData.length; i++) {
                file.write("    \"" + encryptedData[i] + "\"");
                if (i < encryptedData.length - 1)
                    file.write(",");
                file.write("\n");
            }
            file.write("  ],\n");

            file.write("  \"nonces\": [\n");
            for (int i = 0; i < nonces.length; i++) {
                file.write("    \"" + nonces[i] + "\"");
                if (i < nonces.length - 1)
                    file.write(",");
                file.write("\n");
            }
            file.write("  ],\n");

            file.write("  \"encrypted_key\": \"" + encryptedSymmetricKey + "\"\n");

            file.write("}\n");
        }
        try (FileWriter file = new FileWriter("../shared_test_data/encryption_test_private_key.pem")) {
            file.write(privateKeyPem);
        }

        // Decrypt the symmetric key with recipient's private key
        byte[] decryptedSymmetricKeyBytes = EncryptionUtils.asymmetricDecryptSymmetricKey(encryptedSymmetricKey,
                privateKey);
        SecretKey decryptedSymmetricKey = EncryptionUtils.createSecretKeyFromBytes(decryptedSymmetricKeyBytes);

        // Decrypt to get the original data back
        String[] decryptedData = new String[originalData.length];
        for (int i = 0; i < encryptedData.length; i++) {
            decryptedData[i] = EncryptionUtils.symmetricDecryptData(encryptedData[i], decryptedSymmetricKey, nonces[i]);
        }

        assertArrayEquals(decryptedData, originalData);
    }
}

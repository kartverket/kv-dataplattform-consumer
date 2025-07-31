package no.kartverket.kv;

import javax.crypto.SecretKey;
import java.util.Base64;

public class Main {

    public static void main(String[] args) {
        try {
            // Generate a new 32-byte key
            byte[] keyBytes = EncryptionUtils.generateRandomKeyBytes();
            // Wrap it in a SecretKey
            SecretKey secretKey = EncryptionUtils.createSecretKeyFromBytes(keyBytes);

            // Some sample plaintext
            String rawData = "Hello, World!";

            // Encrypt the data
            String[] encryptedResult = EncryptionUtils.symmetricEncryptData(rawData, secretKey);
            String ciphertextB64 = encryptedResult[0];
            String nonceB64 = encryptedResult[1];

            // Decrypt the data
            String decryptedData = EncryptionUtils.symmetricDecryptData(ciphertextB64, secretKey, nonceB64);

            // Print results
            System.out.println("Ciphertext (Base64):   " + ciphertextB64);
            System.out.println("Nonce (Base64):        " + nonceB64);
            System.out.println("Decrypted data:        " + decryptedData);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

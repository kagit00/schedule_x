package com.shedule.x.utils.basic;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.SecureRandom;

@Slf4j
public final class EncryptionUtil {

    private EncryptionUtil() {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    /**
     * Encrypt string.
     *
     * @param data the data
     * @return the string
     */
    public static String encrypt(String data, String secretKey) {
        try {
            Cipher cipher = Cipher.getInstance("AES/CFB/NoPadding");
            byte[] iv = generateRandomIV();
            SecretKeySpec secretKeySpec = new SecretKeySpec(secretKey.getBytes(), "AES");
            IvParameterSpec ivParameterSpec = new IvParameterSpec(iv);
            cipher.init(Cipher.ENCRYPT_MODE, secretKeySpec, ivParameterSpec);
            byte[] encrypted = cipher.doFinal(data.getBytes());
            byte[] combined = new byte[iv.length + encrypted.length];
            System.arraycopy(iv, 0, combined, 0, iv.length);
            System.arraycopy(encrypted, 0, combined, iv.length, encrypted.length);
            return Base64.encodeBase64String(combined);
        } catch (Exception e) {
            log.error("Error encrypting data: {}", e.getMessage());
            return "";
        }
    }

    /**
     * Decrypt string.
     *
     * @param encryptedData the encrypted data
     * @return the string
     */
    public static String decrypt(String encryptedData, String secretKey) {
        try {
            Cipher cipher = Cipher.getInstance("AES/CFB/NoPadding");
            byte[] combined = Base64.decodeBase64(encryptedData);
            byte[] iv = new byte[16];
            byte[] encrypted = new byte[combined.length - iv.length];
            System.arraycopy(combined, 0, iv, 0, iv.length);
            System.arraycopy(combined, iv.length, encrypted, 0, encrypted.length);
            SecretKeySpec secretKeySpec = new SecretKeySpec(secretKey.getBytes(), "AES");
            IvParameterSpec ivParameterSpec = new IvParameterSpec(iv);
            cipher.init(Cipher.DECRYPT_MODE, secretKeySpec, ivParameterSpec);
            byte[] decrypted = cipher.doFinal(encrypted);
            return new String(decrypted);
        } catch (Exception e) {
            log.error("Error decrypting data: {}", e.getMessage());
            return "";
        }
    }

    private static byte[] generateRandomIV() {
        SecureRandom secureRandom = new SecureRandom();
        byte[] iv = new byte[16];
        secureRandom.nextBytes(iv);
        return iv;
    }
}
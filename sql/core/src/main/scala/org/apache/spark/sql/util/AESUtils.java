package org.apache.spark.sql.util;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

public class AESUtils {
    private static final int KEY_LENGTH = 16;//指定key的字节长度
    private static final String KEY_STR = "pingansaskey";//密钥
    private static final String CHARSETNAME = "UTF-8";//编码
    private static final String KEY_ALGORITHM = "AES";
    private static final String CIPHER_ALGORITHM = "AES/ECB/PKCS5Padding";//指定填充方式

    private static SecretKeySpec generateKey(byte[] password) throws Exception {
        if(password.length == KEY_LENGTH) {
            return new SecretKeySpec(password, KEY_ALGORITHM);
        }else if (password.length < KEY_LENGTH) {
            byte[] pwd = new byte[KEY_LENGTH];
            for (int i = 0; i < password.length; i++) {
                pwd[i] = password[i];
            }
            for (int i = password.length; i < KEY_LENGTH; i++) {
                pwd[i] = 0;
            }
            return new SecretKeySpec(pwd, KEY_ALGORITHM);
        } else {
            byte[] pwd = new byte[KEY_LENGTH];
            for (int i = 0; i < KEY_LENGTH ; i++) {
                pwd[i] = password[i];
            }
            return new SecretKeySpec(pwd, KEY_ALGORITHM);
        }
    }

    public static byte[] encrypt(byte[] content, byte[] password) throws Exception {
        Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
        cipher.init(Cipher.ENCRYPT_MODE, generateKey(password));
        return cipher.doFinal(content);
    }

    public static byte[] decrypt(byte[] content, byte[] password) throws Exception {
        Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
        cipher.init(Cipher.DECRYPT_MODE, generateKey(password));
        return cipher.doFinal(content);
    }

    /**
     * 原始加密
     * @param content 加密内容
     * @return
     */
    private static byte[] encrypt(String content) {
        try {
            byte[] byteContent = content.getBytes(CHARSETNAME);
            byte[] password = KEY_STR.getBytes(CHARSETNAME);
            return encrypt(byteContent, password);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 原始加密
     * @param key
     * @param content
     * @return
     */
    private static byte[] encrypt(String key, String content) {
        try {
            if (key == null || key.equals("")) {
                key = KEY_STR;
            }
            byte[] byteContent = content.getBytes(CHARSETNAME);
            byte[] password = key.getBytes(CHARSETNAME);
            return encrypt(byteContent, password);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 解密
     * @param content
     * @return
     */
    public static String decrypt(String content) {
        try {
            byte[] decryptFrom = parseHexStr2Byte(content);
            byte[] password = KEY_STR.getBytes(CHARSETNAME);
            byte[] result = decrypt(decryptFrom, password);
            return new String(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private static byte[] parseHexStr2Byte(String hexStr) {
        if (hexStr.length() < 1) {
            return null;
        }
        byte[] result = new byte[hexStr.length() / 2];
        for (int i = 0; i < hexStr.length() / 2; i++) {
            int high = Integer.parseInt(hexStr.substring(i * 2, i * 2 + 1), 16);
            int low = Integer.parseInt(hexStr.substring(i * 2 + 1, i * 2 + 2),16);
            result[i] = (byte) (high * 16 + low);
        }
        return result;
    }

    public static void main(String[] args) {
        String decryptResult = decrypt("25BE6A471C27881A0AD87C01D7F91C94");
        System.out.println(decryptResult);
    }
}

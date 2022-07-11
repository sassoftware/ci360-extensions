package com.sas.ci360.agent.util;

import net.snowflake.client.jdbc.SnowflakeDriver;

import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.operator.InputDecryptorProvider;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;
import org.bouncycastle.pkcs.PKCSException;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.PrivateKey;
import java.security.Security;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.json.JSONObject;
import com.sas.ci360.agent.util.Constants;

public class SnowflakeUtils {
    private static final Logger logger = LoggerFactory.getLogger(SnowflakeUtils.class);
    // private static final String PRIVATE_KEY_FILE =
    // "C:/temp/snowflake_rsa_key.p8";
    
    private static Connection conn;
    private static Properties config;

    public static class PrivateKeyReader {

        // If you generated an encrypted private key, implement this method to return
        // the passphrase for decrypting your private key.
        private static String getPrivateKeyPassphrase() {
            return "";
        }

        public static PrivateKey get(String filename)
                throws Exception {
            PrivateKeyInfo privateKeyInfo = null;
            Security.addProvider(new BouncyCastleProvider());
            // Read an object from the private key file.
            PEMParser pemParser = new PEMParser(new FileReader(Paths.get(filename).toFile()));
            Object pemObject = pemParser.readObject();
            if (pemObject instanceof PKCS8EncryptedPrivateKeyInfo) {
                // Handle the case where the private key is encrypted.
                PKCS8EncryptedPrivateKeyInfo encryptedPrivateKeyInfo = (PKCS8EncryptedPrivateKeyInfo) pemObject;
                String passphrase = getPrivateKeyPassphrase();
                InputDecryptorProvider pkcs8Prov = new JceOpenSSLPKCS8DecryptorProviderBuilder()
                        .build(passphrase.toCharArray());
                privateKeyInfo = encryptedPrivateKeyInfo.decryptPrivateKeyInfo(pkcs8Prov);
            } else if (pemObject instanceof PrivateKeyInfo) {
                // Handle the case where the private key is unencrypted.
                privateKeyInfo = (PrivateKeyInfo) pemObject;
            }
            pemParser.close();
            JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME);
            return converter.getPrivateKey(privateKeyInfo);
        }
    }

    public static void createConnection(Properties config) {
        SnowflakeUtils.config = config;
        createConnection();
    }

    private static void createConnection() {
        try {

            String url = config.getProperty("sf.connction_url");
            logger.debug("snowflake account url: " + url);
            Properties prop = new Properties();
            prop.put("user", config.getProperty("sf.user"));
            prop.put("privateKey", PrivateKeyReader.get(config.getProperty("sf.PRIVATE_KEY_FILE")));
            prop.put("db", config.getProperty("sf.database"));
            logger.debug("snowflake database: " + config.getProperty("sf.database"));

            prop.put("schema", config.getProperty("sf.schema"));
            logger.debug("snowflake schema: " + config.getProperty("sf.schema"));
            prop.put("warehouse", config.getProperty("sf.warehouse"));
            logger.debug("snowflake warehouse: " + config.getProperty("sf.warehouse"));
            prop.put("role", config.getProperty("sf.role"));
            logger.debug("snowflake role: " + config.getProperty("sf.role"));

            conn = DriverManager.getConnection(url, prop);
            Statement stat = conn.createStatement();
            ResultSet res = stat.executeQuery("select 1");
            res.next();
            logger.debug(res.getString(1));
            res.close();
            stat.close();
            // conn.close();
            logger.info("Connection to Snowflake is established successfully.");
        } catch (Exception ex) {
            logger.error("Exception connecting to Snowflake.", ex);
        }
    }

    public static boolean insertEvent(JSONObject jsonEvent) {
        try {

            String table_name = config.getProperty("sf.table_name");
            String columns = config.getProperty("sf.columns");
            String[] columns_arr = columns.split(",");
            int num_columns = columns_arr.length;
            String values = "";
            for (int i = 0; i < num_columns; i++) {
                values = values + "?,";
            }
            values = values.substring(0, values.length() - 1);

            if (null == conn || conn.isClosed())
                createConnection();
            // Statement stat = conn.createStatement();
            PreparedStatement stat = conn
                    .prepareStatement("insert into " + table_name + " (" + columns + ") values (" + values + ")");
            JSONObject eventAttr = jsonEvent.getJSONObject(Constants.JSON_ATTRIBUTES);
            // String eventName = eventAttr.getString(JSON_EVENTNAME);
            for (int i = 0; i < num_columns; i++) {
                if (columns_arr[i].equals("EVENT_JSON")) {
                    stat.setString(i + 1, jsonEvent.toString());
                } else if (columns_arr[i].equals(Constants.JSON_ROWKEY)) {
                    stat.setString(i + 1, jsonEvent.getString(columns_arr[i]));
                } else {
                    try {
                        stat.setString(i + 1, eventAttr.getString(columns_arr[i]));
                    } catch (Exception ex) {
                        stat.setString(i + 1, "");
                    }
                }
            }
            stat.execute();
            logger.info("(rowKey:"+jsonEvent.getString(Constants.JSON_ROWKEY)+"), "+"Event inserted to Snowflake. Event name: "+eventAttr.getString(Constants.JSON_EVENTNAME));
            stat.close();
            return true;
        } catch (Exception ex) {
            logger.error("(rowKey:"+jsonEvent.getString(Constants.JSON_ROWKEY)+"), "+"Exception inserting event to Snowflake.", ex);
        }
        return false;
    }
}

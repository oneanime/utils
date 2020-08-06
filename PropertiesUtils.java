package com.hp.gmall.realtime.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtils {
    private static PropertiesUtils propertiesUtil = null;
    private Properties properties = null;

    private PropertiesUtils(String filePath) {
        readPropertiesFile(filePath);
    }

    private void readPropertiesFile(String filePath) {
        properties = new Properties();
        InputStream in = null;
        try {
            in = new FileInputStream(filePath);
            properties.load(in);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                assert in != null;
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static synchronized PropertiesUtils load(String filePath) {
        if (propertiesUtil == null) {
            propertiesUtil = new PropertiesUtils(filePath);
        }
        return propertiesUtil;
    }

    public Properties getProperties() {
        return propertiesUtil.properties;
    }

    public int getInt(String key,int defaultValue) {
        return Integer.parseInt(properties.getProperty(key, String.valueOf(defaultValue)));
    }

    public String getString(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    public Boolean getBoolean(String key, Boolean defaultValue) {
        return Boolean.parseBoolean(properties.getProperty(key, String.valueOf(defaultValue)));
    }
}

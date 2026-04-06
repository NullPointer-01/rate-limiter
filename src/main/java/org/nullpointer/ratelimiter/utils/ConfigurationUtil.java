package org.nullpointer.ratelimiter.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigurationUtil {
    private static final String DEFAULT_CONFIG_PATH = "conf.props";
    private static volatile ConfigurationUtil INSTANCE;

    private final Properties properties;

    private ConfigurationUtil(String resourcePath) {
        properties = new Properties();
        try (InputStream stream = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
            if (stream == null) {
                throw new IllegalStateException("Configuration file not found on classpath: " + resourcePath);
            }
            properties.load(stream);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load configuration file: " + resourcePath, e);
        }
    }

    public static ConfigurationUtil getInstance() {
        if (INSTANCE == null) {
            synchronized (ConfigurationUtil.class) {
                if (INSTANCE == null) {
                    INSTANCE = new ConfigurationUtil(DEFAULT_CONFIG_PATH);
                }
            }
        }
        return INSTANCE;
    }

    public String get(String key) {
        return properties.getProperty(key);
    }
}

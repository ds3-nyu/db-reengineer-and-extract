/*
 * The MIT License
 *
 * Copyright 2019 NYU (Heiko Mueller <heiko.mueller@nyu.edu>).
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.ds3.db;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * Establish a connection to a JDBC database. All configuration parameters
 * are expected to be given in the environment variables:
 * 
 * DS3_DB_DRIVER: Name of the JDBC driver
 * DS3_DB_URL: Database URL used by the JDBC driver to connect to the database
 * DS3_DB_USERNAME: Database username for authentication with the database
 * DS3_DB_PASSWORD: User password for authentication with the database
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public final class JDBCConnector {
    
    public static final String DS3_DB_DRIVER = "DS3_DB_DRIVER";
    public static final String DS3_DB_URL = "DS3_DB_URL";
    public static final String DS3_DB_USERNAME = "DS3_DB_USERNAME";
    public static final String DS3_DB_PASSWORD = "DS3_DB_PASSWORD";
    
    /**
     * Open connection to database that is specified in the respective
     * environment variables.
     * 
     * @return
     * @throws java.sql.SQLException 
     */
    public static Connection connect() throws java.sql.SQLException {
        
        String driver;
        driver = JDBCConnector.getEnv(DS3_DB_DRIVER, "org.postgresql.Driver");
        
        try {
            Class.forName(driver);
        } catch (java.lang.ClassNotFoundException ex) {
            throw new RuntimeException(ex);
        }
        
        String url  = JDBCConnector.getEnv(DS3_DB_URL);
        String name  = JDBCConnector.getEnv(DS3_DB_USERNAME);
        String passwd  = JDBCConnector.getEnv(DS3_DB_PASSWORD);
        
        return DriverManager.getConnection(url, name, passwd);
    }
    
    /**
     * Get value for given environment variable. If the variable is not set
     * (or the empty string) the default value is returned.
     * 
     * @param key
     * @param defaultValue
     * @return 
     */
    private static String getEnv(String key, String defaultValue) {
        
        String value = System.getenv(key);
        if (value == null) {
            return defaultValue;
        } else if (value.trim().equals("")) {
            return defaultValue;
        } else {
            return value;
        }
    }
    
    /**
     * Get value for given environment variable. If the variable is not set
     * null is returned as the default value.
     * 
     * @param key
     * @return 
     */
    public static String getEnv(String key) {
        
        return JDBCConnector.getEnv(key, null);
    }
}

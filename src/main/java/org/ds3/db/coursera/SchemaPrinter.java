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
package org.ds3.db.coursera;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.ds3.db.JDBCConnector;

/**
 * Print schema information for non-empty tables in the Coursera database
 * dump.
 * 
 * Outputs the table name and row count together with a list of all column
 * names and their data type.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SchemaPrinter {
    
    private static final Logger LOGGER = Logger
            .getLogger(SchemaPrinter.class.getName());
    
    public void run(PrintWriter out) throws java.sql.SQLException {
    
        try (
                Connection con = JDBCConnector.connect();
                Statement stmt = con.createStatement();
        ) {
            DatabaseMetaData metadata = con.getMetaData();
            try (ResultSet tables = metadata.getTables(null, null, null, new String[]{"TABLE"})) {
                while (tables.next()) {
                    String tableName = tables.getString("TABLE_NAME");
                    String sql = "SELECT COUNT(*) FROM " + tableName;
                    try (ResultSet rs = stmt.executeQuery(sql)) {
                        rs.next();
                        int count = rs.getInt(1);
                        if (count > 0) {
                            out.println(tableName + " [" + count + " rows]");
                            try (ResultSet columns = metadata.getColumns(null, null, tableName, null)) {
                                while (columns.next()) {
                                    String columnName = columns.getString("COLUMN_NAME");
                                    String columnType = columns.getString("TYPE_NAME");
                                    out.println("\t" + columnName + " (" + columnType + ")");
                                }
                            }
                            out.println();
                        }
                    }
                }
            }
        }
    }
    
    public static void main(String[] args) {
        
        try (PrintWriter out = new PrintWriter(System.out)) {
            new SchemaPrinter().run(out);
        } catch (java.sql.SQLException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}

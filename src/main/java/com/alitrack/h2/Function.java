/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.alitrack.h2;

import com.epam.parso.Column;
import com.epam.parso.SasFileReader;
import com.epam.parso.impl.SasFileReaderImpl;
import java.io.FileInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import org.h2.tools.SimpleResultSet;

/**
 *
 * @author steven
 */
public class Function {

    public static void main(String... args) throws Exception {
        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection(
                "jdbc:h2:~/test;", "sa", "");
        Statement stat = conn.createStatement();

        // Using a custom Java function
        stat.execute("CREATE ALIAS if not exists SASREAD "
                + "FOR \"com.alitrack.h2.Function.sasRead\" ");

        String path = "/Users/steven/Downloads/upin2npixw.sas7bdat";
        stat.execute("create table sas as SELECT * FROM SASREAD('" + path + "', 'UTF-8') ");

//        while (rs.next()) {
//            System.out.println(rs.getString(1) + "," + rs.getString(2));
//        }
        stat.close();
        conn.close();
    }

    public static ResultSet sasRead(Connection conn, String path, String encoding) throws IOException {
        InputStream is = new FileInputStream(path);
        SasFileReader sasFileReader = new SasFileReaderImpl(is, encoding);
        List<Column> columns = sasFileReader.getColumns();
        Object[][] rows = sasFileReader.readAll();
        SimpleResultSet simple = new SimpleResultSet();
        int columnCount = columns.size();

        simple.setAutoClose(false);
        for (int i = 0; i < columnCount; i++) {
            Column column = columns.get(i);
            String name = column.getLabel();
            //todo:Number.class or String.class
            //Types.VARCHAR, Integer.MAX_VALUE, 0
            simple.addColumn(name, Types.VARCHAR, column.getLength(), 0);
        }
        int rowLen = rows.length;
        for (int i = 0; i < rowLen; i++) {
            simple.addRow(rows[i]);
        }
        return simple;
    }

}

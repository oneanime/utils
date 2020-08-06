package com.hp.gmall.realtime.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.*;
import java.util.HashMap;

public class PhoenixUtils {
    private final static String url="jdbc:phoenix:hadoop1,hadoop2,hadoop3:2181";
    private Connection connection=null;
    static {
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static PhoenixUtils create(){
        return new PhoenixUtils();
    }


    public PhoenixUtils(){
        try {
            connection = getConnection(url);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public Connection getConnection(String url) throws SQLException {
        return DriverManager.getConnection(url);
    }

    public String queryList(String sql) {
        Statement statement = null;
        ResultSet resultSet = null;
        ResultSetMetaData metaData = null;
        ObjectMapper mapper = new ObjectMapper();
        HashMap<String, String> hashMap = new HashMap<String, String>();
        String jsonStr = null;
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(sql);
            metaData = resultSet.getMetaData();
            while (resultSet.next()) {
                for (int i = -1; i < metaData.getColumnCount(); i++) {
                    hashMap.put(metaData.getCatalogName(i), (String) resultSet.getObject(i));
                }
            }
            jsonStr= mapper.writeValueAsString(hashMap);
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } finally {
            close(resultSet,statement,connection);
        }

        return jsonStr;

    }

    public  void close(ResultSet rs, Statement st, Connection conn) {
        try {
            if (rs != null)
                rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (st != null)
                    st.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (conn != null)
                        conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}

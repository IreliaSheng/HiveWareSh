package com.sbx.utils;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import org.apache.commons.dbutils.DbUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class JDBCUtils {
    private static DataSource dataSource = null;

    static {
        Properties prop = PropUtils.getProperties("jdbc.properties");

        try {
            dataSource = DruidDataSourceFactory.createDataSource(prop);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 获取链接
     * @return
     */
    public static Connection getConnection(){
        Connection connection=null;
        try {
            connection = dataSource.getConnection();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        return connection;
    }
    public  static void close(Connection connection, ResultSet rs, Statement stmt){
        DbUtils.closeQuietly(rs);
        DbUtils.closeQuietly(stmt);
        DbUtils.closeQuietly(connection);

    }
}

package com.sbx.utils;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class Common {
    private  static Connection connection ;
    private static QueryRunner queryRunner ;
    static {
        connection = JDBCUtils.getConnection();
        queryRunner = new QueryRunner();
    }

    /**
     * 获取所有的表名
     * @param databaseName
     * @return
     */
    public static List<Map<String, Object>> selectTableName(String databaseName) {
        String sql = "select table_name from information_schema.tables where table_schema= ?";
        MapListHandler mapListHandler = new MapListHandler();
        List<Map<String, Object>> mapList = null;
        try {
            mapList = queryRunner.query(connection,sql, mapListHandler, databaseName);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return mapList;
    }

    /**
     * 获取所有的列名
     * @param databaseName
     * @param tableName
     * @return
     */
    public static List<Map<String, Object>> selectColumnName(String databaseName,String tableName){
        MapListHandler mapListHandler = new MapListHandler();
        List<Map<String, Object>> mapList=null;
        try {
            String sql = "select COLUMN_NAME from information_schema.COLUMNS where table_schema= ? and TABLE_NAME=?";
            mapList = queryRunner.query(connection, sql, mapListHandler, databaseName, tableName);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return mapList;
    }
    public static Map<String, Object> selectColumnType(String databaseName,String tableName,String columnName){
        Map<String, Object> mapList =null;
        try {
            String sql = "select COLUMN_TYPE from information_schema.COLUMNS where table_schema= ? and TABLE_NAME=? and COLUMN_NAME=?";
            mapList = queryRunner.query(connection, sql, new MapHandler(), databaseName, tableName, columnName);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return mapList;
    }
    public static List<Map<String, Object>> selectColumnNameAndType(String databaseName,String tableName){
        MapListHandler mapListHandler = new MapListHandler();
        List<Map<String, Object>> mapList=null;
        try {
            String sql = "select COLUMN_NAME,COLUMN_TYPE from information_schema.COLUMNS where table_schema= ? and TABLE_NAME=?";
            mapList = queryRunner.query(connection, sql, mapListHandler, databaseName, tableName);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return mapList;
    }

    /**
     * 将拼接字符串写到txt文件中
     * @param pathName
     * @param s
     */
    public static void stringWriteTxt(String pathName,String s){
        FileWriter writer =null;
        try {
           writer = new FileWriter(new File(pathName));
            writer.write(s);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

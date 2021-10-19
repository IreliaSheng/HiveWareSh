import com.alibaba.druid.sql.dialect.ads.visitor.AdsOutputVisitor;
import com.sbx.sh.HdfsToOds;
import com.sbx.sh.MySQLToHdfs;
import com.sbx.sh.OdsCreateTable;
import com.sbx.utils.Common;
import com.sbx.utils.JDBCUtils;
import jdk.internal.org.objectweb.asm.commons.StaticInitMerger;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class App {
    public static void main(String[] args) throws SQLException {
//        Connection connection = JDBCUtils.getConnection();
//        QueryRunner queryRunner = new QueryRunner();
//        List<Map<String, Object>> mapList = Common.selectTableName("datangpo");
//        for (Map<String, Object> map : mapList) {
//            String tableName = (String)map.get("TABLE_NAME");
////            String sql = "select COLUMN_NAME from information_schema.COLUMNS where table_schema= ? and TABLE_NAME=?";
////            List<Map<String, Object>> maps = queryRunner.query(connection, sql, new MapListHandler(), "datangpo",tableName);
//            List<Map<String, Object>> maps = Common.selectColumnNameAndType("datangpo", tableName);
//            System.out.println(tableName);
//            for (Map<String, Object> stringObjectMap : maps) {
//                System.out.println(stringObjectMap);
//
//            }
//
//        }
/*
        List<Map<String, Object>> tableMapList = Common.selectTableName("datangpo");
        for (Map<String, Object> tableMap : tableMapList) {
            List<Map<String, Object>> columnNameAnTypeList = Common.selectColumnNameAndType("datangpo", (String) tableMap.get("TABLE_NAME"));
            for (Map<String, Object> columnNameAnTypeMap : columnNameAnTypeList) {
                String columnName = (String)columnNameAnTypeMap.get("COLUMN_NAME");
                String columnType = (String)columnNameAnTypeMap.get("COLUMN_TYPE");
                System.out.println(columnName+":"+columnType);
            }
        }*/
        /**
         * ods
         */
      /*  String s = OdsCreateTable.selectFieldTypesOfEachTable("geo_standard");
        Common.stringWriteTxt("E:\\sbx\\mysqlTohdfs/odsgeoStandard.txt",s);
        *//*String s = OdsCreateTable.selectFieldTypesOfEachTable("datangpo");
        Common.stringWriteTxt("E:\\sbx\\mysqlTohdfs/odsdatangpo.txt",s);*//*
        System.out.println(s);*/
      /*  String tail="partitioned by (`dt` string)\nrow format delimited fields terminated by \'\\t\'stored as\n"+"    INPUTFORMAT \'com.hadoop.mapred.DeprecatedLzoTextInputFormat\'\n" +
                "    OUTPUTFORMAT \'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat\'\nlocation \'/warehouse/quantyu/ods/\';";
        System.out.println(tail);*/
//        String s = MySQLToHdfs.selectFieldsOfEachTable("datangpo");
//        Common.stringWriteTxt("E:\\sbx\\mysqlTohdfs/test.txt",s)"
     /*   String s = HdfsToOds.splicingHdfsToHive("geo_standard");
        Common.stringWriteTxt("E:\\sbx\\mysqlTohdfs/geo_standard_hdfsToHive.sh",s);
        System.out.println(s);*/
//        createODS("quantyu");
//        mysqlToHDFS("quantyu");
        hdfsToOds("quantyu");
    }


    /**
     * hive ods 创建表的脚本
     * @param databaseName
     */
    public static void createODS(String databaseName){
        String sql = OdsCreateTable.selectFieldTypesOfEachTable(databaseName);
        Common.stringWriteTxt("D:\\CloudPBigData\\中核hive脚本/odsQuantyu.txt",sql);
        System.out.println(sql);
    }
    public static void mysqlToHDFS(String databaseName){
        String sql = MySQLToHdfs.selectFieldsOfEachTable(databaseName);
        Common.stringWriteTxt("D:\\CloudPBigData\\中核hive脚本/mysqlToHDFS.sh",sql);
        System.out.println(sql);
    }
    public static void hdfsToOds(String databaseName){
        String s = HdfsToOds.splicingHdfsToHive(databaseName);
        Common.stringWriteTxt("D:\\CloudPBigData\\中核hive脚本/hdfsToHive.sh",s);
        System.out.println(s);
    }


}

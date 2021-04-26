package com.sbx.sh;

import com.sbx.utils.Common;

import java.util.List;
import java.util.Map;

/**
 * drop table if exists ods_dmzk0101;
 * create external table ods_dmzk0101
 * (
 * `MDBTAD` string COMMENT '工作区编号',
 * `GCJCBN` string COMMENT '钻孔编号',
 * `MDLZZ`  string COMMENT '备注'
 * )
 * partitioned by (`dt` string)
 * row format delimited fields terminated by '\t'
 * stored as
 * INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
 * OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
 * location '/warehouse/quantyu/ods/ods_dmzk0101';
 */
public class OdsCreateTable {
    public static String selectFieldTypesOfEachTable(String databaseName) {
        String header = "drop table if exists ";
        String mid = "create external table ";
        String tail = "partitioned by (`dt` string)\nrow format delimited fields terminated by \'\\t\'\nstored as\n" + "    INPUTFORMAT \'com.hadoop.mapred.DeprecatedLzoTextInputFormat\'\n" +
                "    OUTPUTFORMAT \'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat\'\nlocation \'/warehouse/";
        StringBuilder resBuilder = new StringBuilder();
        List<Map<String, Object>> tableMapList = Common.selectTableName(databaseName);
        for (Map<String, Object> tableMap : tableMapList) {
            StringBuilder builder = new StringBuilder();
            String tableName = (String) tableMap.get("TABLE_NAME");
            String newTableName = "ods_" + tableName;
            builder.append(header +newTableName + ";\n" + mid +newTableName+"\n"+"(\n");
            List<Map<String, Object>> columnNameAnTypeList = Common.selectColumnNameAndType(databaseName, tableName);
            for (int i = 0; i < columnNameAnTypeList.size(); i++) {
                String columnName = (String) columnNameAnTypeList.get(i).get("COLUMN_NAME");
                String columnType = (String) columnNameAnTypeList.get(i).get("COLUMN_TYPE");
                if (columnType.contains("int") || columnType.contains("double")||columnName.contains("decimal")) {
                    builder.append("    "+columnName+"  "+columnType+",\n");
                }else if(columnType.equals("datetime")){
                    builder.append("    "+columnName+"  "+"DATE"+",\n");
                }else{
                    builder.append("    "+columnName+"  "+"string"+",\n");
                }
            }
            int len = builder.length();
            builder.delete(len-2,len-1);
            builder.append(")\n"+tail+databaseName+"/ods/"+newTableName+"\';\n");
            resBuilder.append(builder.toString());
        }
        return resBuilder.toString();
    }

}

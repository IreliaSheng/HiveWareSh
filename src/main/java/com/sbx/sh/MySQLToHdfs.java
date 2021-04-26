package com.sbx.sh;

import com.sbx.utils.Common;
import java.util.List;
import java.util.Map;

public class MySQLToHdfs {

    //遍历查询结果list列表，根据表名查出对应表的各个字段，并拼接成相应格式的字符串
    public static String selectFieldsOfEachTable(String dataBaseName){

        String str = null;
        String str1 = "";
        String str2 = "case $1 in\n";
        String str3 = "\"first\")\n";
        /**
         * import_dmsw0301(){
         * import_data dldmdmq01 "select
         *     CHFCAA,
         *     CHFCAD,
         *     PKIAA
         * from dldmdmq01
         * where 1=1"
         * }
         *
         * case $1 in
         * "dldmdmq01")
         * import_dldmdmq01
         * ;;
         * "first")
         * import_zystszy01
         * ;;
         * esac
         */
        List<Map<String, Object>> tableNameList = Common.selectTableName(dataBaseName);

        //遍历tableNameList，根据表名查询该表的字段名
        for (Map<String, Object> stringObjectMap : tableNameList) {
            String tableName = (String) stringObjectMap.get("TABLE_NAME");

            str1 = str1 + "import_" + tableName + "(){\nimport_data " + tableName + " \"select\n";
            str2 = str2 + "\"" + tableName + "\")\n" +
                    "import_" + tableName + "\n" +
                    ";;\n";
            str3 = str3 + "import_" + tableName + "\n";

            List<Map<String, Object>> columnNameList = Common.selectColumnName(dataBaseName,tableName);

            Map<String, Object> map = null;
            for (int i = 0; i < columnNameList.size() - 1; i++) {
                map= columnNameList.get(i);
                String columnName = (String) map.get("COLUMN_NAME");

                str1 = str1 + "    " + columnName + ",\n";
            }
            map = columnNameList.get(columnNameList.size() - 1);
            String columnName = (String) map.get("COLUMN_NAME");

            str1 = str1 + "    " + columnName + "\n";
            str1 = str1 + "from " + tableName + "\nwhere 1=1\"\n}\n";

        }
        str3 = str3 + ";;\n" +
                "esac";

        str = str1 + str2 + str3;

        return str;
    }

}

import java.sql.*;
import java.util.Properties;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import net.snowflake.client.core.QueryStatus;
import net.snowflake.client.jdbc.SnowflakeConnection;
import net.snowflake.client.jdbc.SnowflakeResultSet;
import net.snowflake.client.jdbc.SnowflakeStatement;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import static java.sql.DriverManager.*;

public class SnowFlakeIntegration {
    public static void main(String[] args)
            throws Exception {
        String url = "jdbc:snowflake://******.snowflakecomputing.com?allowMultiQueries=true";
        Properties prop = new Properties();
        prop.put("user", "mill_int_query_user");
        prop.put("password", "*********");
        prop.put("role", "bundled_role_ba0bbf50_5796_403e_b931_aa46b76382f4");

        try (Connection conn = getConnection(url, prop)) {
            Statement stat = conn.createStatement();
            String query1 = "use role bundled_role_ba0bbf50_5796_403e_b931_aa46b76382f4;";
            String query2 = "select Now() as now_function, at_timezone(Now(),'America/Denver') as denver, at_timezone(Now(),'Asia/Kolkata') as india;";
            String query5 = "select Now() as now_function, Now() at timezone 'America/Chicago' as central_time, Now() at timezone 'UTC' as utc_time;";
            String query3 = "SELECT CURRENT_TIMESTAMP;";
            String query4 = "SELECT * FROM MILL_INT_TEST_DB.MILL_INT_TEST_EDW_PDS_SCHEMA.QUERY_MERGE1;";
            String query6 = "select current_timestamp as now_time, to_char(convert_timezone('America/Denver',current_timestamp())) as denver, to_char(convert_timezone('Asia/Kolkata',current_timestamp())) as india;";
            String query7=  "select convert_timezone('UTC', 'America/Chicago', current_timestamp()::timestamp_ntz) as Chicago_Time;";
            String query8 = "select convert_timezone('America/Chicago', current_timestamp()::timestamp_tz) as Chicago_Time;";
            String query9 = "select current_timestamp(5)";
            ResultSet resultSet = stat.unwrap(SnowflakeStatement.class).executeAsyncQuery(query8);
            QueryStatus queryStatus = QueryStatus.RUNNING;
            while (queryStatus == QueryStatus.RUNNING) {
                Thread.sleep(200); // 2000 milliseconds.
                queryStatus = resultSet.unwrap(SnowflakeResultSet.class).getStatus();
            }
            System.out.println(queryStatus);
            if (queryStatus != QueryStatus.FAILED_WITH_ERROR) {
                if (queryStatus != QueryStatus.SUCCESS) {
                    System.out.println("ERROR: unexpected QueryStatus: " + queryStatus);
                } else {
                    JSONArray json = new JSONArray();
                    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                    ResultSet rs = stat.getResultSet();
                    System.out.println("\n");
                    while (rs.next()) {
                        int numColumns = resultSetMetaData.getColumnCount();
                        JSONObject obj = new JSONObject();
                        for (int i = 1; i <= numColumns; i++) {
                            System.out.println("Executing: " + query8);
                            System.out.println("Output");
                            System.out.println(resultSetMetaData.getColumnName(i) + ':' +  resultSetMetaData.getColumnTypeName(i));
                            String column_name = resultSetMetaData.getColumnName(i);
                            obj.put(column_name, rs.getObject(column_name));
                        }
                        json.add(obj);
                    }
                    System.out.println(json);
                }
            } else {
                System.out.format(
                        "ERROR %d: %s%n", queryStatus.getErrorMessage(), queryStatus.getErrorCode());
            }
            conn.close();
        }
    }

}

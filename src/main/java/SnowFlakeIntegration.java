import java.sql.*;
import java.util.Calendar;
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
        String url = "jdbc:snowflake://cerner_stage.snowflakecomputing.com?allowMultiQueries=true";
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
            ResultSet resultSet = stat.unwrap(SnowflakeStatement.class).executeAsyncQuery(query2);
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

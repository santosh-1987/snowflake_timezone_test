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

public class SnowFlakeIntegration {
    public static void main(String[] args)
            throws Exception
    {
        String url = "jdbc:snowflake://cerner_stage.snowflakecomputing.com?allowMultiQueries=true";
        Properties prop = new Properties();
        prop.put("user", "mill_int_query_user");
        prop.put("password", "*********");
        prop.put("role", "bundled_role_ba0bbf50_5796_403e_b931_aa46b76382f4");

        Connection conn = DriverManager.getConnection(url, prop);
        Statement stat = conn.createStatement();
        String query1 = "use role bundled_role_ba0bbf50_5796_403e_b931_aa46b76382f4;";
        String query2 = "select Now() as now_function, at_timezone(Now(),'Asia/Kolkata') as central_time;";
        String query3 = "SELECT CURRENT_TIMESTAMP;";
//        stat.addBatch(query1);
//        stat.addBatch(query2);
//        stat.unwrap(SnowflakeStatement.class).setParameter(
//                "MULTI_STATEMENT_COUNT", 2);
        ResultSet resultSet = stat.unwrap(SnowflakeStatement.class).executeAsyncQuery( query2);
        QueryStatus queryStatus = QueryStatus.RUNNING;
        while (queryStatus == QueryStatus.RUNNING) {
            Thread.sleep(200); // 2000 milliseconds.
            queryStatus = resultSet.unwrap(SnowflakeResultSet.class).getStatus();
        }
        System.out.println(queryStatus);
        if (queryStatus == QueryStatus.FAILED_WITH_ERROR) {
            System.out.format(
                    "ERROR %d: %s%n", queryStatus.getErrorMessage(), queryStatus.getErrorCode());
        } else if (queryStatus != QueryStatus.SUCCESS) {
            System.out.println("ERROR: unexpected QueryStatus: " + queryStatus);
        } else {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int columnCount = resultSetMetaData.getColumnCount();
            for(int i = 0; i< columnCount; i++){
                System.out.print(String.format("%s(%s)", resultSetMetaData.getColumnLabel(i+1), resultSetMetaData.getColumnTypeName(i+1) ));
                System.out.print(", ");
            }
            ResultSet rs = stat.getResultSet();
            System.out.println("\n");
            while(rs.next())
            {

                for(int i = 0; i< columnCount; i++){
                    Calendar cal = Calendar.getInstance();
                    Timestamp c1 = resultSet.getTimestamp(1,cal);
                    Timestamp c2 = resultSet.getTimestamp(2, cal);

                    System.out.println(c1);
                    System.out.println(c2);
//                    Timestamp c3 = resultSet.getTimestamp(i+1);
//                    Timestamp c4 = resultSet.getTimestamp(i+1);
//                    System.out.println(c3);
//                    System.out.println(c4);
//                    System.out.print(resultSet.getTimestamp(i+1));
//                    System.out.print(", ");
                }
                System.out.println("\n");
            }
        }
        conn.close();
    }

}

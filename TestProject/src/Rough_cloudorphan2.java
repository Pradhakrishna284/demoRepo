
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Rough_cloudorphan2 {
	private static final String DRIVER = "oracle.jdbc.driver.OracleDriver";
    private static final String HOSTNAME = "a201694-cdq0782a.8186.aws-int.thomsonreuters.com";
    private static final int PORT = 1521;
    private static final String SERVICE_NAME = "pdbq0782_1.a008954398186.amazonaws.com";
    private static final String USERNAME_TDR = "CONTENT_REPO";
    private static final String PASSWORD_TDR = "RedWhale";
    private static final String JDBC_URL = "jdbc:oracle:thin:@" + HOSTNAME + ":" + PORT + "/" + SERVICE_NAME;
    
    public static void main(String[] args) {
        Connection connection = null;

        try {
            Class.forName(DRIVER);
            connection = DriverManager.getConnection(JDBC_URL, USERNAME_TDR, PASSWORD_TDR);
            if (connection != null) {
                System.out.println("Connected to Oracle Database!");
                connection.setSchema("SBXTAX5");
                ArrayList<String> entities = GetEntities();

                for (String entity : entities) {
                    handleEntity(connection, entity);
                }
            } else {
                System.out.println("Failed to connect to Oracle Database.");
            }
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        } finally {
            closeConnection(connection);
        }
    }

    private static ArrayList<String> GetEntities() {
        ArrayList<String> entitiesList = new ArrayList<>();
        entitiesList.add("'ZoneAuthorities'");
        entitiesList.add("'Zones'");
        entitiesList.add("'UniqueAreaAuthorities'");
        entitiesList.add("'ComplianceAreaAuthorities'");
        entitiesList.add("'UniqueAreas'");
        
        return entitiesList;
    }

    private static void handleEntity(Connection connection, String entity) throws SQLException {
    	    	 
        String query = GenerateQuery(entity);
        int duplicateCount = 0;

        try (PreparedStatement preparedStatement = connection.prepareStatement(query,
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
             ResultSet resultSet = preparedStatement.executeQuery()) {
            ResultSetMetaData rsMetaData = resultSet.getMetaData();
            int count = rsMetaData.getColumnCount();

            if (resultSet.last()) {
                //duplicateCount = resultSet.getRow() - 1; // Subtract 1 for the non-duplicate entry
                duplicateCount = resultSet.getRow();
                resultSet.beforeFirst();

                for (int j = 1; j <= count; j++) {
                    System.out.print(rsMetaData.getColumnName(j) + "\t\t");
                }
                System.out.println(" ");

                while (resultSet.next()) {
                    for (int j = 1; j <= count; j++) {
                        if (j != count)
                            System.out.print(resultSet.getString(j) + "\t\t");
                        else
                            System.out.println(resultSet.getString(j));
                    }
                }
            } else {
                System.out.println("There are NO duplicate UUIDs for " + entity);
            }
            System.out.println(" ");

            // Print entity name and the count of duplicate UUIDs
            if (duplicateCount > 0) {
                System.out.println("Entity: " + entity + ", Duplicate Count: " + duplicateCount);
            }
        }
    }

    public static String GenerateQuery(String entity) {

		String buildQuery = "";
		

		switch (entity) {
		case "'ZoneAuthorities'":
			buildQuery =  "SELECT o.*, ta.uuid, ta.NAME authority_name "
					+ " FROM ( "
					+ " SELECT za.zone_authority_id ,za.zone_id ,za.authority_id "
					+ " FROM  tb_zone_authorities za "
					+ " WHERE ( "
					+ " NOT EXISTS ( "
					+ " SELECT 1 "
					+ " FROM  tb_authorities au "
					+ " WHERE au.authority_id = za.authority_id "
					+ " ) "
					+ " OR NOT EXISTS ( "
					+ " SELECT 1 "
					+ " FROM  tb_zones z "
					+ " WHERE z.zone_id = za.zone_id "
					+ " ) "
					+ " ) "
					+ " ) o "
					+ " LEFT JOIN tb_authorities ta ON o.authority_id = ta.authority_id "
					+ " JOIN tb_merchants m ON ta.merchant_id = m.merchant_id "
					+ " WHERE m.name = 'Sabrix US Tax Data' "
					+ " ORDER BY 2,1,3 ";
			break;

			case "'Zones'":
			buildQuery =  "SELECT tz.*\r\n"
					+ " FROM  tb_zones tz\r\n"
					+ " JOIN tb_merchants m ON tz.merchant_id = m.merchant_id\r\n"
					+ " WHERE tz.name != 'WORLD'\r\n"
					+ " AND tz.name != 'ZONE_ID placeholder'\r\n"
					+ " AND NOT EXISTS (\r\n"
					+ " SELECT 1\r\n"
					+ " FROM  tb_zones z\r\n"
					+ " WHERE z.zone_id = tz.parent_zone_id\r\n"
					+ " AND z.merchant_id = tz.merchant_id\r\n"
					+ " )\r\n"
					+ " AND m.name = 'Sabrix US Tax Data'\r\n"
					+ "ORDER BY 2,3";
			break;

		case "'UniqueAreaAuthorities'":
			buildQuery =  "SELECT o.*, ta.uuid, ta.NAME authority_name\r\n"
					+ " FROM (\r\n"
					+ " SELECT uaa.unique_area_authority_id ,uaa.unique_area_authority_uuid\r\n"
					+ " ,uaa.unique_area_id, uaa.authority_id\r\n"
					+ " FROM  tb_unique_area_authorities uaa\r\n"
					+ " WHERE (\r\n"
					+ " NOT EXISTS (\r\n"
					+ " SELECT 1\r\n"
					+ " FROM  tb_authorities au\r\n"
					+ " WHERE au.authority_id = uaa.authority_id\r\n"
					+ " )\r\n"
					+ " OR NOT EXISTS (\r\n"
					+ " SELECT 1\r\n"
					+ " FROM  tb_unique_areas ua\r\n"
					+ " WHERE ua.unique_area_id = uaa.unique_area_id\r\n"
					+ " )\r\n"
					+ " )\r\n"
					+ " ) o\r\n"
					+ " LEFT JOIN tb_authorities ta ON o.authority_id = ta.authority_id\r\n"
					+ " JOIN tb_merchants m ON ta.merchant_id = m.merchant_id\r\n"
					+ " WHERE m.name = 'Sabrix US Tax Data'\r\n"
					+ " ORDER BY 2,1,3";	
			break;

		case "'UniqueAreas'":
			//Note:- I replaced ua.* with all the column names except area_polygon since it has xml data
			buildQuery = "SELECT ua.unique_area_id, ua.unique_area_uuid, ua.uaid, ua.area_zone, ua.compliance_area_id, ua.merchant_id,\r\n"
					+ "ua.merchant_uuid, ua.start_date, ua.end_date, ua.created_by, ua.creation_date, ua.last_updated_by, \r\n"
					+ "ua.last_update_date, ua.synchronization_timestamp, ua.uuid, ua.compliance_area_content_uuid, \r\n"
					+ "ua.compliance_area_uuid \r\n"
					+ " FROM  tb_unique_areas ua\r\n"
					+ " JOIN tb_merchants m ON ua.merchant_id = m.merchant_id\r\n"
					+ " WHERE NOT EXISTS (\r\n"
					+ " SELECT 1\r\n"
					+ " FROM  tb_compliance_areas ca\r\n"
					+ " WHERE ca.compliance_area_id = ua.compliance_area_id\r\n"
					+ " AND ca.merchant_id = ua.merchant_id\r\n"
					+ " )\r\n"
					+ " AND m.name = 'Sabrix US Tax Data'\r\n"
					+ "ORDER BY 3";	
			break;
			
		case "'ComplianceAreaAuthorities'":
			buildQuery = "SELECT o.*, ta.uuid, ta.NAME authority_name\r\n"
					+ " FROM (\r\n"
					+ " SELECT caa.compliance_area_auth_id ,caa.compliance_area_id ,caa.authority_id\r\n"
					+ " FROM  tb_comp_area_authorities caa\r\n"
					+ " WHERE (\r\n"
					+ " NOT EXISTS (\r\n"
					+ " SELECT 1\r\n"
					+ " FROM  tb_authorities au\r\n"
					+ " WHERE au.authority_id = caa.authority_id\r\n"
					+ " )\r\n"
					+ " OR NOT EXISTS (\r\n"
					+ " SELECT 1\r\n"
					+ " FROM  tb_compliance_areas ca\r\n"
					+ " WHERE ca.compliance_area_id = caa.compliance_area_id\r\n"
					+ " )\r\n"
					+ " )\r\n"
					+ " ) o\r\n"
					+ " LEFT JOIN tb_authorities ta ON o.authority_id = ta.authority_id\r\n"
					+ " JOIN tb_merchants m ON ta.merchant_id = m.merchant_id\r\n"
					+ " WHERE m.name = 'Sabrix US Tax Data'\r\n"
					+ " ORDER BY 2,1,3";	
			break;

		default:
			System.out.println("Default query executed");
			buildQuery = "select * from TB_TRANSPORTATION_TYPES";
			break;
		}
		return buildQuery;
	}

    private static void closeConnection(Connection connection) {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

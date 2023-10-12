import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class CLOUD_ORPHAN_CHECK {

    private static final String DRIVER = "oracle.jdbc.driver.OracleDriver";
    private static final String HOSTNAME = "a201694-cdq0782a.8186.aws-int.thomsonreuters.com";
    private static final int PORT = 1521;
    private static final String SERVICE_NAME = "pdbq0782_1.a008954398186.amazonaws.com";
    private static final String USERNAME_TDR = "CONTENT_REPO";
    private static final String PASSWORD_TDR = "RedWhale";
    private static final String JDBC_URL = "jdbc:oracle:thin:@" + HOSTNAME + ":" + PORT + "/" + SERVICE_NAME;
    private static final String SCHEMA_NAME = "SBXTAX5";

    public static void main(String[] args) {
        try {
            DatabaseUtils dbUtils = new DatabaseUtils(JDBC_URL, USERNAME_TDR, PASSWORD_TDR);
            Connection connection = dbUtils.getConnection();

            if (connection != null) {
                // Database is connected, set the schema
                dbUtils.setSchema(SCHEMA_NAME);

                ArrayList<String> entities = DatabaseUtils.getEntitiesToCheckForOrphanRecordsInCloud();
                Workbook workbook = new XSSFWorkbook();
                Sheet currentSheet = workbook.createSheet("Orphan Records in Cloud " + (SCHEMA_NAME));
                
                // Print the header for the console output only once
                System.out.println(String.format("%-30s %-15s", "Entity", "Orphan Records Count in " + (SCHEMA_NAME)));

                for (String entity : entities) {
                    DatabaseUtils.handleEntity(connection, entity, currentSheet);
                }

                // Create the second sheet for entity and duplicate count
                Sheet entitySheet = workbook.createSheet("Orphan Records Count");
                DatabaseUtils.createEntityAndDuplicateCountSheet(connection, entitySheet, entities);

                // Save the workbook to a file
                String timestamp = new SimpleDateFormat("dd_MMM_yyyy_HH_mm_ss").format(new Date());
                String filename = "Cloud_Orphan_Check_" + timestamp + ".xlsx";
                DatabaseUtils.createExcelFile(workbook, filename);

                dbUtils.closeConnection();
            } else {
                System.out.println("Failed to connect to the database.");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

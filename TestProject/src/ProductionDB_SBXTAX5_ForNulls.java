import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class ProductionDB_SBXTAX5_ForNulls {

    private static final String DRIVER = "oracle.jdbc.driver.OracleDriver";
    private static final String HOSTNAME = "a201694-cdp0797a.0908.aws-int.thomsonreuters.com";
    private static final int PORT = 1521;
    private static final String SERVICE_NAME = "pdbp0797_1.a261952290908.amazonaws.com";
    private static final String USERNAME_TDR = "dev_user";
    private static final String PASSWORD_TDR = "CopperBaboon";
    private static final String JDBC_URL = "jdbc:oracle:thin:@" + HOSTNAME + ":" + PORT + "/" + SERVICE_NAME;
    private static final String SCHEMA_NAME = "SBXTAX5";
    private static final String MERCHANT_NAME = "Sabrix US Tax Data";

    public static void main(String[] args) {

        try {
        	DatabaseUtilsForNulls_Fair dbUtils = new DatabaseUtilsForNulls_Fair(JDBC_URL, USERNAME_TDR, PASSWORD_TDR);
            Connection connection = dbUtils.getConnection();

            if (connection != null) {
                // Database is connected, set the schema
                dbUtils.setSchema(SCHEMA_NAME);
                
                //Get merchant id based on name
                int merchantId = DatabaseUtilsForNulls_Fair.getMerchantIdByName(connection, MERCHANT_NAME);

                //Logic to verify null uuids
                List<String> listentities = DatabaseUtilsForNulls_Fair.getEntityAndTB_TableName_ToValidate_NULLandReferential_UUIDs(SCHEMA_NAME);
               
                Workbook workbook = new XSSFWorkbook();
                Sheet currentSheet = workbook.createSheet("Null UUIDs records in " + SCHEMA_NAME);
                
                //write records of entity that has null uuids.
                for (String entity : listentities) {
                	DatabaseUtilsForNulls_Fair.handleEntityForNullUUIDs(connection, entity, currentSheet, SCHEMA_NAME, MERCHANT_NAME, merchantId);
                }

                // Create the second sheet for entity and duplicate count
                Sheet entitySheet = workbook.createSheet("PROD DB_" + SCHEMA_NAME + "_NULL UUIDs Check");
                DatabaseUtilsForNulls_Fair.createEntityAndNullCountSheet(connection, entitySheet, listentities, SCHEMA_NAME, MERCHANT_NAME, merchantId);
                
                //Get the classname
                Class thisClass = new Object(){}.getClass();
                String className = thisClass.getEnclosingClass().getSimpleName();                
                
                // Save the workbook to a file
                String timestamp = new SimpleDateFormat("dd_MMM_yyyy_HH_mm_ss").format(new Date());
                String filename = className + "_" + timestamp + ".xlsx";
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

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

public class CLOUD_CONTENT_DB_CLOUD_US_G2_QA_Schema_Verify_for_NULLandDuplicate_UUIDs {

    private static final String DRIVER = "oracle.jdbc.driver.OracleDriver";
    private static final String HOSTNAME = "a201694-cdp0795a.0908.aws-int.thomsonreuters.com";
    private static final int PORT = 1521;
    private static final String SERVICE_NAME = "pdbp0795_1.a261952290908.amazonaws.com";
    private static final String USERNAME_TDR = "Content";
    private static final String PASSWORD_TDR = "BlueOstrich";
    private static final String JDBC_URL = "jdbc:oracle:thin:@" + HOSTNAME + ":" + PORT + "/" + SERVICE_NAME;
    private static final String SCHEMA_NAME = "CLOUD_US_G2_QA";
    private static final String MERCHANT_NAME = "Sabrix US Tax Data";

    public static void main(String[] args) {

        try {
            DatabaseUtils dbUtils = new DatabaseUtils(JDBC_URL, USERNAME_TDR, PASSWORD_TDR);
            Connection connection = dbUtils.getConnection();

            if (connection != null) {
            	// Database is connected, set the schema
                dbUtils.setSchema(SCHEMA_NAME);
                                
                //Get merchant id based on name
                int merchantId = DatabaseUtils.getMerchantIdByName(connection, MERCHANT_NAME);                
                
                List<String> listEntities = DatabaseUtils.listOf_TB_tables_for_UUID_and_Referntial_Validation(SCHEMA_NAME);     

                Workbook workbook = new XSSFWorkbook();
                
                /*
                 * UUID & Referential Validation - printing null uuid count in console and writing in excel sheet
                 * Create a sheet to write entity name and the count in the excel sheet
                 * 1. prints entity and count of records with null uuid in the console
                 * 2. writes entity name and the count in the excel sheet
                 */
                Sheet countSheet = workbook.createSheet("CLOUD CONTENT DB_" + SCHEMA_NAME + "_NULL UUID COUNT");              
                DatabaseUtils.print_NullUUID_Count_in_Console_and_write_in_ExcelSheet(connection, countSheet, listEntities, SCHEMA_NAME, MERCHANT_NAME, merchantId);
               
                System.out.println("---------------------------------------------------------------------------");
                
                /*
                 * Duplicate UUID Validation
                 * Create a sheet  write entity name and the count in the excel sheet
                 * 1. prints entity and count of records duplicate uuid in the console
                 * 2. writes entity name and the count in the excel sheet
                 */                      
                // Define column widths for formatting
        		int tableNameColumnWidth = 40;
        		int duplicateUUIDValidationColumnWidth = 20;        		
        		
        		// Print headers to the console with proper formatting
        		System.out.printf("%-" + tableNameColumnWidth + "s %-" + duplicateUUIDValidationColumnWidth + "s  %n",
        				"TABLE NAME", "DUPLICATE UUID COUNT IN " + SCHEMA_NAME);      

        		Map<String, String> mapEntities = DatabaseUtils.listOf_TableNames_To_GetDuplicateUUIDs(SCHEMA_NAME);
                countSheet = workbook.createSheet("CLOUD CONTENT DB_" + SCHEMA_NAME + "_DUPLICATE UUID COUNT");
                DatabaseUtils.print_Duplicate_Count_in_Console_and_write_in_ExcelSheet(connection, countSheet, mapEntities, SCHEMA_NAME, merchantId);

                /*
                 * UUID & Referential Validation - writing null uuid records in excel sheet
                 * 1. Create a sheet for writing null uuid records
                 * 2. For each entity, write null uuid records in excel sheet
                 */
                Sheet recordsSheet = workbook.createSheet("NULL UUID RECORDS");
                for (String entity : listEntities) {
                	DatabaseUtils.write_NullUUID_Records_in_ExcelSheet(connection, entity, recordsSheet, SCHEMA_NAME, MERCHANT_NAME, merchantId);
                }
                
                /*
                 * Duplicate UUID Validation
                 * 1. Create a sheet for writing duplicate uuid records
                 * 2. For each entity, write duplicate uuid records and the count in the excel sheet
                 */      
                recordsSheet = workbook.createSheet("DUPLICATE UUID RECORDS");                 

                for (String entity : mapEntities.keySet()) {
                    DatabaseUtils.write_Duplicate_Records_in_ExcelSheet(connection, entity, recordsSheet, SCHEMA_NAME, merchantId);
                }
                             
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

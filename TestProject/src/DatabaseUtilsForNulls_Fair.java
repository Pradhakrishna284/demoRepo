import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DatabaseUtilsForNulls_Fair {
	private Connection connection;
	private static String SCHEMANAME_SBXTAX5 = "SBXTAX5";
	
	public DatabaseUtilsForNulls_Fair(String jdbcUrl, String username, String password) {
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			connection = DriverManager.getConnection(jdbcUrl, username, password);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}

	public Connection getConnection() {
		return connection;
	}

	public void setSchema(String schemaName) {
		try {
			Statement stmt = connection.createStatement();
			stmt.execute("ALTER SESSION SET CURRENT_SCHEMA = " + schemaName);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static int getMerchantIdByName(Connection connection, String merchantName) throws SQLException {
		int merchantId = -1; // Default value in case the merchant is not found

		// Define the SQL query
		String query = "SELECT merchant_id FROM tb_merchants WHERE name = ?";

		try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
			preparedStatement.setString(1, merchantName);

			try (ResultSet resultSet = preparedStatement.executeQuery()) {
				if (resultSet.next()) {
					merchantId = resultSet.getInt("merchant_id");
				}
			}
		}

		return merchantId;
	}

	public void closeConnection() {
		try {
			if (connection != null) {
				connection.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void createExcelFile(Workbook workbook, String filename) {
		try (FileOutputStream fileOut = new FileOutputStream(filename)) {
			workbook.write(fileOut);
			System.out.println("Excel file created: " + filename);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void createSheet(Workbook workbook, String sheetName) {
		workbook.createSheet(sheetName);
	}

	public static Row createRow(Sheet sheet, int rowNum) {
		return sheet.createRow(rowNum);
	}

	public static void createCell(Row row, int cellNum, String value) {
		Cell cell = row.createCell(cellNum);
		cell.setCellValue(value);
	}

	public static int getRowCount(ResultSet resultSet) throws SQLException {
		resultSet.last();
		int rowCount = resultSet.getRow();
		resultSet.beforeFirst();
		return rowCount;
	}

	public static ResultSet executeQuery(Connection connection, String query) throws SQLException {
		PreparedStatement preparedStatement = connection.prepareStatement(query, ResultSet.TYPE_SCROLL_INSENSITIVE,
				ResultSet.CONCUR_READ_ONLY);
		return preparedStatement.executeQuery();
	}

	public static List<String> getEntityAndTB_TableName_ToValidate_NULLandReferential_UUIDs(String schemaName) {
		List<String> entityNames = new ArrayList<>();
		entityNames.add("TB_AUTHORITIES_UUID_Validation");
		entityNames.add("TB_AUTHORITIES_Referential_Validation");
		entityNames.add("TB_AUTHORITY_LOGIC_ELEMENTS_UUID_Validation");
		entityNames.add("TB_AUTHORITY_LOGIC_ELEMENTS_Referential_Validation");
		entityNames.add("TB_AUTHORITY_LOGIC_GROUP_XREF_UUID_Validation");
		entityNames.add("TB_AUTHORITY_LOGIC_GROUP_XREF_Referential_Validation");
		entityNames.add("TB_AUTHORITY_LOGIC_GROUPS_UUID_Validation");
		entityNames.add("TB_AUTHORITY_LOGIC_GROUPS_Referential_Validation");
		
		entityNames.add("TB_AUTHORITY_REQUIREMENTS_UUID_Validation");
		entityNames.add("TB_AUTHORITY_REQUIREMENTS_Referential_Validation");
		entityNames.add("TB_AUTHORITY_TYPES_UUID_Validation");
		entityNames.add("TB_AUTHORITY_TYPES_Referential_Validation");
		entityNames.add("TB_CONTRIBUTING_AUTHORITIES_UUID_Validation");
		entityNames.add("TB_CONTRIBUTING_AUTHORITIES_Referential_Validation");
		
		if(schemaName == SCHEMANAME_SBXTAX5)
		{
			entityNames.add("TB_JE_MAPPINGS_UUID_Validation");
			entityNames.add("TB_JE_MAPPINGS_Referential_Validation");
			entityNames.add("TB_OPER_LIC_TYPE_MAPPINGS_UUID_Validation");
			entityNames.add("TB_OPER_LIC_TYPE_MAPPINGS_Referential_Validation");
			entityNames.add("TB_OPERATING_LICENSE_TYPES_UUID_Validation");
			entityNames.add("TB_OPERATING_LICENSE_TYPES_Referential_Validation");

		}
		
		entityNames.add("TB_PRODUCT_CATEGORIES_UUID_Validation");
		entityNames.add("TB_PRODUCT_CATEGORIES_Referential_Validation");
		entityNames.add("TB_RATE_TIERS_UUID_Validation");
		entityNames.add("TB_RATE_TIERS_Referential_Validation");
		entityNames.add("TB_RATES_UUID_Validation");
		entityNames.add("TB_RATES_Referential_Validation");
		entityNames.add("TB_REFERENCE_LISTS_UUID_Validation");
		entityNames.add("TB_REFERENCE_LISTS_Referential_Validation");
		entityNames.add("TB_REFERENCE_VALUES_UUID_Validation");
		entityNames.add("TB_REFERENCE_VALUES_Referential_Validation");
		entityNames.add("TB_RULE_QUALIFIERS_UUID_Validation");
		entityNames.add("TB_RULE_QUALIFIERS_Referential_Validation");
		entityNames.add("TB_RULES_UUID_Validation");
		entityNames.add("TB_RULES_Referential_Validation");
		entityNames.add("TB_RULE_OUTPUTS_UUID_Validation"); //Note:- operation_date is hard_coded, please check before running the query
		//entityNames.add("TB_RULE_OUTPUTS_Referential_Validation"); //There is no such table

		if(schemaName == SCHEMANAME_SBXTAX5)
		{
			entityNames.add("TB_TRANSACTION_LOGIC_PATHS_UUID_Validation");
			entityNames.add("TB_TRANSACTION_LOGIC_PATHS_Referential_Validation");
			entityNames.add("TB_TRANSPORTATION_TYPES_UUID_Validation");
			entityNames.add("TB_TRANSPORTATION_TYPES_Referential_Validation");

			entityNames.add("TB_COMPLIANCE_AREAS_UUID_Validation");
			entityNames.add("TB_COMPLIANCE_AREAS_Referential_Validation");
			entityNames.add("TB_COMP_AREA_AUTHORITIES_UUID_Validation");
			entityNames.add("TB_COMP_AREA_AUTHORITIES_Referential_Validation");
		
			//Table or view does not exist for SBXTAX7 as on 9th Oct 2023
			entityNames.add("TB_UNIQUE_AREAS_UUID_Validation");
			entityNames.add("TB_UNIQUE_AREAS_Referential_Validation");
			entityNames.add("TB_UNIQUE_AREA_AUTHORITIES_UUID_Validation");
			entityNames.add("TB_UNIQUE_AREA_AUTHORITIES_Referential_Validation");
		}
		
 		entityNames.add("TB_ZONES_UUID_Validation");
		entityNames.add("TB_ZONES_Referential_Validation");
		entityNames.add("TB_ZONE_AUTHORITIES_UUID_Validation");
		entityNames.add("TB_ZONE_AUTHORITIES_Referential_Validation");

		entityNames.add("TB_ZONE_MATCH_CONTEXTS_UUID_Validation");
		entityNames.add("TB_ZONE_MATCH_CONTEXTS_Referential_Validation");
		entityNames.add("TB_ZONE_MATCH_PATTERNS_UUID_Validation");
		entityNames.add("TB_ZONE_MATCH_PATTERNS_Referential_Validation");		

		return entityNames;
	}

	public static String GenerateQueryStatement(String entityName, String merchantName, int merchantid) {

		String buildQuery = "";

		switch (entityName) {
		case "TB_AUTHORITIES_UUID_Validation":
			buildQuery =  "SELECT * FROM tb_authorities \r\n"
					+ "          WHERE \r\n"
					+ "                merchant_id = " + merchantid + "\r\n"
					+ "            AND (\r\n"
					+ "                    UUID IS NULL\r\n"
					+ "                OR  merchant_id IS NOT NULL AND merchant_uuid IS NULL\r\n"
					+ "                OR  PRODUCT_GROUP_ID IS NOT NULL AND PRODUCT_GROUP_UUID IS NULL\r\n"
					+ "                OR  ADMIN_ZONE_LEVEL_ID IS NOT NULL AND ADMIN_ZONE_LEVEL_UUID IS NULL\r\n"
					+ "                OR  EFFECTIVE_ZONE_LEVEL_ID IS NOT NULL AND EFFECTIVE_ZONE_LEVEL_UUID IS NULL\r\n"
					+ "                OR  AUTHORITY_TYPE_ID IS NOT NULL AND AUTHORITY_TYPE_UUID IS NULL\r\n"
					+ "                )";
			break;

		case "TB_AUTHORITIES_Referential_Validation":
			buildQuery =  "(\r\n"
					+ "          SELECT 'Not matching with ID', ta.name authority_name, tpg.name product_group, eff.name eff_zone_level, adm.name admin_zone_level\r\n"
					+ "              FROM tb_authorities ta \r\n"
					+ "              JOIN tb_zone_levels eff on eff.zone_level_id = ta.effective_zone_level_id\r\n"
					+ "              JOIN tb_zone_levels adm on adm.zone_level_id = ta.admin_zone_level_id\r\n"
					+ "              JOIN tb_product_groups tpg on tpg.product_group_id = ta.product_group_id\r\n"
					+ "              JOIN tb_merchants tm on tm.merchant_id = ta.merchant_id \r\n"
					+ "         WHERE tm.name = '" + merchantName + "' \r\n"
					+ "          MINUS\r\n"
					+ "          SELECT 'Not matching with ID', ta.name authority_name, tpg.name product_group, eff.name eff_zone_level, adm.name admin_zone_level\r\n"
					+ "              FROM tb_authorities ta \r\n"
					+ "              JOIN tb_zone_levels eff on eff.uuid = ta.effective_zone_level_uuid\r\n"
					+ "              JOIN tb_zone_levels adm on adm.uuid = ta.admin_zone_level_uuid\r\n"
					+ "              JOIN tb_product_groups tpg on tpg.uuid = ta.product_group_uuid\r\n"
					+ "              JOIN tb_merchants tm on tm.external_token = ta.merchant_uuid  \r\n"
					+ "          WHERE tm.name = '" + merchantName + "' \r\n"
					+ "          )\r\n"
					+ "          UNION\r\n"
					+ "          (\r\n"
					+ "          SELECT 'Not matching with UUID', ta.name authority_name, tpg.name product_group, eff.name eff_zone_level, adm.name admin_zone_level\r\n"
					+ "              FROM tb_authorities ta \r\n"
					+ "              JOIN tb_zone_levels eff on eff.uuid = ta.effective_zone_level_uuid\r\n"
					+ "              JOIN tb_zone_levels adm on adm.uuid = ta.admin_zone_level_uuid\r\n"
					+ "              JOIN tb_product_groups tpg on tpg.uuid = ta.product_group_uuid\r\n"
					+ "              JOIN tb_merchants tm on tm.external_token = ta.merchant_uuid  \r\n"
					+ "         WHERE tm.name = '" + merchantName + "' \r\n"
					+ "          MINUS\r\n"
					+ "          SELECT 'Not matching with UUID', ta.name authority_name, tpg.name product_group, eff.name eff_zone_level, adm.name admin_zone_level\r\n"
					+ "              FROM tb_authorities ta \r\n"
					+ "              JOIN tb_zone_levels eff on eff.zone_level_id = ta.effective_zone_level_id\r\n"
					+ "              JOIN tb_zone_levels adm on adm.zone_level_id = ta.admin_zone_level_id\r\n"
					+ "              JOIN tb_product_groups tpg on tpg.product_group_id = ta.product_group_id\r\n"
					+ "              JOIN tb_merchants tm on tm.merchant_id = ta.merchant_id \r\n"
					+ "          WHERE tm.name = '" + merchantName + "' \r\n"
					+ "          )";
			break;

		case "TB_AUTHORITY_LOGIC_ELEMENTS_UUID_Validation":
			buildQuery =  "SELECT * FROM TB_AUTHORITY_LOGIC_ELEMENTS \r\n"
					+ "          WHERE \r\n"
					+ "                (\r\n"
					+ "                    UUID IS NULL\r\n"
					+ "                OR  authority_logic_group_ID IS NOT NULL AND authority_logic_group_UUID IS NULL\r\n"
					+ "                )";	
			break;

		case "TB_AUTHORITY_LOGIC_ELEMENTS_Referential_Validation":
			buildQuery = "(\r\n"
					+ "          SELECT 'Not matching with UUID',tag.name, tale.condition, tale.SELECTor, tale.start_date, tale.end_date\r\n"
					+ "              FROM tb_authority_logic_elements tale JOIN tb_authority_logic_groups tag on tag.uuid = tale.authority_logic_group_uuid\r\n"
					+ "              JOIN tb_merchants tm on tm.external_token = tag.merchant_uuid\r\n"
					+ "          WHERE tm.name = '" + merchantName + "' \r\n"
					+ "          MINUS\r\n"
					+ "          SELECT 'Not matching with UUID',tag.name, tale.condition, tale.SELECTor, tale.start_date, tale.end_date\r\n"
					+ "              FROM tb_authority_logic_elements tale JOIN tb_authority_logic_groups tag on tag.authority_logic_group_id = tale.authority_logic_group_id\r\n"
					+ "              JOIN tb_merchants tm on tm.merchant_id = tag.merchant_id\r\n"
					+ "          WHERE tm.name = '" + merchantName + "' \r\n"
					+ "          )\r\n"
					+ "          UNION\r\n"
					+ "          (\r\n"
					+ "          SELECT 'Not matching with ID',tag.name, tale.condition, tale.SELECTor, tale.start_date, tale.end_date\r\n"
					+ "              FROM tb_authority_logic_elements tale JOIN tb_authority_logic_groups tag on tag.authority_logic_group_id = tale.authority_logic_group_id\r\n"
					+ "              JOIN tb_merchants tm on tm.merchant_id = tag.merchant_id\r\n"
					+ "          WHERE tm.name = '" + merchantName + "' \r\n"
					+ "          MINUS\r\n"
					+ "          SELECT 'Not matching with ID',tag.name, tale.condition, tale.SELECTor, tale.start_date, tale.end_date\r\n"
					+ "              FROM tb_authority_logic_elements tale JOIN tb_authority_logic_groups tag on tag.uuid = tale.authority_logic_group_uuid\r\n"
					+ "              JOIN tb_merchants tm on tm.external_token = tag.merchant_uuid\r\n"
					+ "          WHERE tm.name = '" + merchantName + "' \r\n"
					+ "          )";	
			break;

		case "TB_AUTHORITY_LOGIC_GROUP_XREF_UUID_Validation":
			buildQuery = "SELECT * FROM TB_AUTHORITY_LOGIC_GROUP_XREF\r\n"
					+ "          WHERE \r\n"
					+ "                (\r\n"
					+ "                    UUID IS NULL\r\n"
					+ "                OR  authority_logic_group_ID IS NOT NULL AND authority_logic_group_UUID IS NULL\r\n"
					+ "                )\r\n"
					+ "             and authority_id != 2000001703 --(placeholder record)";	
			break;

		case "TB_AUTHORITY_LOGIC_GROUP_XREF_Referential_Validation":
			buildQuery = "(\r\n"
					+ "          SELECT 'Not matching with UUID', ta.name authority_name, tag.name logic_group_name, tr.start_date, tr.end_date, tr.process_order\r\n"
					+ "           FROM tb_authority_logic_group_xref tr\r\n"
					+ "           JOIN tb_authorities ta on ta.uuid = tr.authority_uuid\r\n"
					+ "           JOIN tb_merchants tm on tm.external_token = ta.merchant_uuid\r\n"
					+ "           JOIN tb_authority_logic_groups tag on tag.uuid = tr.authority_logic_group_uuid\r\n"
					+ "          WHERE tm.name = '" + merchantName + "' \r\n"
					+ "          MINUS\r\n"
					+ "          SELECT 'Not matching with UUID', ta.name authority_name, tag.name logic_group_name, tr.start_date, tr.end_date, tr.process_order\r\n"
					+ "           FROM tb_authority_logic_group_xref tr\r\n"
					+ "           JOIN tb_authorities ta on ta.authority_id = tr.authority_id\r\n"
					+ "           JOIN tb_merchants tm on tm.merchant_id = ta.merchant_id\r\n"
					+ "           JOIN tb_authority_logic_groups tag on tag.authority_logic_group_id = tr.authority_logic_group_id\r\n"
					+ "          WHERE tm.name = '" + merchantName + "' \r\n"
					+ "          )\r\n"
					+ "          UNION\r\n"
					+ "          (\r\n"
					+ "          SELECT 'Not matching with ID', ta.name authority_name, tag.name logic_group_name, tr.start_date, tr.end_date, tr.process_order\r\n"
					+ "           FROM tb_authority_logic_group_xref tr\r\n"
					+ "           JOIN tb_authorities ta on ta.authority_id = tr.authority_id\r\n"
					+ "           JOIN tb_merchants tm on tm.merchant_id = ta.merchant_id\r\n"
					+ "           JOIN tb_authority_logic_groups tag on tag.authority_logic_group_id = tr.authority_logic_group_id\r\n"
					+ "          WHERE tm.name = '" + merchantName + "' \r\n"
					+ "          MINUS\r\n"
					+ "          SELECT 'Not matching with ID', ta.name authority_name, tag.name logic_group_name, tr.start_date, tr.end_date, tr.process_order\r\n"
					+ "           FROM tb_authority_logic_group_xref tr\r\n"
					+ "           JOIN tb_authorities ta on ta.uuid = tr.authority_uuid\r\n"
					+ "           JOIN tb_merchants tm on tm.external_token = ta.merchant_uuid\r\n"
					+ "           JOIN tb_authority_logic_groups tag on tag.uuid = tr.authority_logic_group_uuid\r\n"
					+ "          WHERE tm.name = '" + merchantName + "' \r\n"
					+ "          )";	
			break;

		case "TB_AUTHORITY_LOGIC_GROUPS_UUID_Validation":
			buildQuery = "SELECT * FROM TB_AUTHORITY_LOGIC_GROUPS\r\n"
					+ "          WHERE \r\n"
					+ "            Merchant_ID = " + merchantid + "\r\n"
					+ "            AND\r\n"
					+ "                (\r\n"
					+ "                    UUID IS NULL\r\n"
					+ "                OR  merchant_uuid IS NULL\r\n"
					+ "                )";	
			break;

		case "TB_AUTHORITY_LOGIC_GROUPS_Referential_Validation":
			buildQuery = "(\r\n"
					+ "          SELECT 'Not matching with ID', tag.name\r\n"
					+ "            FROM tb_authority_logic_groups tag JOIN tb_merchants tm on tm.merchant_id = tag.merchant_id\r\n"
					+ "           WHERE tm.name = '" + merchantName + "' \r\n"
					+ "          MINUS\r\n"
					+ "          SELECT 'Not matching with ID', tag.name\r\n"
					+ "            FROM tb_authority_logic_groups tag JOIN tb_merchants tm on tm.external_token = tag.merchant_uuid\r\n"
					+ "           WHERE tm.name = '" + merchantName + "' \r\n"
					+ "          )\r\n"
					+ "          UNION\r\n"
					+ "          (\r\n"
					+ "          SELECT 'Not matching with UUID', tag.name\r\n"
					+ "            FROM tb_authority_logic_groups tag JOIN tb_merchants tm on tm.external_token = tag.merchant_uuid\r\n"
					+ "           WHERE tm.name = '" + merchantName + "' \r\n"
					+ "          MINUS\r\n"
					+ "          SELECT 'Not matching with UUID', tag.name\r\n"
					+ "            FROM tb_authority_logic_groups tag JOIN tb_merchants tm on tm.merchant_id = tag.merchant_id\r\n"
					+ "           WHERE tm.name = '" + merchantName + "' \r\n"
					+ "          )";	
			break;			

			case "TB_AUTHORITY_REQUIREMENTS_UUID_Validation":
				buildQuery = "SELECT * FROM TB_AUTHORITY_REQUIREMENTS\r\n"
						+ "          WHERE \r\n"
						+ "            Merchant_ID = " + merchantid + "\r\n"
						+ "            AND\r\n"
						+ "                (\r\n"
						+ "                    UUID IS NULL\r\n"
						+ "                OR  merchant_uuid IS NULL\r\n"
						+ "                OR  authority_UUID IS NULL\r\n"
						+ "                )";	
				break;

			case "TB_AUTHORITY_REQUIREMENTS_Referential_Validation":
				buildQuery = "(\r\n"
						+ "          SELECT 'Not matching with UUID', tr.name auth_req_name, tr.start_date, tr.end_date, tr.condition, tr.value, ta.name authority_name\r\n"
						+ "           FROM tb_authority_requirements tr\r\n"
						+ "           JOIN tb_merchants tm on tm.external_token = tr.merchant_uuid\r\n"
						+ "           JOIN tb_authorities ta on ta.uuid = tr.authority_uuid\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with UUID', tr.name auth_req_name, tr.start_date, tr.end_date, tr.condition, tr.value, ta.name authority_name\r\n"
						+ "           FROM tb_authority_requirements tr\r\n"
						+ "           JOIN tb_merchants tm on tm.merchant_id = tr.merchant_id\r\n"
						+ "           JOIN tb_authorities ta on ta.authority_id = tr.authority_id\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          )\r\n"
						+ "          UNION\r\n"
						+ "          (\r\n"
						+ "          SELECT 'Not matching with ID', tr.name auth_req_name, tr.start_date, tr.end_date, tr.condition, tr.value, ta.name authority_name\r\n"
						+ "           FROM tb_authority_requirements tr\r\n"
						+ "           JOIN tb_merchants tm on tm.merchant_id = tr.merchant_id\r\n"
						+ "           JOIN tb_authorities ta on ta.authority_id = tr.authority_id\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with ID', tr.name auth_req_name, tr.start_date, tr.end_date, tr.condition, tr.value, ta.name authority_name\r\n"
						+ "           FROM tb_authority_requirements tr\r\n"
						+ "           JOIN tb_merchants tm on tm.external_token = tr.merchant_uuid\r\n"
						+ "           JOIN tb_authorities ta on ta.uuid = tr.authority_uuid\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          )";	
				break;

			case "TB_AUTHORITY_TYPES_UUID_Validation":
				buildQuery = "SELECT * FROM TB_AUTHORITY_TYPES\r\n"
						+ "          WHERE \r\n"
						+ "            Merchant_ID = " + merchantid + "\r\n"
						+ "            AND\r\n"
						+ "                (\r\n"
						+ "                    UUID IS NULL\r\n"
						+ "                OR  merchant_uuid IS NULL\r\n"
						+ "                )";	
				break;

			case "TB_AUTHORITY_TYPES_Referential_Validation":
				buildQuery = "(\r\n"
						+ "          SELECT 'Not matching with UUID', tm.name merchant_name, tat.name, tat.description\r\n"
						+ "            FROM tb_authority_types tat\r\n"
						+ "                 JOIN tb_merchants tm\r\n"
						+ "                     ON tm.external_token = tat.merchant_uuid\r\n"
						+ "           WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with UUID', tm.name merchant_name, tat.name, tat.description\r\n"
						+ "            FROM tb_authority_types tat\r\n"
						+ "                 JOIN tb_merchants tm\r\n"
						+ "                     ON tm.merchant_id = tat.merchant_id\r\n"
						+ "           WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          )\r\n"
						+ "          UNION\r\n"
						+ "          (\r\n"
						+ "          SELECT 'Not matching with ID',tm.name merchant_name, tat.name, tat.description\r\n"
						+ "            FROM tb_authority_types tat\r\n"
						+ "                 JOIN tb_merchants tm\r\n"
						+ "                     ON tm.merchant_id = tat.merchant_id\r\n"
						+ "           WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with ID',tm.name merchant_name, tat.name, tat.description\r\n"
						+ "            FROM tb_authority_types tat\r\n"
						+ "                 JOIN tb_merchants tm\r\n"
						+ "                     ON tm.external_token = tat.merchant_uuid\r\n"
						+ "           WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          )";	
				break;

			case "TB_COMP_AREA_AUTHORITIES_UUID_Validation":
				buildQuery = " SELECT * FROM TB_COMP_AREA_AUTHORITIES\r\n"
						+ "          WHERE \r\n"
						+ "                (\r\n"
						+ "                    UUID IS NULL\r\n"
						+ "                OR  Authority_uuid IS NULL\r\n"
						+ "                )";	
				break;

			case "TB_COMP_AREA_AUTHORITIES_Referential_Validation":
				buildQuery = "(\r\n"
						+ "          SELECT 'Not matching with UUID', ta.name authority_name, tm.name merchant, tca.name compliance_area, tzl.name effective_zone_level,\r\n"
						+ "                tca.associated_area_count, tca.start_date comp_start_date, tca.end_date comp_end_date\r\n"
						+ "           FROM tb_comp_area_authorities tcaa\r\n"
						+ "           JOIN tb_compliance_areas tca on tcaa.compliance_area_content_uuid = tca.uuid\r\n"
						+ "           JOIN tb_authorities ta on ta.uuid = tcaa.authority_uuid\r\n"
						+ "           JOIN tb_merchants tm on tca.merchant_uuid = tm.external_token\r\n"
						+ "           JOIN tb_zone_levels tzl on tzl.uuid = tca.effective_zone_level_uuid\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with UUID', ta.name authority_name, tm.name merchant, tca.name compliance_area, tzl.name effective_zone_level,\r\n"
						+ "                tca.associated_area_count, tca.start_date comp_start_date, tca.end_date comp_end_date\r\n"
						+ "           FROM tb_comp_area_authorities tcaa\r\n"
						+ "           JOIN tb_compliance_areas tca on tcaa.compliance_area_id = tca.compliance_area_id\r\n"
						+ "           JOIN tb_authorities ta on ta.authority_id = tcaa.authority_id\r\n"
						+ "           JOIN tb_merchants tm on tca.merchant_id = tm.merchant_id\r\n"
						+ "           JOIN tb_zone_levels tzl on tzl.zone_level_id = tca.effective_zone_level_id\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          )\r\n"
						+ "          UNION\r\n"
						+ "          (\r\n"
						+ "          SELECT 'Not matching with ID', ta.name authority_name, tm.name merchant, tca.name compliance_area, tzl.name effective_zone_level,\r\n"
						+ "                tca.associated_area_count, tca.start_date comp_start_date, tca.end_date comp_end_date\r\n"
						+ "           FROM tb_comp_area_authorities tcaa\r\n"
						+ "           JOIN tb_compliance_areas tca on tcaa.compliance_area_id = tca.compliance_area_id\r\n"
						+ "           JOIN tb_authorities ta on ta.authority_id = tcaa.authority_id\r\n"
						+ "           JOIN tb_merchants tm on tca.merchant_id = tm.merchant_id\r\n"
						+ "           JOIN tb_zone_levels tzl on tzl.zone_level_id = tca.effective_zone_level_id\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with ID', ta.name authority_name, tm.name merchant, tca.name compliance_area, tzl.name effective_zone_level,\r\n"
						+ "                tca.associated_area_count, tca.start_date comp_start_date, tca.end_date comp_end_date\r\n"
						+ "           FROM tb_comp_area_authorities tcaa\r\n"
						+ "           JOIN tb_compliance_areas tca on tcaa.compliance_area_content_uuid = tca.uuid\r\n"
						+ "           JOIN tb_authorities ta on ta.uuid = tcaa.authority_uuid\r\n"
						+ "           JOIN tb_merchants tm on tca.merchant_uuid = tm.external_token\r\n"
						+ "           JOIN tb_zone_levels tzl on tzl.uuid = tca.effective_zone_level_uuid\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          )";	
				break;

			case "TB_COMPLIANCE_AREAS_UUID_Validation":
				buildQuery =  "SELECT * FROM TB_COMPLIANCE_AREAS\r\n"
						+ "          WHERE \r\n"
						+ "            Merchant_ID = " + merchantid + "\r\n"
						+ "            AND\r\n"
						+ "                (\r\n"
						+ "                    UUID IS NULL\r\n"
						+ "                OR  merchant_uuid IS NULL\r\n"
						+ "                OR  effective_zone_level_id IS NOT NULL AND effective_zone_level_uuid IS NULL\r\n"
						+ "                )";
				break;

			case "TB_COMPLIANCE_AREAS_Referential_Validation":
				buildQuery =  "(\r\n"
						+ "          SELECT 'Not matching with UUID', tm.name merchant, tca.name compliance_area, tzl.name effective_zone_level, tca.associated_area_count, tca.start_date, tca.end_date\r\n"
						+ "           FROM tb_compliance_areas tca JOIN tb_merchants tm on tca.merchant_uuid = tm.external_token\r\n"
						+ "          JOIN tb_zone_levels tzl on tzl.uuid = tca.effective_zone_level_uuid\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with UUID', tm.name merchant, tca.name compliance_area, tzl.name effective_zone_level, tca.associated_area_count, tca.start_date, tca.end_date\r\n"
						+ "           FROM tb_compliance_areas tca JOIN tb_merchants tm on tca.merchant_id = tm.merchant_id\r\n"
						+ "          JOIN tb_zone_levels tzl on tzl.zone_level_id = tca.effective_zone_level_id\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          )\r\n"
						+ "          UNION\r\n"
						+ "          (\r\n"
						+ "          SELECT 'Not matching with ID', tm.name merchant, tca.name compliance_area, tzl.name effective_zone_level, tca.associated_area_count, tca.start_date, tca.end_date\r\n"
						+ "           FROM tb_compliance_areas tca JOIN tb_merchants tm on tca.merchant_id = tm.merchant_id\r\n"
						+ "          JOIN tb_zone_levels tzl on tzl.zone_level_id = tca.effective_zone_level_id\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with ID', tm.name merchant, tca.name compliance_area, tzl.name effective_zone_level, tca.associated_area_count, tca.start_date, tca.end_date\r\n"
						+ "           FROM tb_compliance_areas tca JOIN tb_merchants tm on tca.merchant_uuid = tm.external_token\r\n"
						+ "          JOIN tb_zone_levels tzl on tzl.uuid = tca.effective_zone_level_uuid\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          )";
				break;

			case "TB_CONTRIBUTING_AUTHORITIES_UUID_Validation":
				buildQuery =  "SELECT * FROM TB_CONTRIBUTING_AUTHORITIES\r\n"
						+ "          WHERE \r\n"
						+ "            Merchant_ID = " + merchantid + "\r\n"
						+ "            AND\r\n"
						+ "                (\r\n"
						+ "                    UUID IS NULL\r\n"
						+ "                OR  merchant_uuid IS NULL\r\n"
						+ "                OR  authority_ID IS NOT NULL AND authority_uuid IS NULL\r\n"
						+ "                OR this_authority_ID IS NOT NULL AND this_authority_UUID is NULL\r\n"
						+ "                )";
				break;

			case "TB_CONTRIBUTING_AUTHORITIES_Referential_Validation":
				buildQuery =  "(\r\n"
						+ "          SELECT 'Not matching with UUID', ta.name authority_name, ta_this.name this_authority_name, tca.basis_percent, tca.start_date, tca.end_date\r\n"
						+ "           FROM tb_contributing_authorities tca JOIN tb_merchants tm on tm.external_token = tca.merchant_uuid\r\n"
						+ "           JOIN tb_authorities ta on ta.uuid = tca.authority_uuid\r\n"
						+ "           JOIN tb_authorities ta_this on ta_this.uuid = tca.this_authority_uuid\r\n"
						+ "           JOIN tb_merchants tm on tm.external_token = tca.merchant_uuid\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with UUID', ta.name authority_name, ta_this.name this_authority_name, tca.basis_percent, tca.start_date, tca.end_date\r\n"
						+ "           FROM tb_contributing_authorities tca\r\n"
						+ "           JOIN tb_merchants tm on tm.merchant_id = tca.merchant_id\r\n"
						+ "           JOIN tb_authorities ta on ta.authority_id = tca.authority_id\r\n"
						+ "           JOIN tb_authorities ta_this on ta_this.authority_id = tca.this_authority_id\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          )\r\n"
						+ "          UNION\r\n"
						+ "          (\r\n"
						+ "          SELECT 'Not matching with ID', ta.name authority_name, ta_this.name this_authority_name, tca.basis_percent, tca.start_date, tca.end_date\r\n"
						+ "           FROM tb_contributing_authorities tca\r\n"
						+ "           JOIN tb_merchants tm on tm.merchant_id = tca.merchant_id\r\n"
						+ "           JOIN tb_authorities ta on ta.authority_id = tca.authority_id\r\n"
						+ "           JOIN tb_authorities ta_this on ta_this.authority_id = tca.this_authority_id\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with ID', ta.name authority_name, ta_this.name this_authority_name, tca.basis_percent, tca.start_date, tca.end_date\r\n"
						+ "           FROM tb_contributing_authorities tca\r\n"
						+ "           JOIN tb_merchants tm on tm.external_token = tca.merchant_uuid\r\n"
						+ "           JOIN tb_authorities ta on ta.uuid = tca.authority_uuid\r\n"
						+ "           JOIN tb_authorities ta_this on ta_this.uuid = tca.this_authority_uuid\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          )";
				break;

			case "TB_JE_MAPPINGS_UUID_Validation":
				buildQuery =  "SELECT * FROM TB_JE_MAPPINGS\r\n"
						+ "          WHERE \r\n"
						+ "                (\r\n"
						+ "                    UUID IS NULL\r\n"
						+ "                OR  authority_logic_group_id IS NOT NULL AND authority_logic_group_uuid IS NULL\r\n"
						+ "                OR  transaction_logic_path_id IS NOT NULL AND tran_logic_path_content_uuid IS NULL\r\n"
						+ "                )";
				break;

			case "TB_JE_MAPPINGS_Referential_Validation":
				buildQuery =  "(\r\n"
						+ "          SELECT 'Not matching with UUID', tag.name, tlp.transaction_type||''||tlp.direction||''||tlp.bulk_flag||''||tlp.origin_registered_flag||''||tlp.dest_registered_flag||''||tlp.point_of_title_transfer||''||tlp.start_date||''||tlp.end_date\r\n"
						+ "                 tdbodps, je.tax_type, je.tax_direction, je.start_date, je.end_date FROM tb_je_mappings je JOIN tb_authority_logic_groups tag on tag.uuid = je.authority_logic_group_uuid\r\n"
						+ "          JOIN tb_transaction_logic_paths tlp on tlp.uuid = je.tran_logic_path_content_uuid\r\n"
						+ "          JOIN tb_merchants tm on tm.external_token = je.merchant_uuid\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with UUID', tag.name, tlp.transaction_type||''||tlp.direction||''||tlp.bulk_flag||''||tlp.origin_registered_flag||''||tlp.dest_registered_flag||''||tlp.point_of_title_transfer||''||tlp.start_date||''||tlp.end_date\r\n"
						+ "                 tdbodps, je.tax_type, je.tax_direction, je.start_date, je.end_date\r\n"
						+ "          FROM tb_je_mappings je\r\n"
						+ "          JOIN tb_authority_logic_groups tag on tag.authority_logic_group_id = je.authority_logic_group_id\r\n"
						+ "          JOIN tb_transaction_logic_paths tlp on tlp.transaction_logic_path_id = je.transaction_logic_path_id\r\n"
						+ "          JOIN tb_merchants tm on tm.merchant_id = je.merchant_id\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          )\r\n"
						+ "          UNION\r\n"
						+ "          (\r\n"
						+ "          SELECT 'Not matching with ID', tag.name, tlp.transaction_type||''||tlp.direction||''||tlp.bulk_flag||''||tlp.origin_registered_flag||''||tlp.dest_registered_flag||''||tlp.point_of_title_transfer||''||tlp.start_date||''||tlp.end_date\r\n"
						+ "                 tdbodps, je.tax_type, je.tax_direction, je.start_date, je.end_date\r\n"
						+ "          FROM tb_je_mappings je\r\n"
						+ "          JOIN tb_authority_logic_groups tag on tag.authority_logic_group_id = je.authority_logic_group_id\r\n"
						+ "          JOIN tb_transaction_logic_paths tlp on tlp.transaction_logic_path_id = je.transaction_logic_path_id\r\n"
						+ "          JOIN tb_merchants tm on tm.merchant_id = je.merchant_id\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with ID', tag.name, tlp.transaction_type||''||tlp.direction||''||tlp.bulk_flag||''||tlp.origin_registered_flag||''||tlp.dest_registered_flag||''||tlp.point_of_title_transfer||''||tlp.start_date||''||tlp.end_date\r\n"
						+ "                 tdbodps, je.tax_type, je.tax_direction, je.start_date, je.end_date FROM tb_je_mappings je JOIN tb_authority_logic_groups tag on tag.uuid = je.authority_logic_group_uuid\r\n"
						+ "          JOIN tb_transaction_logic_paths tlp on tlp.uuid = je.tran_logic_path_content_uuid\r\n"
						+ "          JOIN tb_merchants tm on tm.external_token = je.merchant_uuid\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          )";
				break;

			case "TB_OPER_LIC_TYPE_MAPPINGS_UUID_Validation":
				buildQuery =  "SELECT * FROM TB_OPER_LIC_TYPE_MAPPINGS\r\n"
						+ "          WHERE \r\n"
						+ "            Merchant_ID = " + merchantid + "\r\n"
						+ "            AND\r\n"
						+ "                (\r\n"
						+ "                    UUID IS NULL\r\n"
						+ "                OR  Merchant_uuid IS NULL\r\n"
						+ "                OR  transaction_logic_path_id IS NOT NULL AND tran_logic_path_content_uuid IS NULL\r\n"
						+ "                OR  buyer_oper_lic_type_id IS NOT NULL AND buy_oper_lic_type_content_uuid IS NULL\r\n"
						+ "                OR  seller_oper_lic_type_id IS NOT NULL AND sel_oper_lic_type_content_uuid IS NULL\r\n"
						+ "                OR  authority_id IS NOT NULL AND authority_uuid IS NULL\r\n"
						+ "                OR  product_category_id IS NOT NULL AND product_category_uuid IS NULL\r\n"
						+ "                )";
				break;

			case "TB_OPER_LIC_TYPE_MAPPINGS_Referential_Validation":
				buildQuery =  "(\r\n"
						+ "          SELECT 'Not matching with UUID', ta.name, tpc.name product_name, bolt.name buyer, solt.name seller,\r\n"
						+ "                 oltm.tax_type, oltm.result_reason_text, oltm.legal_citation, oltm.start_date, oltm.end_date,\r\n"
						+ "                 tlp.transaction_type||''||tlp.direction||''||tlp.bulk_flag||''||tlp.origin_registered_flag||''||tlp.dest_registered_flag||''||tlp.point_of_title_transfer||''||tlp.start_date||''||tlp.end_date\r\n"
						+ "                 tdbodps\r\n"
						+ "            FROM tb_oper_lic_type_mappings oltm\r\n"
						+ "            JOIN tb_operating_license_types bolt on bolt.uuid = oltm.buy_oper_lic_type_content_uuid\r\n"
						+ "            JOIN tb_operating_license_types solt on solt.uuid = oltm.sel_oper_lic_type_content_uuid\r\n"
						+ "          left JOIN tb_product_categories tpc on tpc.uuid = oltm.product_category_uuid\r\n"
						+ "            JOIN tb_authorities ta on ta.uuid = oltm.authority_uuid\r\n"
						+ "              JOIN tb_merchants tm on tm.external_token = oltm.merchant_uuid\r\n"
						+ "            JOIN tb_transaction_logic_paths tlp on tlp.uuid = oltm.tran_logic_path_content_uuid\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with UUID', ta.name, tpc.name product_name, bolt.name buyer, solt.name seller,\r\n"
						+ "                 oltm.tax_type, oltm.result_reason_text, oltm.legal_citation, oltm.start_date, oltm.end_date,\r\n"
						+ "                 tlp.transaction_type||''||tlp.direction||''||tlp.bulk_flag||''||tlp.origin_registered_flag||''||tlp.dest_registered_flag||''||tlp.point_of_title_transfer||''||tlp.start_date||''||tlp.end_date\r\n"
						+ "                 tdbodps\r\n"
						+ "            FROM tb_oper_lic_type_mappings oltm\r\n"
						+ "            JOIN tb_operating_license_types bolt on bolt.operating_license_type_id = oltm.buyer_oper_lic_type_id\r\n"
						+ "            JOIN tb_operating_license_types solt on solt.operating_license_type_id = oltm.seller_oper_lic_type_id\r\n"
						+ "          left JOIN tb_product_categories tpc on tpc.product_category_id = oltm.product_category_id\r\n"
						+ "            JOIN tb_authorities ta on ta.authority_id = oltm.authority_id\r\n"
						+ "              JOIN tb_merchants tm on tm.merchant_id = oltm.merchant_id\r\n"
						+ "            JOIN tb_transaction_logic_paths tlp on tlp.transaction_logic_path_id = oltm.transaction_logic_path_id\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          )\r\n"
						+ "          UNION\r\n"
						+ "          (\r\n"
						+ "          SELECT 'Not matching with ID', ta.name, tpc.name product_name, bolt.name buyer, solt.name seller,\r\n"
						+ "                 oltm.tax_type, oltm.result_reason_text, oltm.legal_citation, oltm.start_date, oltm.end_date,\r\n"
						+ "                 tlp.transaction_type||''||tlp.direction||''||tlp.bulk_flag||''||tlp.origin_registered_flag||''||tlp.dest_registered_flag||''||tlp.point_of_title_transfer||''||tlp.start_date||''||tlp.end_date\r\n"
						+ "                 tdbodps\r\n"
						+ "            FROM tb_oper_lic_type_mappings oltm\r\n"
						+ "            JOIN tb_operating_license_types bolt on bolt.operating_license_type_id = oltm.buyer_oper_lic_type_id\r\n"
						+ "            JOIN tb_operating_license_types solt on solt.operating_license_type_id = oltm.seller_oper_lic_type_id\r\n"
						+ "          left JOIN tb_product_categories tpc on tpc.product_category_id = oltm.product_category_id\r\n"
						+ "            JOIN tb_authorities ta on ta.authority_id = oltm.authority_id\r\n"
						+ "              JOIN tb_merchants tm on tm.merchant_id = oltm.merchant_id\r\n"
						+ "            JOIN tb_transaction_logic_paths tlp on tlp.transaction_logic_path_id = oltm.transaction_logic_path_id\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with ID', ta.name, tpc.name product_name, bolt.name buyer, solt.name seller,\r\n"
						+ "                 oltm.tax_type, oltm.result_reason_text, oltm.legal_citation, oltm.start_date, oltm.end_date,\r\n"
						+ "                 tlp.transaction_type||''||tlp.direction||''||tlp.bulk_flag||''||tlp.origin_registered_flag||''||tlp.dest_registered_flag||''||tlp.point_of_title_transfer||''||tlp.start_date||''||tlp.end_date\r\n"
						+ "                 tdbodps\r\n"
						+ "            FROM tb_oper_lic_type_mappings oltm\r\n"
						+ "            JOIN tb_operating_license_types bolt on bolt.uuid = oltm.buy_oper_lic_type_content_uuid\r\n"
						+ "            JOIN tb_operating_license_types solt on solt.uuid = oltm.sel_oper_lic_type_content_uuid\r\n"
						+ "          left JOIN tb_product_categories tpc on tpc.uuid = oltm.product_category_uuid\r\n"
						+ "            JOIN tb_authorities ta on ta.uuid = oltm.authority_uuid\r\n"
						+ "              JOIN tb_merchants tm on tm.external_token = oltm.merchant_uuid\r\n"
						+ "            JOIN tb_transaction_logic_paths tlp on tlp.uuid = oltm.tran_logic_path_content_uuid\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          )";
				break;

			case "TB_OPERATING_LICENSE_TYPES_UUID_Validation":
				buildQuery =  "SELECT * FROM TB_OPERATING_LICENSE_TYPES\r\n"
						+ "          WHERE \r\n"
						+ "            Merchant_ID = " + merchantid + "\r\n"
						+ "            AND\r\n"
						+ "                (\r\n"
						+ "                    UUID IS NULL\r\n"
						+ "                OR  Merchant_uuid IS NULL\r\n"
						+ "                OR  zone_id IS NOT NULL AND zone_uuid IS NULL\r\n"
						+ "                )";
				break;

			case "TB_OPERATING_LICENSE_TYPES_Referential_Validation":
				buildQuery =  "(\r\n"
						+ "          SELECT 'Not matching with UUID', tol.name, description, start_date, end_date, tz.name zone_name\r\n"
						+ "            FROM tb_operating_license_types tol JOIN tb_merchants tm on tol.merchant_uuid = tm.external_token\r\n"
						+ "            JOIN tb_zones tz on tz.uuid = tol.zone_uuid\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with UUID', tol.name, description, start_date, end_date, tz.name zone_name\r\n"
						+ "            FROM tb_operating_license_types tol JOIN tb_merchants tm on tol.merchant_id = tm.merchant_id\r\n"
						+ "            JOIN tb_zones tz on tz.zone_id = tol.zone_id\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          )\r\n"
						+ "          UNION\r\n"
						+ "          (\r\n"
						+ "          SELECT 'Not matching with ID', tol.name, description, start_date, end_date, tz.name zone_name\r\n"
						+ "            FROM tb_operating_license_types tol JOIN tb_merchants tm on tol.merchant_id = tm.merchant_id\r\n"
						+ "            JOIN tb_zones tz on tz.zone_id = tol.zone_id\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with ID', tol.name, description, start_date, end_date, tz.name zone_name\r\n"
						+ "            FROM tb_operating_license_types tol JOIN tb_merchants tm on tol.merchant_uuid = tm.external_token\r\n"
						+ "            JOIN tb_zones tz on tz.uuid = tol.zone_uuid\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          )";
				break;

			case "TB_PRODUCT_CATEGORIES_UUID_Validation":
				buildQuery =  "SELECT * FROM TB_PRODUCT_CATEGORIES\r\n"
						+ "          WHERE \r\n"
						+ "            Merchant_ID = " + merchantid + "\r\n"
						+ "            AND\r\n"
						+ "                (\r\n"
						+ "                    UUID IS NULL\r\n"
						+ "                OR  Merchant_uuid IS NULL\r\n"
						+ "                OR  product_group_id IS NOT NULL AND product_group_uuid IS NULL\r\n"
						+ "                OR  Parent_product_category_id IS NOT NULL AND Parent_product_category_uuid IS NULL\r\n"
						+ "                )";
				break;

			case "TB_PRODUCT_CATEGORIES_Referential_Validation":
				buildQuery =  "(\r\n"
						+ "          SELECT 'Not matching with UUID', tpc.name, tpc.description, tpc.prodcode, tpc2.name parent_product, tpc2.prodcode parent_prodcode\r\n"
						+ "            FROM tb_product_categories tpc JOIN tb_product_groups tpg on tpg.uuid = tpc.product_group_uuid\r\n"
						+ "            JOIN tb_merchants tm on tm.external_token = tpc.merchant_uuid\r\n"
						+ "            JOIN tb_product_categories tpc2 on tpc2.uuid = tpc.parent_product_category_uuid\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with UUID', tpc.name, tpc.description, tpc.prodcode, tpc2.name parent_product, tpc2.prodcode parent_prodcode\r\n"
						+ "            FROM tb_product_categories tpc JOIN tb_product_groups tpg on tpg.product_group_id = tpc.product_group_id\r\n"
						+ "            JOIN tb_merchants tm on tm.merchant_id = tpc.merchant_id\r\n"
						+ "            JOIN tb_product_categories tpc2 on tpc2.product_category_id = tpc.parent_product_category_id\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          )\r\n"
						+ "          UNION\r\n"
						+ "          (\r\n"
						+ "          SELECT 'Not matching with ID', tpc.name, tpc.description, tpc.prodcode, tpc2.name parent_product, tpc2.prodcode parent_prodcode\r\n"
						+ "            FROM tb_product_categories tpc JOIN tb_product_groups tpg on tpg.product_group_id = tpc.product_group_id\r\n"
						+ "            JOIN tb_merchants tm on tm.merchant_id = tpc.merchant_id\r\n"
						+ "            JOIN tb_product_categories tpc2 on tpc2.product_category_id = tpc.parent_product_category_id\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with ID', tpc.name, tpc.description, tpc.prodcode, tpc2.name parent_product, tpc2.prodcode parent_prodcode\r\n"
						+ "            FROM tb_product_categories tpc JOIN tb_product_groups tpg on tpg.uuid = tpc.product_group_uuid\r\n"
						+ "            JOIN tb_merchants tm on tm.external_token = tpc.merchant_uuid\r\n"
						+ "            JOIN tb_product_categories tpc2 on tpc2.uuid = tpc.parent_product_category_uuid\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          )";
				break;

			case "TB_RATE_TIERS_UUID_Validation":
				buildQuery =  " SELECT * FROM TB_RATE_TIERS\r\n"
						+ "          WHERE \r\n"
						+ "                (\r\n"
						+ "                    UUID IS NULL\r\n"
						+ "                OR  rate_id IS NOT NULL AND rate_uuid IS NULL\r\n"
						+ "                )\r\n"
						+ "            and rate_tier_id != 2000001703";
				break;

			case "TB_RATE_TIERS_Referential_Validation":
				buildQuery =  "(\r\n"
						+ "          SELECT 'Not matching with UUID', ta.name, tr.rate_code, tr.start_date, nvl(tr.is_local, 'N') is_ocal, trt.rate, trt.flat_fee, trt.amount_low, trt.amount_high, trt.rate_code, trt.exempt, trt.amount_increment\r\n"
						+ "           FROM tb_rate_tiers trt JOIN tb_rates tr on tr.uuid = trt.rate_uuid\r\n"
						+ "          JOIN tb_merchants tm on tm.external_token = tr.merchant_uuid\r\n"
						+ "          JOIN tb_authorities ta on ta.uuid = tr.authority_uuid\r\n"
						+ "          WHERE tm.name = '" + merchantName + "'  and rate_tier_id != 2000001703\r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with UUID', ta.name, tr.rate_code, tr.start_date, nvl(tr.is_local, 'N') is_ocal, trt.rate, trt.flat_fee, trt.amount_low, trt.amount_high, trt.rate_code, trt.exempt, trt.amount_increment\r\n"
						+ "           FROM tb_rate_tiers trt JOIN tb_rates tr on tr.rate_id = trt.rate_id\r\n"
						+ "          JOIN tb_merchants tm on tm.merchant_id = tr.merchant_id\r\n"
						+ "          JOIN tb_authorities ta on ta.authority_id = tr.authority_id\r\n"
						+ "          WHERE tm.name = '" + merchantName + "'  and rate_tier_id != 2000001703\r\n"
						+ "          )\r\n"
						+ "          UNION\r\n"
						+ "          (\r\n"
						+ "          SELECT 'Not matching with ID', ta.name, tr.rate_code, tr.start_date, nvl(tr.is_local, 'N') is_ocal, trt.rate, trt.flat_fee, trt.amount_low, trt.amount_high, trt.rate_code, trt.exempt, trt.amount_increment\r\n"
						+ "           FROM tb_rate_tiers trt JOIN tb_rates tr on tr.rate_id = trt.rate_id\r\n"
						+ "          JOIN tb_merchants tm on tm.merchant_id = tr.merchant_id\r\n"
						+ "          JOIN tb_authorities ta on ta.authority_id = tr.authority_id\r\n"
						+ "          WHERE tm.name = '" + merchantName + "'  and rate_tier_id != 2000001703\r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with ID', ta.name, tr.rate_code, tr.start_date, nvl(tr.is_local, 'N') is_ocal, trt.rate, trt.flat_fee, trt.amount_low, trt.amount_high, trt.rate_code, trt.exempt, trt.amount_increment\r\n"
						+ "           FROM tb_rate_tiers trt JOIN tb_rates tr on tr.uuid = trt.rate_uuid\r\n"
						+ "          JOIN tb_merchants tm on tm.external_token = tr.merchant_uuid\r\n"
						+ "          JOIN tb_authorities ta on ta.uuid = tr.authority_uuid\r\n"
						+ "          WHERE tm.name = '" + merchantName + "'  and rate_tier_id != 2000001703\r\n"
						+ "          )";
				break;

			case "TB_RATES_UUID_Validation":
				buildQuery =  "SELECT * FROM TB_RATES\r\n"
						+ "          WHERE \r\n"
						+ "            Merchant_ID = " + merchantid + "\r\n"
						+ "            AND\r\n"
						+ "                (\r\n"
						+ "                    UUID IS NULL\r\n"
						+ "                OR  Merchant_uuid IS NULL\r\n"
						+ "                OR  authority_id IS NOT NULL AND authority_uuid IS NULL\r\n"
						+ "                OR  currency_id IS NOT NULL AND currency_uuid IS NULL\r\n"
						+ "                )";
				break;

			case "TB_RATES_Referential_Validation":
				buildQuery =  "(\r\n"
						+ "          SELECT 'Not matching with UUID', a.name, a.description, a.location_code, a.official_name, a.authority_category, aty.name \"Authority Type\", al.name \"Admin Level\", el.name \"Effective Level\",\r\n"
						+ "                 r.rate_code, r.description, r.rate, r.start_date, r.end_date, st.description \"Split Type\", sat.description \"Split Amount Type\", r.is_local \"Cascading\"\r\n"
						+ "            FROM tb_rates r\r\n"
						+ "            JOIN tb_authorities a\r\n"
						+ "                     ON (a.uuid = r.authority_uuid)\r\n"
						+ "            LEFT OUTER JOIN tb_lookups st\r\n"
						+ "                     ON (    st.code_group = 'SPLIT_TYPE'\r\n"
						+ "                         AND st.code = NVL (r.split_type, 'x'))\r\n"
						+ "             LEFT OUTER JOIN tb_lookups sat\r\n"
						+ "                     ON (    sat.code_group = 'SPLIT_AMT_TYPE'\r\n"
						+ "                         AND sat.code = NVL (r.split_amount_type, 'x'))\r\n"
						+ "             LEFT OUTER JOIN tb_authority_types aty\r\n"
						+ "                     ON (aty.uuid = a.authority_type_uuid)\r\n"
						+ "             LEFT OUTER JOIN tb_zone_levels al\r\n"
						+ "                     ON (al.uuid = a.admin_zone_level_uuid)\r\n"
						+ "             LEFT OUTER JOIN tb_zone_levels el\r\n"
						+ "                     ON (el.uuid = a.effective_zone_level_uuid)\r\n"
						+ "           WHERE r.merchant_uuid = (SELECT external_token FROM tb_merchants WHERE name = '" + merchantName + "' )\r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with UUID', a.name, a.description, a.location_code, a.official_name, a.authority_category, aty.name \"Authority Type\", al.name \"Admin Level\", el.name \"Effective Level\",\r\n"
						+ "                 r.rate_code, r.description, r.rate, r.start_date, r.end_date, st.description \"Split Type\", sat.description \"Split Amount Type\", r.is_local \"Cascading\"\r\n"
						+ "            FROM tb_rates r\r\n"
						+ "            JOIN tb_authorities a\r\n"
						+ "                     ON (a.authority_id = r.authority_id)\r\n"
						+ "            LEFT OUTER JOIN tb_lookups st\r\n"
						+ "                     ON (    st.code_group = 'SPLIT_TYPE'\r\n"
						+ "                         AND st.code = NVL (r.split_type, 'x'))\r\n"
						+ "             LEFT OUTER JOIN tb_lookups sat\r\n"
						+ "                     ON (    sat.code_group = 'SPLIT_AMT_TYPE'\r\n"
						+ "                         AND sat.code = NVL (r.split_amount_type, 'x'))\r\n"
						+ "             LEFT OUTER JOIN tb_authority_types aty\r\n"
						+ "                     ON (aty.authority_type_id = a.authority_type_id)\r\n"
						+ "             LEFT OUTER JOIN tb_zone_levels al\r\n"
						+ "                     ON (al.zone_level_id = a.admin_zone_level_id)\r\n"
						+ "             LEFT OUTER JOIN tb_zone_levels el\r\n"
						+ "                     ON (el.zone_level_id = a.effective_zone_level_id)\r\n"
						+ "           WHERE r.merchant_id = (SELECT merchant_id FROM tb_merchants WHERE name = '" + merchantName + "' )\r\n"
						+ "          )\r\n"
						+ "          UNION\r\n"
						+ "          (\r\n"
						+ "          SELECT 'Not matching with ID', a.name, a.description, a.location_code, a.official_name, a.authority_category, aty.name \"Authority Type\", al.name \"Admin Level\", el.name \"Effective Level\",\r\n"
						+ "                 r.rate_code, r.description, r.rate, r.start_date, r.end_date, st.description \"Split Type\", sat.description \"Split Amount Type\", r.is_local \"Cascading\"\r\n"
						+ "            FROM tb_rates r\r\n"
						+ "            JOIN tb_authorities a\r\n"
						+ "                     ON (a.authority_id = r.authority_id)\r\n"
						+ "            LEFT OUTER JOIN tb_lookups st\r\n"
						+ "                     ON (    st.code_group = 'SPLIT_TYPE'\r\n"
						+ "                         AND st.code = NVL (r.split_type, 'x'))\r\n"
						+ "             LEFT OUTER JOIN tb_lookups sat\r\n"
						+ "                     ON (    sat.code_group = 'SPLIT_AMT_TYPE'\r\n"
						+ "                         AND sat.code = NVL (r.split_amount_type, 'x'))\r\n"
						+ "             LEFT OUTER JOIN tb_authority_types aty\r\n"
						+ "                     ON (aty.authority_type_id = a.authority_type_id)\r\n"
						+ "             LEFT OUTER JOIN tb_zone_levels al\r\n"
						+ "                     ON (al.zone_level_id = a.admin_zone_level_id)\r\n"
						+ "             LEFT OUTER JOIN tb_zone_levels el\r\n"
						+ "                     ON (el.zone_level_id = a.effective_zone_level_id)\r\n"
						+ "           WHERE r.merchant_id = (SELECT merchant_id FROM tb_merchants WHERE name = '" + merchantName + "' )\r\n"
						+ "           MINUS\r\n"
						+ "           SELECT 'Not matching with ID', a.name, a.description, a.location_code, a.official_name, a.authority_category, aty.name \"Authority Type\", al.name \"Admin Level\", el.name \"Effective Level\",\r\n"
						+ "                 r.rate_code, r.description, r.rate, r.start_date, r.end_date, st.description \"Split Type\", sat.description \"Split Amount Type\", r.is_local \"Cascading\"\r\n"
						+ "            FROM tb_rates r\r\n"
						+ "            JOIN tb_authorities a\r\n"
						+ "                     ON (a.uuid = r.authority_uuid)\r\n"
						+ "            LEFT OUTER JOIN tb_lookups st\r\n"
						+ "                     ON (    st.code_group = 'SPLIT_TYPE'\r\n"
						+ "                         AND st.code = NVL (r.split_type, 'x'))\r\n"
						+ "             LEFT OUTER JOIN tb_lookups sat\r\n"
						+ "                     ON (    sat.code_group = 'SPLIT_AMT_TYPE'\r\n"
						+ "                         AND sat.code = NVL (r.split_amount_type, 'x'))\r\n"
						+ "             LEFT OUTER JOIN tb_authority_types aty\r\n"
						+ "                     ON (aty.uuid = a.authority_type_uuid)\r\n"
						+ "             LEFT OUTER JOIN tb_zone_levels al\r\n"
						+ "                     ON (al.uuid = a.admin_zone_level_uuid)\r\n"
						+ "             LEFT OUTER JOIN tb_zone_levels el\r\n"
						+ "                     ON (el.uuid = a.effective_zone_level_uuid)\r\n"
						+ "           WHERE r.merchant_uuid = (SELECT external_token FROM tb_merchants WHERE name = '" + merchantName + "' )\r\n"
						+ "           )";
				break;

			case "TB_REFERENCE_LISTS_UUID_Validation":
				buildQuery =  "SELECT * FROM TB_REFERENCE_LISTS\r\n"
						+ "          WHERE \r\n"
						+ "            Merchant_ID = " + merchantid + "\r\n"
						+ "            AND\r\n"
						+ "                (\r\n"
						+ "                    UUID IS NULL\r\n"
						+ "                OR  Merchant_uuid IS NULL\r\n"
						+ "                )";
				break;

			case "TB_REFERENCE_LISTS_Referential_Validation":
				buildQuery =  "(\r\n"
						+ "          SELECT 'Not matching with UUID', trl.name, trl.description, trl.start_date, trl.end_date\r\n"
						+ "           FROM tb_reference_lists trl JOIN tb_merchants tm on tm.external_token = trl.merchant_uuid\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with UUID', trl.name, trl.description, trl.start_date, trl.end_date\r\n"
						+ "           FROM tb_reference_lists trl JOIN tb_merchants tm on tm.merchant_id = trl.merchant_id\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          )\r\n"
						+ "          UNION\r\n"
						+ "          (\r\n"
						+ "          SELECT 'Not matching with ID', trl.name, trl.description, trl.start_date, trl.end_date\r\n"
						+ "           FROM tb_reference_lists trl JOIN tb_merchants tm on tm.merchant_id = trl.merchant_id\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with ID', trl.name, trl.description, trl.start_date, trl.end_date\r\n"
						+ "           FROM tb_reference_lists trl JOIN tb_merchants tm on tm.external_token = trl.merchant_uuid\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          )";
				break;

			case "TB_REFERENCE_VALUES_UUID_Validation":
				buildQuery =  " SELECT * FROM TB_REFERENCE_VALUES\r\n"
						+ "          WHERE \r\n"
						+ "                (\r\n"
						+ "                    UUID IS NULL\r\n"
						+ "                OR  reference_list_id IS NOT NULL AND reference_list_uuid IS NULL AND reference_list_id in (SELECT reference_list_id FROM tb_reference_lists WHERE merchant_id = " + merchantid + ")\r\n"
						+ "                )\r\n"
						+ "            and reference_value_id != 2000001703";
				break;

			case "TB_REFERENCE_VALUES_Referential_Validation":
				buildQuery =  "(\r\n"
						+ "          SELECT 'Not matching with UUID', trl.name ref_list_name, trv.value ref_value_name, trv.start_date, trv.end_date\r\n"
						+ "            FROM tb_reference_values trv JOIN tb_reference_lists trl on trv.reference_list_uuid = trl.uuid\r\n"
						+ "            JOIN tb_merchants tm on tm.external_token = trl.merchant_uuid\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with UUID', trl.name ref_list_name, trv.value ref_value_name, trv.start_date, trv.end_date\r\n"
						+ "            FROM tb_reference_values trv JOIN tb_reference_lists trl on trv.reference_list_id = trl.reference_list_id\r\n"
						+ "            JOIN tb_merchants tm on tm.merchant_id = trl.merchant_id\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          )\r\n"
						+ "          UNION\r\n"
						+ "          (\r\n"
						+ "          SELECT 'Not matching with ID', trl.name ref_list_name, trv.value ref_value_name, trv.start_date, trv.end_date\r\n"
						+ "            FROM tb_reference_values trv JOIN tb_reference_lists trl on trv.reference_list_id = trl.reference_list_id\r\n"
						+ "            JOIN tb_merchants tm on tm.merchant_id = trl.merchant_id\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with ID', trl.name ref_list_name, trv.value ref_value_name, trv.start_date, trv.end_date\r\n"
						+ "            FROM tb_reference_values trv JOIN tb_reference_lists trl on trv.reference_list_uuid = trl.uuid\r\n"
						+ "            JOIN tb_merchants tm on tm.external_token = trl.merchant_uuid\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          )";
				break;

			case "TB_RULE_QUALIFIERS_UUID_Validation":
				buildQuery =  "SELECT * FROM TB_RULE_QUALIFIERS\r\n"
						+ "          WHERE \r\n"
						+ "            rule_id IN (SELECT rule_id FROM TB_RULES WHERE merchant_id = " + merchantid + ")\r\n"
						+ "            AND\r\n"
						+ "                (\r\n"
						+ "                    UUID IS NULL \r\n"
						+ "                OR  reference_list_id IS NOT NULL AND reference_list_uuid IS NULL \r\n"
						+ "                OR  authority_id IS NOT NULL AND authority_uuid IS NULL\r\n"
						+ "                OR rule_uuid IS NULL\r\n"
						+ "                )";
				break;

			case "TB_RULE_QUALIFIERS_Referential_Validation":
				buildQuery =  "(\r\n"
						+ "          SELECT 'Not matching with UUID', ta.name authority_name, tr.rule_order, tr.start_date, nvl(tr.is_local, 'N') is_local,\r\n"
						+ "          trq.rule_qualifier_type, trq.element, trq.operator, trq.value, trq.start_date, trq.end_date, rl.name, ta2.name\r\n"
						+ "          FROM tb_rule_qualifiers trq JOIN tb_rules tr on tr.uuid = trq.rule_uuid\r\n"
						+ "          JOIN tb_authorities ta on ta.uuid = tr.authority_uuid\r\n"
						+ "          JOIN tb_merchants tm on tm.external_token = tr.merchant_uuid\r\n"
						+ "          left JOIN tb_reference_lists rl on rl.uuid = trq.reference_list_uuid\r\n"
						+ "          left JOIN tb_authorities ta2 on ta2.uuid = trq.authority_uuid\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with UUID', ta.name authority_name, tr.rule_order, tr.start_date, nvl(tr.is_local, 'N') is_local,\r\n"
						+ "          trq.rule_qualifier_type, trq.element, trq.operator, trq.value, trq.start_date, trq.end_date, rl.name, ta2.name\r\n"
						+ "          FROM tb_rule_qualifiers trq\r\n"
						+ "          JOIN tb_rules tr on tr.rule_id = trq.rule_id\r\n"
						+ "          JOIN tb_authorities ta on ta.authority_id = tr.authority_id\r\n"
						+ "          JOIN tb_merchants tm on tm.merchant_id = tr.merchant_id\r\n"
						+ "          left JOIN tb_reference_lists rl on rl.reference_list_id = trq.reference_list_id\r\n"
						+ "          left JOIN tb_authorities ta2 on ta2.authority_id = trq.authority_id\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          )\r\n"
						+ "          UNION\r\n"
						+ "          (\r\n"
						+ "          SELECT 'Not matching with ID', ta.name authority_name, tr.rule_order, tr.start_date, nvl(tr.is_local, 'N') is_local,\r\n"
						+ "          trq.rule_qualifier_type, trq.element, trq.operator, trq.value, trq.start_date, trq.end_date, rl.name, ta2.name\r\n"
						+ "          FROM tb_rule_qualifiers trq\r\n"
						+ "          JOIN tb_rules tr on tr.rule_id = trq.rule_id\r\n"
						+ "          JOIN tb_authorities ta on ta.authority_id = tr.authority_id\r\n"
						+ "          JOIN tb_merchants tm on tm.merchant_id = tr.merchant_id\r\n"
						+ "          left JOIN tb_reference_lists rl on rl.reference_list_id = trq.reference_list_id\r\n"
						+ "          left JOIN tb_authorities ta2 on ta2.authority_id = trq.authority_id\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with ID', ta.name authority_name, tr.rule_order, tr.start_date, nvl(tr.is_local, 'N') is_local,\r\n"
						+ "          trq.rule_qualifier_type, trq.element, trq.operator, trq.value, trq.start_date, trq.end_date, rl.name, ta2.name\r\n"
						+ "          FROM tb_rule_qualifiers trq JOIN tb_rules tr on tr.uuid = trq.rule_uuid\r\n"
						+ "          JOIN tb_authorities ta on ta.uuid = tr.authority_uuid\r\n"
						+ "          JOIN tb_merchants tm on tm.external_token = tr.merchant_uuid\r\n"
						+ "          left JOIN tb_reference_lists rl on rl.uuid = trq.reference_list_uuid\r\n"
						+ "          left JOIN tb_authorities ta2 on ta2.uuid = trq.authority_uuid\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          )";
				break;

			case "TB_RULES_UUID_Validation":
				buildQuery =  " SELECT * FROM TB_RULES\r\n"
						+ "          WHERE \r\n"
						+ "            Merchant_ID = " + merchantid + "\r\n"
						+ "            AND\r\n"
						+ "                (\r\n"
						+ "                    UUID IS NULL \r\n"
						+ "                OR  Merchant_UUID IS NULL\r\n"
						+ "                OR  product_category_uuid IS NULL AND product_category_id IS NOT NULL\r\n"
						+ "                OR  authority_id IS NOT NULL AND authority_uuid IS NULL\r\n"
						+ "                OR  local_authority_type_id IS NOT NULL AND local_authority_type_uuid IS NULL\r\n"
						+ "                OR  material_set_list_id IS NOT NULL AND material_set_list_uuid IS NULL\r\n"
						+ "                OR  authority_rate_set_id IS NOT NULL AND authority_rate_set_uuid IS NULL                \r\n"
						+ "                )";
				break;

			case "TB_RULES_Referential_Validation":
				buildQuery =  "(\r\n"
						+ "          SELECT DISTINCT 'Not matching with UUID', ta.name authority_name, a.rule_order, a.start_date, a.is_local cascading, a.end_date, pt.name product_name, pt.prodcode commodity_code, tpg.name product_group,\r\n"
						+ "                 a.input_recovery_amount input_recovery_amt, a.input_recovery_percent input_recovery_pct, a.rule_comment, a.exempt_reason_code, allocated_charge\r\n"
						+ "            FROM tb_rules a\r\n"
						+ "                 LEFT JOIN tb_product_categories pt\r\n"
						+ "                     ON (a.product_category_uuid = pt.uuid )\r\n"
						+ "                 LEFT JOIN tb_product_groups tpg\r\n"
						+ "                     ON (tpg.uuid = pt.product_group_uuid)\r\n"
						+ "                 LEFT JOIN tb_lookups c\r\n"
						+ "                     ON (a.calculation_method = c.code\r\n"
						+ "                         AND c.code_group = 'TBI_CALC_METH')\r\n"
						+ "                JOIN tb_authorities ta ON (ta.uuid = a.authority_uuid)\r\n"
						+ "                 LEFT JOIN tb_lookups d\r\n"
						+ "                     ON (a.tax_type = d.code AND d.code_group = 'US_TAX_TYPE')\r\n"
						+ "            WHERE a.merchant_uuid = ( SELECT external_token FROM tb_merchants WHERE name = '" + merchantName + "' )\r\n"
						+ "          MINUS\r\n"
						+ "          SELECT DISTINCT 'Not matching with UUID', ta.name authority_name, a.rule_order, a.start_date, a.is_local cascading, a.end_date, pt.name product_name, pt.prodcode commodity_code, tpg.name product_group,\r\n"
						+ "\r\n"
						+ "                 a.input_recovery_amount input_recovery_amt, a.input_recovery_percent input_recovery_pct, a.rule_comment, a.exempt_reason_code, allocated_charge\r\n"
						+ "            FROM tb_rules a\r\n"
						+ "                 LEFT JOIN tb_product_categories pt\r\n"
						+ "                     ON (a.product_category_id = pt.product_category_id )\r\n"
						+ "                 LEFT JOIN tb_product_groups tpg\r\n"
						+ "                     ON (tpg.product_group_id = pt.product_group_id)\r\n"
						+ "                 LEFT JOIN tb_lookups c\r\n"
						+ "                     ON (a.calculation_method = c.code\r\n"
						+ "                         AND c.code_group = 'TBI_CALC_METH')\r\n"
						+ "                JOIN tb_authorities ta ON (ta.authority_id = a.authority_id)\r\n"
						+ "                 LEFT JOIN tb_lookups d\r\n"
						+ "                     ON (a.tax_type = d.code AND d.code_group = 'US_TAX_TYPE')\r\n"
						+ "                WHERE a.merchant_id = ( SELECT merchant_id FROM tb_merchants WHERE name = '" + merchantName + "' )\r\n"
						+ "          )\r\n"
						+ "          UNION\r\n"
						+ "          (\r\n"
						+ "          SELECT DISTINCT 'Not matching with ID', ta.name authority_name, a.rule_order, a.start_date, a.is_local cascading, a.end_date, pt.name product_name, pt.prodcode commodity_code, tpg.name product_group,\r\n"
						+ "\r\n"
						+ "                 a.input_recovery_amount input_recovery_amt, a.input_recovery_percent input_recovery_pct, a.rule_comment, a.exempt_reason_code, allocated_charge\r\n"
						+ "            FROM tb_rules a\r\n"
						+ "                 LEFT JOIN tb_product_categories pt\r\n"
						+ "                     ON (a.product_category_id = pt.product_category_id )\r\n"
						+ "                 LEFT JOIN tb_product_groups tpg\r\n"
						+ "                     ON (tpg.product_group_id = pt.product_group_id)\r\n"
						+ "                 LEFT JOIN tb_lookups c\r\n"
						+ "                     ON (a.calculation_method = c.code\r\n"
						+ "                         AND c.code_group = 'TBI_CALC_METH')\r\n"
						+ "                JOIN tb_authorities ta ON (ta.authority_id = a.authority_id)\r\n"
						+ "                 LEFT JOIN tb_lookups d\r\n"
						+ "                     ON (a.tax_type = d.code AND d.code_group = 'US_TAX_TYPE')\r\n"
						+ "                WHERE a.merchant_id = ( SELECT merchant_id FROM tb_merchants WHERE name = '" + merchantName + "' )\r\n"
						+ "          MINUS\r\n"
						+ "          SELECT DISTINCT 'Not matching with ID', ta.name authority_name, a.rule_order, a.start_date, a.is_local cascading, a.end_date, pt.name product_name, pt.prodcode commodity_code, tpg.name product_group,\r\n"
						+ "\r\n"
						+ "                 a.input_recovery_amount input_recovery_amt, a.input_recovery_percent input_recovery_pct, a.rule_comment, a.exempt_reason_code, allocated_charge\r\n"
						+ "            FROM tb_rules a\r\n"
						+ "                 LEFT JOIN tb_product_categories pt\r\n"
						+ "                     ON (a.product_category_uuid = pt.uuid )\r\n"
						+ "                 LEFT JOIN tb_product_groups tpg\r\n"
						+ "                     ON (tpg.uuid = pt.product_group_uuid)\r\n"
						+ "                 LEFT JOIN tb_lookups c\r\n"
						+ "                     ON (a.calculation_method = c.code\r\n"
						+ "                         AND c.code_group = 'TBI_CALC_METH')\r\n"
						+ "                JOIN tb_authorities ta ON (ta.uuid = a.authority_uuid)\r\n"
						+ "                 LEFT JOIN tb_lookups d\r\n"
						+ "                     ON (a.tax_type = d.code AND d.code_group = 'US_TAX_TYPE')\r\n"
						+ "                WHERE a.merchant_uuid = ( SELECT external_token FROM tb_merchants WHERE name = '" + merchantName + "' )\r\n"
						+ "          )";
				break;
				
			 case "TB_RULE_OUTPUTS_UUID_Validation":
			        buildQuery = "(\n" +
			                "  SELECT 'Not matching with UUID' AS msg,\n" +
			                "         ta.name,\n" +
			                "         tr.rule_order,\n" +
			                "         tr.start_date AS rule_start_date,\n" +
			                "         tr.end_date AS rule_end_date,\n" +
			                "         tro.value AS rule_output_value,\n" +
			                "         tro.start_date AS rule_output_start_date,\n" +
			                "         tro.end_date AS rule_output_end_date\n" +
			                "  FROM tb_rule_outputs tro\n" +
			                "       JOIN tb_rules tr ON tr.uuid = tro.rule_uuid\n" +
			                "       JOIN tb_merchants tm ON tm.external_token = tr.merchant_uuid\n" +
			                "       JOIN tb_authorities ta ON ta.uuid = tr.authority_uuid\n" +
			                "  WHERE tm.name = '" + merchantName + "' \n" +
			                "    AND tr.rule_id IN (\n" +
			                "                        SELECT DISTINCT primary_key\n" +
			                "                        FROM tb_content_journal\n" +
			                "                        WHERE table_name = 'TB_RULES'\n" +
			                "                            AND operation = 'U'\n" +
			                "                            AND operation_date >= '02-Feb-2023'\n" +
			                "                      )\n" +
			                "  MINUS\n" +
			                "  SELECT 'Not matching with UUID' AS msg,\n" +
			                "         ta.name,\n" +
			                "         tr.rule_order,\n" +
			                "         tr.start_date AS rule_start_date,\n" +
			                "         tr.end_date AS rule_end_date,\n" +
			                "         tro.value AS rule_output_value,\n" +
			                "         tro.start_date AS rule_output_start_date,\n" +
			                "         tro.end_date AS rule_output_end_date\n" +
			                "  FROM tb_rule_outputs tro\n" +
			                "       JOIN tb_rules tr ON tr.rule_id = tro.rule_id\n" +
			                "       JOIN tb_merchants tm ON tm.merchant_id = tr.merchant_id\n" +
			                "       JOIN tb_authorities ta ON ta.authority_id = tr.authority_id\n" +
			                "  WHERE tm.name = '" + merchantName + "' \n" +
			                "    AND tr.rule_id IN (\n" +
			                "                        SELECT DISTINCT primary_key\n" +
			                "                        FROM tb_content_journal\n" +
			                "                        WHERE table_name = 'TB_RULES'\n" +
			                "                            AND operation = 'U'\n" +
			                "                            AND operation_date >= '02-Feb-2023'\n" +
			                "                      )\n" +
			                ")\n" +
			                "UNION\n" +
			                "(\n" +
			                "  SELECT 'Not matching with ID' AS msg,\n" +
			                "         ta.name,\n" +
			                "         tr.rule_order,\n" +
			                "         tr.start_date AS rule_start_date,\n" +
			                "         tr.end_date AS rule_end_date,\n" +
			                "         tro.value AS rule_output_value,\n" +
			                "         tro.start_date AS rule_output_start_date,\n" +
			                "         tro.end_date AS rule_output_end_date\n" +
			                "  FROM tb_rule_outputs tro\n" +
			                "       JOIN tb_rules tr ON tr.rule_id = tro.rule_id\n" +
			                "       JOIN tb_merchants tm ON tm.merchant_id = tr.merchant_id\n" +
			                "       JOIN tb_authorities ta ON ta.authority_id = tr.authority_id\n" +
			                "  WHERE tm.name = '" + merchantName + "' \n" +
			                "    AND tr.rule_id IN (\n" +
			                "                        SELECT DISTINCT primary_key\n" +
			                "                        FROM tb_content_journal\n" +
			                "                        WHERE table_name = 'TB_RULES'\n" +
			                "                            AND operation = 'U'\n" +
			                "                            AND operation_date >= '02-Feb-2023'\n" +
			                "                      )\n" +
			                "  MINUS\n" +
			                "  SELECT 'Not matching with ID' AS msg,\n" +
			                "         ta.name,\n" +
			                "         tr.rule_order,\n" +
			                "         tr.start_date AS rule_start_date,\n" +
			                "         tr.end_date AS rule_end_date,\n" +
			                "         tro.value AS rule_output_value,\n" +
			                "         tro.start_date AS rule_output_start_date,\n" +
			                "         tro.end_date AS rule_output_end_date\n" +
			                "  FROM tb_rule_outputs tro\n" +
			                "       JOIN tb_rules tr ON tr.uuid = tro.rule_uuid\n" +
			                "       JOIN tb_merchants tm ON tm.external_token = tr.merchant_uuid\n" +
			                "       JOIN tb_authorities ta ON ta.uuid = tr.authority_uuid\n" +
			                "  WHERE tm.name = '" + merchantName + "' \n" +
			                "    AND tr.rule_id IN (\n" +
			                "                        SELECT DISTINCT primary_key\n" +
			                "                        FROM tb_content_journal\n" +
			                "                        WHERE table_name = 'TB_RULES'\n" +
			                "                            AND operation = 'U'\n" +
			                "                            AND operation_date >= '02-Feb-2023'\n" +
			                "                      )\n" +
			                ")";
			        break;

			case "TB_TRANSACTION_LOGIC_PATHS_UUID_Validation":
				buildQuery =  "SELECT * FROM TB_TRANSACTION_LOGIC_PATHS\r\n"
						+ "          WHERE \r\n"
						+ "            Merchant_ID = " + merchantid + "\r\n"
						+ "            AND\r\n"
						+ "                (\r\n"
						+ "                    UUID IS NULL \r\n"
						+ "                OR  Merchant_UUID IS NULL              \r\n"
						+ "                )";
				break;

			case "TB_TRANSACTION_LOGIC_PATHS_Referential_Validation":
				buildQuery =  "(\r\n"
						+ "          SELECT 'Not matching with UUID', tlp.transaction_type, tlp.direction, tlp.bulk_flag, tlp.origin_registered_flag, tlp.dest_registered_flag,\r\n"
						+ "                 tlp.point_of_title_transfer, tlp.start_date, tlp.end_date\r\n"
						+ "          FROM tb_transaction_logic_paths tlp\r\n"
						+ "          JOIN tb_merchants tm on tm.external_token = tlp.merchant_uuid\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with UUID', tlp.transaction_type, tlp.direction, tlp.bulk_flag, tlp.origin_registered_flag, tlp.dest_registered_flag,\r\n"
						+ "                 tlp.point_of_title_transfer, tlp.start_date, tlp.end_date\r\n"
						+ "          FROM tb_transaction_logic_paths tlp\r\n"
						+ "          JOIN tb_merchants tm on tm.merchant_id = tlp.merchant_id\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          )\r\n"
						+ "          UNION\r\n"
						+ "          (\r\n"
						+ "          SELECT 'Not matching with UUID', tlp.transaction_type, tlp.direction, tlp.bulk_flag, tlp.origin_registered_flag, tlp.dest_registered_flag,\r\n"
						+ "                 tlp.point_of_title_transfer, tlp.start_date, tlp.end_date\r\n"
						+ "          FROM tb_transaction_logic_paths tlp\r\n"
						+ "          JOIN tb_merchants tm on tm.merchant_id = tlp.merchant_id\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with UUID', tlp.transaction_type, tlp.direction, tlp.bulk_flag, tlp.origin_registered_flag, tlp.dest_registered_flag,\r\n"
						+ "                 tlp.point_of_title_transfer, tlp.start_date, tlp.end_date\r\n"
						+ "          FROM tb_transaction_logic_paths tlp\r\n"
						+ "          JOIN tb_merchants tm on tm.external_token = tlp.merchant_uuid\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          )";
				break;

			case "TB_TRANSPORTATION_TYPES_UUID_Validation":
				buildQuery =  "SELECT * FROM TB_TRANSPORTATION_TYPES\r\n"
						+ "          WHERE \r\n"
						+ "            Merchant_ID = " + merchantid + "\r\n"
						+ "            AND\r\n"
						+ "                (\r\n"
						+ "                    UUID IS NULL \r\n"
						+ "                OR  Merchant_UUID IS NULL               \r\n"
						+ "                )";
				break;

			case "TB_TRANSPORTATION_TYPES_Referential_Validation":
				buildQuery =  "(\r\n"
						+ "          SELECT 'Not matching with UUID', ttl.name, ttl.description, ttl.bulk_flag, ttl.start_date, ttl.end_date\r\n"
						+ "           FROM tb_transportation_types ttl JOIN tb_merchants tm on tm.external_token = ttl.merchant_uuid\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with UUID', ttl.name, ttl.description, ttl.bulk_flag, ttl.start_date, ttl.end_date\r\n"
						+ "           FROM tb_transportation_types ttl JOIN tb_merchants tm on tm.merchant_id = ttl.merchant_id\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          )\r\n"
						+ "          UNION\r\n"
						+ "          (\r\n"
						+ "          SELECT 'Not matching with ID', ttl.name, ttl.description, ttl.bulk_flag, ttl.start_date, ttl.end_date\r\n"
						+ "           FROM tb_transportation_types ttl JOIN tb_merchants tm on tm.merchant_id = ttl.merchant_id\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with ID', ttl.name, ttl.description, ttl.bulk_flag, ttl.start_date, ttl.end_date\r\n"
						+ "           FROM tb_transportation_types ttl JOIN tb_merchants tm on tm.external_token = ttl.merchant_uuid\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          )";
				break;

			case "TB_UNIQUE_AREA_AUTHORITIES_UUID_Validation":
				buildQuery =  "SELECT * FROM TB_UNIQUE_AREA_AUTHORITIES\r\n"
						+ "          WHERE \r\n"
						+ "          --  Merchant_ID = " + merchantid + "\r\n"
						+ "          --  AND\r\n"
						+ "                (\r\n"
						+ "                    UUID IS NULL \r\n"
						+ "                OR  unique_area_id IS NOT NULL AND unique_area_content_uuid IS NULL\r\n"
						+ "                OR  authority_id IS NOT NULL AND authority_uuid IS NULL\r\n"
						+ "                )";
				break;

			case "TB_UNIQUE_AREA_AUTHORITIES_Referential_Validation":
				buildQuery =  "(\r\n"
						+ "          SELECT 'Not matching with UUID', ta.name, tua.uaid, tua.area_zone, tca.name compliance_area\r\n"
						+ "            FROM tb_unique_area_authorities tuaa\r\n"
						+ "            JOIN tb_unique_areas tua on tuaa.unique_area_content_uuid = tua.uuid\r\n"
						+ "            JOIN tb_authorities ta on ta.uuid = tuaa.authority_uuid\r\n"
						+ "            JOIN tb_merchants tm on tm.external_token = tua.merchant_uuid\r\n"
						+ "            JOIN tb_compliance_areas tca on tca.uuid = tua.compliance_area_content_uuid\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with UUID', ta.name, tua.uaid, tua.area_zone, tca.name compliance_area\r\n"
						+ "            FROM tb_unique_area_authorities tuaa\r\n"
						+ "            JOIN tb_unique_areas tua on tuaa.unique_area_id = tua.unique_area_id\r\n"
						+ "            JOIN tb_authorities ta on ta.authority_id = tuaa.authority_id\r\n"
						+ "            JOIN tb_merchants tm on tm.merchant_id = tua.merchant_id\r\n"
						+ "            JOIN tb_compliance_areas tca on tca.compliance_area_id = tua.compliance_area_id\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          )\r\n"
						+ "          UNION\r\n"
						+ "          (\r\n"
						+ "          SELECT 'Not matching with ID', ta.name, tua.uaid, tua.area_zone, tca.name compliance_area\r\n"
						+ "            FROM tb_unique_area_authorities tuaa\r\n"
						+ "            JOIN tb_unique_areas tua on tuaa.unique_area_id = tua.unique_area_id\r\n"
						+ "            JOIN tb_authorities ta on ta.authority_id = tuaa.authority_id\r\n"
						+ "            JOIN tb_merchants tm on tm.merchant_id = tua.merchant_id\r\n"
						+ "            JOIN tb_compliance_areas tca on tca.compliance_area_id = tua.compliance_area_id\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with ID', ta.name, tua.uaid, tua.area_zone, tca.name compliance_area\r\n"
						+ "            FROM tb_unique_area_authorities tuaa\r\n"
						+ "            JOIN tb_unique_areas tua on tuaa.unique_area_content_uuid = tua.uuid\r\n"
						+ "            JOIN tb_authorities ta on ta.uuid = tuaa.authority_uuid\r\n"
						+ "            JOIN tb_merchants tm on tm.external_token = tua.merchant_uuid\r\n"
						+ "            JOIN tb_compliance_areas tca on tca.uuid = tua.compliance_area_content_uuid\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          )";
				break;

			case "TB_UNIQUE_AREAS_UUID_Validation":
				buildQuery =  "SELECT * FROM TB_UNIQUE_AREAS\r\n"
						+ "          WHERE \r\n"
						+ "            Merchant_ID = " + merchantid + "\r\n"
						+ "            AND\r\n"
						+ "                (\r\n"
						+ "                    UUID IS NULL \r\n"
						+ "                OR  COMPLIANCE_AREA_ID IS NOT NULL AND COMPLIANCE_AREA_CONTENT_UUID IS NULL\r\n"
						+ "                OR  merchant_uuid IS NULL\r\n"
						+ "                )";
				break;

			case "TB_UNIQUE_AREAS_Referential_Validation":
				buildQuery =  "(\r\n"
						+ "          SELECT 'Not matching with UUID', tua.uaid, tua.area_zone, tca.name compliance_area\r\n"
						+ "            FROM tb_unique_areas tua\r\n"
						+ "            JOIN tb_merchants tm on tm.external_token = tua.merchant_uuid\r\n"
						+ "            JOIN tb_compliance_areas tca on tca.uuid = tua.compliance_area_content_uuid\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with UUID', tua.uaid, tua.area_zone, tca.name compliance_area\r\n"
						+ "            FROM tb_unique_areas tua\r\n"
						+ "            JOIN tb_merchants tm on tm.merchant_id = tua.merchant_id\r\n"
						+ "            JOIN tb_compliance_areas tca on tca.compliance_area_id = tua.compliance_area_id\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          )\r\n"
						+ "          UNION\r\n"
						+ "          (\r\n"
						+ "          SELECT 'Not matching with ID', tua.uaid, tua.area_zone, tca.name compliance_area\r\n"
						+ "            FROM tb_unique_areas tua\r\n"
						+ "            JOIN tb_merchants tm on tm.merchant_id = tua.merchant_id\r\n"
						+ "            JOIN tb_compliance_areas tca on tca.compliance_area_id = tua.compliance_area_id\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with ID', tua.uaid, tua.area_zone, tca.name compliance_area\r\n"
						+ "            FROM tb_unique_areas tua\r\n"
						+ "            JOIN tb_merchants tm on tm.external_token = tua.merchant_uuid\r\n"
						+ "            JOIN tb_compliance_areas tca on tca.uuid = tua.compliance_area_content_uuid\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          )";
				break;

			case "TB_ZONE_AUTHORITIES_UUID_Validation":
				buildQuery =  "SELECT * FROM TB_ZONE_AUTHORITIES\r\n"
						+ "          WHERE \r\n"
						+ "            zone_id IN (SELECT zone_id FROM TB_ZONES WHERE merchant_id = " + merchantid + ")\r\n"
						+ "            AND\r\n"
						+ "                (\r\n"
						+ "                    UUID IS NULL \r\n"
						+ "                OR  Zone_ID IS NOT NULL AND Zone_uuid IS NULL \r\n"
						+ "                OR  authority_id IS NOT NULL AND authority_uuid IS NULL\r\n"
						+ "                )";
				break;

			case "TB_ZONE_AUTHORITIES_Referential_Validation":
				buildQuery =  "(\r\n"
						+ "          SELECT 'Not matching with ID', ta.name authority_name, tz.name zone_name, tz3.name tax_parent_zone, tz2.name parent_zone, tzl.name zone_level, tz.eu_zone_as_of_date, tz.code_2char, tz.code_3char, tz.code_iso,\r\n"
						+ "                 tz.code_fips, tz.reverse_flag, tz.terminator_flag, tz.default_flag, tz.range_min, tz.range_max, tz.eu_exit_date, tz.gcc_as_of_date, tz.gcc_exit_date\r\n"
						+ "           FROM tb_zone_authorities tza\r\n"
						+ "           JOIN tb_zones tz on tza.zone_id = tz.zone_id\r\n"
						+ "           JOIN tb_authorities ta on ta.authority_id = tza.authority_id\r\n"
						+ "           JOIN tb_merchants tm on tm.merchant_id = tz.merchant_id\r\n"
						+ "           JOIN tb_zones tz2 on tz2.zone_id =  tz.parent_zone_id\r\n"
						+ "           JOIN tb_zone_levels tzl on tzl.zone_level_id = tz.zone_level_id\r\n"
						+ "          left JOIN tb_zones tz3 on tz3.zone_id = tz.tax_parent_zone_id\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with ID', ta.name authority_name, tz.name zone_name, tz3.name tax_parent_zone, tz2.name parent_zone, tzl.name zone_level, tz.eu_zone_as_of_date, tz.code_2char, tz.code_3char, tz.code_iso,\r\n"
						+ "                 tz.code_fips, tz.reverse_flag, tz.terminator_flag, tz.default_flag, tz.range_min, tz.range_max, tz.eu_exit_date, tz.gcc_as_of_date, tz.gcc_exit_date\r\n"
						+ "           FROM tb_zone_authorities tza\r\n"
						+ "           JOIN tb_zones tz on tza.zone_uuid = tz.uuid\r\n"
						+ "           JOIN tb_authorities ta on ta.uuid = tza.authority_uuid\r\n"
						+ "           JOIN tb_merchants tm on tm.external_token = tz.merchant_uuid\r\n"
						+ "           JOIN tb_zones tz2 on tz2.uuid =  tz.parent_zone_uuid\r\n"
						+ "           JOIN tb_zone_levels tzl on tzl.uuid = tz.zone_level_uuid\r\n"
						+ "          left JOIN tb_zones tz3 on tz3.uuid = tz.tax_parent_zone_uuid\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          )\r\n"
						+ "          UNION\r\n"
						+ "          (\r\n"
						+ "          SELECT 'Not matching with UUID', ta.name authority_name, tz.name zone_name, tz3.name tax_parent_zone, tz2.name parent_zone, tzl.name zone_level, tz.eu_zone_as_of_date, tz.code_2char, tz.code_3char, tz.code_iso,\r\n"
						+ "                 tz.code_fips, tz.reverse_flag, tz.terminator_flag, tz.default_flag, tz.range_min, tz.range_max, tz.eu_exit_date, tz.gcc_as_of_date, tz.gcc_exit_date\r\n"
						+ "           FROM tb_zone_authorities tza\r\n"
						+ "           JOIN tb_zones tz on tza.zone_uuid = tz.uuid\r\n"
						+ "           JOIN tb_authorities ta on ta.uuid = tza.authority_uuid\r\n"
						+ "           JOIN tb_merchants tm on tm.external_token = tz.merchant_uuid\r\n"
						+ "           JOIN tb_zones tz2 on tz2.uuid =  tz.parent_zone_uuid\r\n"
						+ "           JOIN tb_zone_levels tzl on tzl.uuid = tz.zone_level_uuid\r\n"
						+ "          left JOIN tb_zones tz3 on tz3.uuid = tz.tax_parent_zone_uuid\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with UUID', ta.name authority_name, tz.name zone_name, tz3.name tax_parent_zone, tz2.name parent_zone, tzl.name zone_level, tz.eu_zone_as_of_date, tz.code_2char, tz.code_3char, tz.code_iso,\r\n"
						+ "                 tz.code_fips, tz.reverse_flag, tz.terminator_flag, tz.default_flag, tz.range_min, tz.range_max, tz.eu_exit_date, tz.gcc_as_of_date, tz.gcc_exit_date\r\n"
						+ "           FROM tb_zone_authorities tza\r\n"
						+ "           JOIN tb_zones tz on tza.zone_id = tz.zone_id\r\n"
						+ "           JOIN tb_authorities ta on ta.authority_id = tza.authority_id\r\n"
						+ "           JOIN tb_merchants tm on tm.merchant_id = tz.merchant_id\r\n"
						+ "           JOIN tb_zones tz2 on tz2.zone_id =  tz.parent_zone_id\r\n"
						+ "           JOIN tb_zone_levels tzl on tzl.zone_level_id = tz.zone_level_id\r\n"
						+ "          left JOIN tb_zones tz3 on tz3.zone_id = tz.tax_parent_zone_id\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          )";
				break;

			case "TB_ZONE_MATCH_CONTEXTS_UUID_Validation":
				buildQuery =  "SELECT * FROM TB_ZONE_MATCH_CONTEXTS\r\n"
						+ "          WHERE \r\n"
						+ "            zone_id IN (SELECT zone_id FROM TB_ZONES WHERE merchant_id = " + merchantid + ")\r\n"
						+ "            AND\r\n"
						+ "                (\r\n"
						+ "                    UUID IS NULL \r\n"
						+ "                OR  ZONE_MATCH_PATTERN_ID IS NOT NULL AND ZONE_MATCH_PATTERN_UUID IS NULL\r\n"
						+ "                OR  ZONE_LEVEL_ID IS NOT NULL AND ZONE_LEVEL_UUID IS NULL\r\n"
						+ "                OR  ZONE_ID IS NOT NULL AND ZONE_UUID IS NULL\r\n"
						+ "                )";
				break;

			case "TB_ZONE_MATCH_CONTEXTS_Referential_Validation":
				buildQuery =  "(\r\n"
						+ "          SELECT 'Not matching with ID', tzl.name zone_level, tz.name zone_name, tp.pattern, tp.value, tp.type\r\n"
						+ "          FROM tb_zone_match_contexts a JOIN tb_zone_levels tzl on a.zone_level_id = tzl.zone_level_id\r\n"
						+ "          JOIN tb_zones tz on tz.zone_id = a.zone_id\r\n"
						+ "          JOIN tb_zone_match_patterns tp on tp.zone_match_pattern_id = a.zone_match_pattern_id\r\n"
						+ "          JOIN tb_merchants tm on tm.merchant_id = tz.merchant_id AND tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with ID', tzl.name zone_level, tz.name zone_name, tp.pattern, tp.value, tp.type\r\n"
						+ "          FROM tb_zone_match_contexts a\r\n"
						+ "          JOIN tb_zone_levels tzl on a.zone_level_uuid = tzl.uuid\r\n"
						+ "          JOIN tb_zones tz on tz.uuid = a.zone_uuid\r\n"
						+ "          JOIN tb_zone_match_patterns tp on tp.uuid = a.zone_match_pattern_uuid\r\n"
						+ "          JOIN tb_merchants tm on tm.external_token = tz.merchant_uuid AND tm.name = '" + merchantName + "' \r\n"
						+ "          )\r\n"
						+ "          UNION\r\n"
						+ "          (\r\n"
						+ "          SELECT 'Not matching with UUID', tzl.name zone_level, tz.name zone_name, tp.pattern, tp.value, tp.type\r\n"
						+ "          FROM tb_zone_match_contexts a\r\n"
						+ "          JOIN tb_zone_levels tzl on a.zone_level_uuid = tzl.uuid\r\n"
						+ "          JOIN tb_zones tz on tz.uuid = a.zone_uuid\r\n"
						+ "          JOIN tb_zone_match_patterns tp on tp.uuid = a.zone_match_pattern_uuid\r\n"
						+ "          JOIN tb_merchants tm on tm.external_token = tz.merchant_uuid AND tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with UUID', tzl.name zone_level, tz.name zone_name, tp.pattern, tp.value, tp.type\r\n"
						+ "          FROM tb_zone_match_contexts a JOIN tb_zone_levels tzl on a.zone_level_id = tzl.zone_level_id\r\n"
						+ "          JOIN tb_zones tz on tz.zone_id = a.zone_id\r\n"
						+ "          JOIN tb_zone_match_patterns tp on tp.zone_match_pattern_id = a.zone_match_pattern_id\r\n"
						+ "          JOIN tb_merchants tm on tm.merchant_id = tz.merchant_id AND tm.name = '" + merchantName + "' \r\n"
						+ "          )";
				break;

			case "TB_ZONE_MATCH_PATTERNS_UUID_Validation":
				buildQuery =  "SELECT * FROM TB_ZONE_MATCH_PATTERNS\r\n"
						+ "          WHERE \r\n"
						+ "            merchant_id = " + merchantid + "\r\n"
						+ "            AND\r\n"
						+ "                (\r\n"
						+ "                    UUID IS NULL \r\n"
						+ "                OR  MERCHANT_UUID IS NULL\r\n"
						+ "                )";
				break;

			case "TB_ZONE_MATCH_PATTERNS_Referential_Validation":
				buildQuery =  "(\r\n"
						+ "          SELECT 'Not matching with ID', pattern, value, type FROM tb_zone_match_patterns zp JOIN tb_merchants tm on tm.merchant_id = zp.merchant_id AND tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with ID', pattern, value, type FROM tb_zone_match_patterns zp JOIN tb_merchants tm on tm.external_token = zp.merchant_uuid AND tm.name = '" + merchantName + "' \r\n"
						+ "          )\r\n"
						+ "          UNION\r\n"
						+ "          (\r\n"
						+ "          SELECT 'Not matching with UUID', pattern, value, type FROM tb_zone_match_patterns zp JOIN tb_merchants tm on tm.external_token = zp.merchant_uuid AND tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with UUID', pattern, value, type FROM tb_zone_match_patterns zp JOIN tb_merchants tm on tm.merchant_id = zp.merchant_id AND tm.name = '" + merchantName + "' \r\n"
						+ "          )";
				break;

			case "TB_ZONES_UUID_Validation":
				buildQuery =  "SELECT *\n" +
			            "FROM tb_zones\n" +
			            "WHERE merchant_id = " + merchantid + "\n" +
			            "    AND NAME != 'WORLD'\n" +
			            "    AND (\n" +
			            "        uuid IS NULL\n" +
			            "        OR merchant_uuid IS NULL\n" +
			            "        OR parent_zone_id IS NOT NULL AND parent_zone_uuid IS NULL\n" +
			            "        OR tax_parent_zone_id IS NOT NULL AND tax_parent_zone_uuid IS NULL\n" +
			            "        OR zone_level_id IS NOT NULL AND zone_level_uuid IS NULL\n" +
			            "    );";
				break;

			case "TB_ZONES_Referential_Validation":
				buildQuery =  "(\r\n"
						+ "          SELECT 'Not matching with ID', tz.name zone_name, tz3.name tax_parent_zone, tz2.name parent_zone, tzl.name zone_level, tz.eu_zone_as_of_date, tz.code_2char, tz.code_3char, tz.code_iso,\r\n"
						+ "                 tz.code_fips, tz.reverse_flag, tz.terminator_flag, tz.default_flag, tz.range_min, tz.range_max, tz.eu_exit_date, tz.gcc_as_of_date, tz.gcc_exit_date\r\n"
						+ "           FROM tb_zones tz JOIN tb_merchants tm on tm.merchant_id = tz.merchant_id\r\n"
						+ "          JOIN tb_zones tz2 on tz2.zone_id =  tz.parent_zone_id\r\n"
						+ "          JOIN tb_zone_levels tzl on tzl.zone_level_id = tz.zone_level_id\r\n"
						+ "          left JOIN tb_zones tz3 on tz3.zone_id = tz.tax_parent_zone_id\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with ID', tz.name zone_name, tz3.name tax_parent_zone, tz2.name parent_zone, tzl.name zone_level, tz.eu_zone_as_of_date, tz.code_2char, tz.code_3char, tz.code_iso,\r\n"
						+ "                 tz.code_fips, tz.reverse_flag, tz.terminator_flag, tz.default_flag, tz.range_min, tz.range_max, tz.eu_exit_date, tz.gcc_as_of_date, tz.gcc_exit_date\r\n"
						+ "           FROM tb_zones tz JOIN tb_merchants tm on tm.external_token = tz.merchant_uuid\r\n"
						+ "          JOIN tb_zones tz2 on tz2.uuid =  tz.parent_zone_uuid\r\n"
						+ "          JOIN tb_zone_levels tzl on tzl.uuid = tz.zone_level_uuid\r\n"
						+ "          left JOIN tb_zones tz3 on tz3.uuid = tz.tax_parent_zone_uuid\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          )\r\n"
						+ "          UNION\r\n"
						+ "          (\r\n"
						+ "          SELECT 'Not matching with UUID', tz.name zone_name, tz3.name tax_parent_zone, tz2.name parent_zone, tzl.name zone_level, tz.eu_zone_as_of_date, tz.code_2char, tz.code_3char, tz.code_iso,\r\n"
						+ "                 tz.code_fips, tz.reverse_flag, tz.terminator_flag, tz.default_flag, tz.range_min, tz.range_max, tz.eu_exit_date, tz.gcc_as_of_date, tz.gcc_exit_date\r\n"
						+ "           FROM tb_zones tz JOIN tb_merchants tm on tm.external_token = tz.merchant_uuid\r\n"
						+ "          JOIN tb_zones tz2 on tz2.uuid =  tz.parent_zone_uuid\r\n"
						+ "          JOIN tb_zone_levels tzl on tzl.uuid = tz.zone_level_uuid\r\n"
						+ "          left JOIN tb_zones tz3 on tz3.uuid = tz.tax_parent_zone_uuid\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          MINUS\r\n"
						+ "          SELECT 'Not matching with UUID', tz.name zone_name, tz3.name tax_parent_zone, tz2.name parent_zone, tzl.name zone_level, tz.eu_zone_as_of_date, tz.code_2char, tz.code_3char, tz.code_iso,\r\n"
						+ "                 tz.code_fips, tz.reverse_flag, tz.terminator_flag, tz.default_flag, tz.range_min, tz.range_max, tz.eu_exit_date, tz.gcc_as_of_date, tz.gcc_exit_date\r\n"
						+ "           FROM tb_zones tz JOIN tb_merchants tm on tm.merchant_id = tz.merchant_id\r\n"
						+ "          JOIN tb_zones tz2 on tz2.zone_id =  tz.parent_zone_id\r\n"
						+ "          JOIN tb_zone_levels tzl on tzl.zone_level_id = tz.zone_level_id\r\n"
						+ "          left JOIN tb_zones tz3 on tz3.zone_id = tz.tax_parent_zone_id\r\n"
						+ "          WHERE tm.name = '" + merchantName + "' \r\n"
						+ "          )";
				break;	

			default:
				System.out.println("Default query executed");
				buildQuery = "Unknown entity: " + entityName;
				break;
		}
		return buildQuery;
	}

	public static void createEntityAndNullCountSheet(Connection connection, Sheet entitySheet,
			List<String> entities, String schemaName, String merchantName, int merchantId) {

		// Define column widths for formatting
	    int tableNameColumnWidth = 40;
	    int uuidValidationColumnWidth = 20;
	    int referentialValidationColumnWidth = 20;

	    // Print headers to the console with proper formatting
	    System.out.printf("%-" + tableNameColumnWidth + "s%-" + uuidValidationColumnWidth + "s  %-" + referentialValidationColumnWidth + "s%n",
	            "Table Name", "Null UUID Validation", "Referential Validation");


		// Use LinkedHashMap to maintain the order of entities
		Map<String, Integer> tableNameToUUIDCount = new LinkedHashMap<>();
		Map<String, Integer> tableNameToReferentialCount = new LinkedHashMap<>();

		// Initialize counts for all table names
		for (String entity : entities) {
			String tableName = extractTableName(entity);
			tableNameToUUIDCount.put(tableName, 0);
			tableNameToReferentialCount.put(tableName, 0);
		}

		for (String entity : entities) {
			String nullUUIDQuery = GenerateQueryStatement(entity, merchantName, merchantId);

			int nullUUIDCount = 0;

			try (PreparedStatement nullUUIDStatement = connection.prepareStatement(nullUUIDQuery);
					ResultSet nullUUIDResultSet = nullUUIDStatement.executeQuery()) {

				while (nullUUIDResultSet.next()) {
					nullUUIDCount++;
				}

				String tableName = extractTableName(entity);
				// Remove any single quotes
				entity = entity.replace("'", "").trim();

				// Determine if it's UUID or Referential validation and update the respective count
				if (entity.endsWith("_UUID_Validation")) {
					tableNameToUUIDCount.put(tableName, tableNameToUUIDCount.get(tableName) + nullUUIDCount);
				} else if (entity.endsWith("_Referential_Validation")) {
					tableNameToReferentialCount.put(tableName, tableNameToReferentialCount.get(tableName) + nullUUIDCount);
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		// Create a flag to track if the header row in sheet2 has been written
		boolean headerRowWritten = false;

		// Iterate over the map and print the table name and counts to the console
		for (String tableName : tableNameToUUIDCount.keySet()) {
			int uuidValidationCount = tableNameToUUIDCount.get(tableName);
			int referentialValidationCount = tableNameToReferentialCount.get(tableName);

			 // Print with proper formatting
			System.out.printf("%-" + tableNameColumnWidth + "s%-" + uuidValidationColumnWidth + "d  %-" + referentialValidationColumnWidth + "d%n",
			        tableName, uuidValidationCount, referentialValidationCount);


			// Check if we are processing the first entity and header row has not been written in entitySheet
			if (!headerRowWritten) {
				// Create headers for the Excel sheet at row 0
				Row headerRow = entitySheet.createRow(0); //first row in excel sheet2
				createCell(headerRow, 0, "TABLE NAME");
				createCell(headerRow, 1, "UUID Validation");
				createCell(headerRow, 2, "REFERENTIAL Validation");

				// Set the flag to true to indicate that the header row has been written
				headerRowWritten = true;
			}

			// Keep track of the current row number for this table
			int rowNum = entitySheet.getLastRowNum() + 1;

			// Create data rows in the Excel sheet
			Row dataRow = entitySheet.createRow(rowNum++);
			createCell(dataRow, 0, tableName);
			createCell(dataRow, 1, String.valueOf(uuidValidationCount));
			createCell(dataRow, 2, String.valueOf(referentialValidationCount));
		}
	}

	// Helper method to extract the table name
	private static String extractTableName(String entity) {
		// Remove any single quotes, trim whitespace and spilt the entity
		String[] parts = entity.replace("'", "").trim().split("_(UUID|Referential)_Validation$");
		return parts[0];

	}

	public static void handleEntityForNullUUIDs(Connection connection, String entity, Sheet currentSheet, String schemaName, String merchantName, int merchantId) throws SQLException {
		String query = GenerateQueryStatement(entity, merchantName, merchantId);
		int nullUUIDCount = 0;

		try (PreparedStatement preparedStatement = connection.prepareStatement(query);
				ResultSet resultSet = preparedStatement.executeQuery()) {

			ResultSetMetaData rsMetaData = resultSet.getMetaData();
			int count = rsMetaData.getColumnCount();

			// Check if there are any data rows
			boolean hasDataRows = resultSet.next();

			if (hasDataRows) {
				// Create a row to display the entity name
				Row entityNameRow = currentSheet.createRow(currentSheet.getLastRowNum() + 1);
				createCell(entityNameRow, 0, "TABLE NAME: " + entity);

				// Create a row for column headers for each entity
				Row headerRow = currentSheet.createRow(currentSheet.getLastRowNum() + 1);
				for (int j = 1; j <= count; j++) {
					createCell(headerRow, j - 1, rsMetaData.getColumnName(j));
				}
			}

			while (hasDataRows) {
				nullUUIDCount++;
				Row dataRow = currentSheet.createRow(currentSheet.getLastRowNum() + 1);
				for (int j = 1; j <= count; j++) {
					String str = resultSet.getString(j);
					if (str != null) {
						createCell(dataRow, j - 1, str);
					}
				}

				// Check if there are more data rows
				hasDataRows = resultSet.next();
			}

			// Add an empty row between different tables
			currentSheet.createRow(currentSheet.getLastRowNum() + 1);
		}
	}


}

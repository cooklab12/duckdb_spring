package com.example.demo.config;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.types.Types;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

@Configuration
@Profile("s3-iceberg")
public class S3IcebergConfig {

    @Value("${aws.s3.bucket}")
    private String s3Bucket;

    @Value("${aws.s3.region}")
    private String s3Region;

    @Value("${aws.access.key}")
    private String awsAccessKey;

    @Value("${aws.secret.key}")
    private String awsSecretKey;

    @Value("${iceberg.warehouse.path}")
    private String warehousePath;

    @Bean
    public Connection duckDBConnection() throws SQLException {
        // Create connection
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");

        try (Statement stmt = conn.createStatement()) {
            // Install and load required extensions
            stmt.execute("INSTALL httpfs;");
            stmt.execute("LOAD httpfs;");
            stmt.execute("INSTALL iceberg;");
            stmt.execute("LOAD iceberg;");

            // Configure AWS credentials
            stmt.execute(String.format("SET s3_region='%s';", s3Region));
            stmt.execute(String.format("SET s3_access_key_id='%s';", awsAccessKey));
            stmt.execute(String.format("SET s3_secret_access_key='%s';", awsSecretKey));

            // Create Iceberg table
            String createTableSQL = String.format("""
                CREATE TABLE IF NOT EXISTS file_details 
                USING iceberg 
                WITH (
                    location='s3://%s/%s',
                    catalog_type='hadoop',
                    s3_region='%s'
                ) AS 
                SELECT * FROM (
                    SELECT 
                        CAST(NULL AS BIGINT) as id,
                        CAST(NULL AS VARCHAR) as application_name,
                        CAST(NULL AS VARCHAR) as file_location,
                        CAST(NULL AS VARCHAR) as uploaded_by,
                        CAST(NULL AS TIMESTAMP) as uploaded_time,
                        CAST(NULL AS VARCHAR) as tag
                    WHERE 1=0
                );
                """,
                    s3Bucket,
                    warehousePath,
                    s3Region
            );

            stmt.execute(createTableSQL);
        }

        return conn;
    }
}

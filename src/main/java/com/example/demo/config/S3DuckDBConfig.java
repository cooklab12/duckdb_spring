package com.example.demo.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;



@Configuration
@Profile("s3")
public class S3DuckDBConfig {

    @Value("${aws.s3.bucket}")
    private String s3Bucket;

    @Value("${aws.s3.region}")
    private String s3Region;

    @Value("${aws.access.key}")
    private String awsAccessKey;

    @Value("${aws.secret.key}")
    private String awsSecretKey;

    @Value("${aws.s3.folder}")
    private String s3Folder;

    @Bean
    public Connection duckDBConnection() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");

        try (Statement stmt = conn.createStatement()) {
            // Install and load httpfs extension
            stmt.execute("INSTALL httpfs;");
            stmt.execute("LOAD httpfs;");

            // Set AWS credentials
            stmt.execute(String.format("SET s3_region='%s';", s3Region));
            stmt.execute(String.format("SET s3_access_key_id='%s';", awsAccessKey));
            stmt.execute(String.format("SET s3_secret_access_key='%s';", awsSecretKey));

            // Create table using S3 storage with specific folder
            String tablePath = String.format("s3://%s/%s/file_details", s3Bucket, s3Folder);
            stmt.execute(String.format("""
                CREATE TABLE IF NOT EXISTS file_details (
                    id BIGINT PRIMARY KEY,
                    application_name VARCHAR,
                    file_location VARCHAR,
                    uploaded_by VARCHAR,
                    uploaded_time TIMESTAMP,
                    tag VARCHAR
                ) USING parquet STORAGE_TYPE='s3' LOCATION '%s';
                """, tablePath));
        }

        return conn;
    }
}
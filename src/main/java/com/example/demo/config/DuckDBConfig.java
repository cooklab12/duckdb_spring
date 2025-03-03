package com.example.demo.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

@Configuration
public class DuckDBConfig {

    private static final String DB_FILE = "file_management.db";

    @Bean
    public Connection duckDBConnection() throws SQLException {
        // Create a persistent connection to DuckDB
        Connection conn = DriverManager.getConnection("jdbc:duckdb:" + DB_FILE);
        initializeDatabase(conn);
        return conn;
    }

    private void initializeDatabase(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            // Create table if it doesn't exist
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS file_details (
                    id BIGINT PRIMARY KEY,
                    application_name VARCHAR,
                    file_location VARCHAR,
                    uploaded_by VARCHAR,
                    uploaded_time TIMESTAMP,
                    tag VARCHAR
                )
            """);
        }
    }
}
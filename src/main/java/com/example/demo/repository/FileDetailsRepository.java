package com.example.demo.repository;



import com.example.demo.model.FileDetails;
import org.springframework.stereotype.Repository;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Repository
public class FileDetailsRepository {

    private final Connection connection;

    public FileDetailsRepository(Connection connection) {
        this.connection = connection;
    }

    public void save(FileDetails fileDetails) throws SQLException {
        String sql = """
            INSERT INTO file_details (id, application_name, file_location, uploaded_by, uploaded_time, tag)
            VALUES (?, ?, ?, ?, ?, ?)
        """;

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setLong(1, System.currentTimeMillis()); // Using timestamp as ID
            pstmt.setString(2, fileDetails.getApplicationName());
            pstmt.setString(3, fileDetails.getFileLocation());
            pstmt.setString(4, fileDetails.getUploadedBy());
            pstmt.setTimestamp(5, Timestamp.valueOf(fileDetails.getUploadedTime()));
            pstmt.setString(6, fileDetails.getTag());
            pstmt.executeUpdate();
        }
    }

    public List<FileDetails> findAll() throws SQLException {
        List<FileDetails> fileDetailsList = new ArrayList<>();
        String sql = "SELECT * FROM file_details";

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                FileDetails fileDetails = new FileDetails();
                fileDetails.setId(rs.getLong("id"));
                fileDetails.setApplicationName(rs.getString("application_name"));
                fileDetails.setFileLocation(rs.getString("file_location"));
                fileDetails.setUploadedBy(rs.getString("uploaded_by"));
                fileDetails.setUploadedTime(rs.getTimestamp("uploaded_time").toLocalDateTime());
                fileDetails.setTag(rs.getString("tag"));
                fileDetailsList.add(fileDetails);
            }
        }

        return fileDetailsList;
    }
}
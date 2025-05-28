package com.example.demo.repository;

import com.example.demo.model.FileDetails;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class FileDetailsRepositoryTest {

    @Mock
    private Connection connection;

    @Mock
    private PreparedStatement preparedStatement;

    @Mock
    private Statement statement;

    @Mock
    private ResultSet resultSet;

    @InjectMocks
    private FileDetailsRepository fileDetailsRepository;

    @BeforeEach
    void setUp() throws SQLException {
        MockitoAnnotations.openMocks(this);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(resultSet);
    }

    @Test
    void save_shouldInsertFileDetails() throws SQLException {
        FileDetails fileDetails = new FileDetails();
        fileDetails.setApplicationName("testApp");
        fileDetails.setFileLocation("/path/to/file.txt");
        fileDetails.setUploadedBy("user1");
        fileDetails.setUploadedTime(LocalDateTime.now());
        fileDetails.setTag("testTag");

        when(preparedStatement.executeUpdate()).thenReturn(1);

        fileDetailsRepository.save(fileDetails);

        verify(connection).prepareStatement(
                "    INSERT INTO file_details (id, application_name, file_location, uploaded_by, uploaded_time, tag)\n" +
                "    VALUES (?, ?, ?, ?, ?, ?)\n"
        );
        verify(preparedStatement).setLong(eq(1), anyLong()); // ID is System.currentTimeMillis()
        verify(preparedStatement).setString(2, "testApp");
        verify(preparedStatement).setString(3, "/path/to/file.txt");
        verify(preparedStatement).setString(4, "user1");
        verify(preparedStatement).setTimestamp(5, Timestamp.valueOf(fileDetails.getUploadedTime()));
        verify(preparedStatement).setString(6, "testTag");
        verify(preparedStatement).executeUpdate();
    }

    @Test
    void findAll_shouldReturnListOfFileDetails() throws SQLException {
        FileDetails fileDetails = new FileDetails();
        fileDetails.setId(1L);
        fileDetails.setApplicationName("testApp");
        fileDetails.setFileLocation("/path/to/file.txt");
        fileDetails.setUploadedBy("user1");
        LocalDateTime now = LocalDateTime.now();
        fileDetails.setUploadedTime(now);
        fileDetails.setTag("testTag");

        when(resultSet.next()).thenReturn(true).thenReturn(false); // Simulate one row in ResultSet
        when(resultSet.getLong("id")).thenReturn(fileDetails.getId());
        when(resultSet.getString("application_name")).thenReturn(fileDetails.getApplicationName());
        when(resultSet.getString("file_location")).thenReturn(fileDetails.getFileLocation());
        when(resultSet.getString("uploaded_by")).thenReturn(fileDetails.getUploadedBy());
        when(resultSet.getTimestamp("uploaded_time")).thenReturn(Timestamp.valueOf(now));
        when(resultSet.getString("tag")).thenReturn(fileDetails.getTag());

        List<FileDetails> actualList = fileDetailsRepository.findAll();

        assertFalse(actualList.isEmpty());
        assertEquals(1, actualList.size());
        FileDetails actualFileDetails = actualList.get(0);
        assertEquals(fileDetails.getId(), actualFileDetails.getId());
        assertEquals(fileDetails.getApplicationName(), actualFileDetails.getApplicationName());
        assertEquals(fileDetails.getFileLocation(), actualFileDetails.getFileLocation());
        assertEquals(fileDetails.getUploadedBy(), actualFileDetails.getUploadedBy());
        assertEquals(fileDetails.getUploadedTime(), actualFileDetails.getUploadedTime());
        assertEquals(fileDetails.getTag(), actualFileDetails.getTag());

        verify(statement).executeQuery("SELECT * FROM file_details");
    }
    
    @Test
    void findAll_shouldReturnEmptyList_whenNoRecords() throws SQLException {
        when(resultSet.next()).thenReturn(false); // Simulate no rows in ResultSet

        List<FileDetails> actualList = fileDetailsRepository.findAll();

        assertTrue(actualList.isEmpty());
        verify(statement).executeQuery("SELECT * FROM file_details");
    }
}

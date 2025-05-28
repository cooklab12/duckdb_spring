package com.example.demo.controller;

import com.example.demo.model.FileDetails;
import com.example.demo.repository.FileDetailsRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.hasSize;

@WebMvcTest(FileDetailsController.class)
class FileDetailsControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private FileDetailsRepository fileDetailsRepository;

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule()); // To handle LocalDateTime serialization
    }

    @Test
    void saveFileDetails_shouldReturnOkWithMessage_onSuccess() throws Exception {
        FileDetails fileDetails = new FileDetails();
        fileDetails.setApplicationName("testApp");
        fileDetails.setFileLocation("/path/to/file.txt");
        fileDetails.setUploadedBy("user1");
        fileDetails.setUploadedTime(LocalDateTime.now());
        fileDetails.setTag("testTag");

        doNothing().when(fileDetailsRepository).save(any(FileDetails.class));

        mockMvc.perform(post("/api/files")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(fileDetails)))
                .andExpect(status().isOk())
                .andExpect(content().string("File details saved successfully"));
    }
    
    @Test
    void saveFileDetails_shouldReturnInternalServerError_onSQLException() throws Exception {
        FileDetails fileDetails = new FileDetails();
        fileDetails.setApplicationName("testApp");
        // ... other properties ...

        doThrow(new SQLException("Database error")).when(fileDetailsRepository).save(any(FileDetails.class));

        mockMvc.perform(post("/api/files")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(fileDetails)))
                .andExpect(status().isInternalServerError())
                .andExpect(content().string("Error saving file details: Database error"));
    }


    @Test
    void getAllFileDetails_shouldReturnListOfFileDetails_onSuccess() throws Exception {
        FileDetails file1 = new FileDetails();
        file1.setId(1L);
        file1.setApplicationName("app1");
        file1.setFileLocation("/file1.txt");
        file1.setUploadedBy("user1");
        file1.setUploadedTime(LocalDateTime.now()); // Ensure consistent time for comparison if needed
        file1.setTag("tag1");

        FileDetails file2 = new FileDetails();
        file2.setId(2L);
        file2.setApplicationName("app2");
        file2.setFileLocation("/file2.txt");
        file2.setUploadedBy("user2");
        file2.setUploadedTime(LocalDateTime.now().minusDays(1)); // Ensure consistent time
        file2.setTag("tag2");

        List<FileDetails> allFiles = Arrays.asList(file1, file2);

        when(fileDetailsRepository.findAll()).thenReturn(allFiles);

        mockMvc.perform(get("/api/files")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(2)))
                .andExpect(jsonPath("$[0].applicationName", is("app1")))
                .andExpect(jsonPath("$[1].applicationName", is("app2")));
    }
    
    @Test
    void getAllFileDetails_shouldReturnEmptyList_whenNoFiles_onSuccess() throws Exception {
        when(fileDetailsRepository.findAll()).thenReturn(Collections.emptyList());

        mockMvc.perform(get("/api/files")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(0)));
    }

    @Test
    void getAllFileDetails_shouldReturnInternalServerError_onSQLException() throws Exception {
        when(fileDetailsRepository.findAll()).thenThrow(new SQLException("Database error"));

        mockMvc.perform(get("/api/files")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isInternalServerError())
                .andExpect(content().string("Error retrieving file details: Database error"));
    }
}

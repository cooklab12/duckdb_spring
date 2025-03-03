package com.example.demo.controller;


import com.example.demo.model.FileDetails;
import com.example.demo.repository.FileDetailsRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.sql.SQLException;
import java.util.List;

@RestController
@RequestMapping("/api/files")
public class FileDetailsController {

    private final FileDetailsRepository repository;

    public FileDetailsController(FileDetailsRepository repository) {
        this.repository = repository;
    }

    @PostMapping
    public ResponseEntity<String> saveFileDetails(@RequestBody FileDetails fileDetails) {
        try {
            repository.save(fileDetails);
            return ResponseEntity.ok("File details saved successfully");
        } catch (SQLException e) {
            return ResponseEntity.internalServerError().body("Error saving file details: " + e.getMessage());
        }
    }

    @GetMapping
    public ResponseEntity<?> getAllFileDetails() {
        try {
            List<FileDetails> fileDetails = repository.findAll();
            return ResponseEntity.ok(fileDetails);
        } catch (SQLException e) {
            return ResponseEntity.internalServerError().body("Error retrieving file details: " + e.getMessage());
        }
    }
}
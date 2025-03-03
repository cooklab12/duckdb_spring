package com.example.demo.model;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class FileDetails {
    private Long id;
    private String applicationName;
    private String fileLocation;
    private String uploadedBy;
    private LocalDateTime uploadedTime;
    private String tag;
}
package com.example.demo.canalexample.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;


@lombok.Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Data {

    private String id;
    @JsonProperty("contnr_code")
    private String contnrCode;
}
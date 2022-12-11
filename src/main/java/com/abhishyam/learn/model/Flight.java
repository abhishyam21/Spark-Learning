package com.abhishyam.learn.model;


import lombok.Data;

@Data
public class Flight {

    private String dest_country_name;
    private String origin_country_name;
    private Long count;

}

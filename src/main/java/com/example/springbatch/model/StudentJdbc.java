package com.example.springbatch.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class StudentJdbc {

    private Long id;

    private String firstName;

    private String lastName;

    private String email;
}

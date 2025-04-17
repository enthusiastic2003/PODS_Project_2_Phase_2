package com.saikiran.demo_onlycrud.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;


@Entity
@Table()
public class Account {
    @Id
    private Integer id;
    private String name;
    private String email;
    private Boolean discount_availed = false;
  
    public Account() {
    }

    public Account(Integer id, String name, String email, Boolean discountAvailed) {
        this.id = id;
        this.name = name;
        this.email = email;
        this.discount_availed = discountAvailed;
    }

    public Integer getId() {
        return id;
    }
    public String getName() {
        return name;
    }
    public String getEmail() {
        return email;
    }
    public Boolean getDiscount_availed() {
        return discount_availed;
    }

    public void setId(Integer id) {
        this.id = id;
    }
    public void setName(String name) {
        this.name = name;
    }
    public void setEmail(String email) {
        this.email = email;
    }
    public void setDiscount_availed(Boolean discountAvailed) {
        this.discount_availed = discountAvailed;
    }

}
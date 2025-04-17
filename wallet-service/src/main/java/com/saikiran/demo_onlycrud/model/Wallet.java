package com.saikiran.demo_onlycrud.model;

import jakarta.persistence.Entity;

import jakarta.persistence.Id;
import jakarta.persistence.Table;


@Entity
@Table()
public class Wallet {
    @Id
    private Integer user_id;
    private Integer balance;

  

    public Wallet() {
    }
    public Wallet(Integer id, Integer balance) {
        this.user_id = id;
        this.balance = balance;

    }

    public Integer getUser_id() {
        return user_id;
    }
    public Integer getBalance() {
        return balance;
    }

    public void setUser_id(Integer id) {
        this.user_id = id;
    }
    public void setBalance(Integer balance) {
        this.balance = balance;
    }

}
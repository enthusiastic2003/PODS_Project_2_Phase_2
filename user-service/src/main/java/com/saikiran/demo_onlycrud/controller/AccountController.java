package com.saikiran.demo_onlycrud.controller;

import com.saikiran.demo_onlycrud.model.Account;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.PutMapping;

import com.saikiran.demo_onlycrud.service.AccountService;
import org.springframework.web.bind.annotation.PathVariable;

import java.util.List;



@RestController
@RequestMapping("/users")
public class AccountController {

    AccountService accountService;
    public AccountController(AccountService accountService) {
        this.accountService = accountService;
    }



    @GetMapping("/{id}")
    public Account getAccount(@PathVariable("id") Integer id) {
        
        return accountService.getAccount(id);
    }

    @GetMapping()
    public List<Account> getAllAccount() {
        return accountService.getAllAccounts();
    }

    @PostMapping
    public ResponseEntity<Account> postAccount(@RequestBody Account account) {
        Account createdAccount = accountService.createAccount(account);
        return new ResponseEntity<>(createdAccount, HttpStatus.CREATED);
    }

   
    @PutMapping("/{id}")
    public ResponseEntity<Account> putAccount(@PathVariable("id") Integer id) {
        Account updatedAccount = accountService.updateAccount(id);
        return new ResponseEntity<>(updatedAccount, HttpStatus.OK);
    }



    @DeleteMapping("/{id}")
    public String deleteAccount(@PathVariable("id") Integer id) {
        accountService.deleteAccount(id);
        return "Account deleted successfully";
    }

    @DeleteMapping()
    public String deleteAll() {
        accountService.deleteAll();
        return "All accounts deleted successfully";
    }
    

}

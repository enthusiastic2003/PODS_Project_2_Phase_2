package com.saikiran.demo_onlycrud.service;
import java.util.List;
import com.saikiran.demo_onlycrud.model.Account;

public interface AccountService {

    public Account createAccount(Account account);
    public Account updateAccount(Integer id);
    public String deleteAccount(Integer id);
    public String deleteAll();
    public Account getAccount(Integer id);
    public List<Account> getAllAccounts();

    
} 
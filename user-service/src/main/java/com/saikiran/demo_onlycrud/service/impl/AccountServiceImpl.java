package com.saikiran.demo_onlycrud.service.impl;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.server.ResponseStatusException;

import com.saikiran.demo_onlycrud.model.Account;
import com.saikiran.demo_onlycrud.repository.AccountRepository;
import com.saikiran.demo_onlycrud.service.AccountService;

import jakarta.transaction.Transactional;




@Service
public class AccountServiceImpl implements AccountService {


    AccountRepository accountRepository;
    RestTemplate restTemplate;

    public AccountServiceImpl(AccountRepository accountRepository, RestTemplate restTemplate) {
        this.accountRepository = accountRepository;
        this.restTemplate = restTemplate;
    }

    @Override
    public Account createAccount(Account account) {
        if(accountRepository.existsByEmail(account.getEmail())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Email already exists");
        }
        if(accountRepository.existsById(account.getId())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "ID already exists");
        }
        
        return accountRepository.save(account);
    }

    @Override
    public Account updateAccount(Integer id) {
        Account existingAccount = accountRepository.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Account not found"));
        existingAccount.setDiscount_availed(true);
        accountRepository.save(existingAccount);
        return existingAccount;
    }

   


    @Override
    public String deleteAccount(Integer id) {
        if (!accountRepository.existsById(id)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Account not found");
        }
        
        
        accountRepository.deleteById(id);

        // Delete marketplace
        String url2 = "http://host.docker.internal:8081/marketplace/users/" + id;
        // String url2 = "http://localhost:8081/marketplace/users/" + id;
        // String url2 = "http://marketplace-service:8081/marketplace/users/" + id;
        try {
            restTemplate.delete(url2);
        } catch (HttpClientErrorException.NotFound e) {
            System.out.println("User not found in marketplace for user ID: " + id);
        }

        // Delete wallet
        String url1 = "http://host.docker.internal:8082/wallets/" + id;
        // String url1 = "http://localhost:8082/wallets/" + id;
        // String url1 = "http://wallet-service:8082/wallets/" + id;
        try {
            restTemplate.delete(url1);
        } catch (HttpClientErrorException.NotFound e) {
            System.out.println("Wallet not found for user ID: " + id);
        }
        

        return "Account deleted successfully";
    }



  

    @Override
    public Account getAccount(Integer id) {
        return accountRepository.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Account not found"));
    }

    @Override
    public List<Account> getAllAccounts() {
        return accountRepository.findAll();
    }

    @Override
    public String deleteAll() {
        

        accountRepository.deleteAll();
        String walletendpoint = "http://host.docker.internal:8082/wallets";
        // String walletendpoint = "http://localhost:8082/wallets";
        // String walletendpoint = "http://wallet-service:8082/wallets";
        try {
            restTemplate.delete(walletendpoint);
        } catch (HttpClientErrorException.NotFound e) {
            // Log the error or handle it as needed
            System.out.println("wallets end point not found ");
        }
        String marketplaceendpoint = "http://host.docker.internal:8081/marketplace";
        // String marketplaceendpoint = "http://localhost:8081/marketplace";
        // String marketplaceendpoint = "http://marketplace-service:8081/marketplace";
        try {
            restTemplate.delete(marketplaceendpoint);
        } catch (HttpClientErrorException.NotFound e) {
            // Log the error or handle it as needed
            System.out.println("Marketplace end point not found ");
        }

        
      
        return "All accounts deleted successfully";
    }
    
}

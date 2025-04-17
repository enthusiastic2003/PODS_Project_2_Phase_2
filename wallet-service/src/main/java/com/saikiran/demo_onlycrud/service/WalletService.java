package com.saikiran.demo_onlycrud.service;
import java.util.List;
import com.saikiran.demo_onlycrud.model.Wallet;

// import jakarta.persistence.criteria.CriteriaBuilder.In;

// import jakarta.persistence.criteria.CriteriaBuilder.In;

public interface WalletService {

  
    public Wallet updateWalletBalance(Integer id, String action, Integer amount);
    public String deleteWallet(Integer id);
    public String deleteAll();
    public Wallet getWallet(Integer id);
    public List<Wallet> getAllWallets();

    
} 
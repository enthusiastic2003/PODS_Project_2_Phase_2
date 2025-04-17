package com.saikiran.demo_onlycrud.service.impl;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import com.saikiran.demo_onlycrud.model.Wallet;
import com.saikiran.demo_onlycrud.repository.WalletRepository;
import com.saikiran.demo_onlycrud.service.WalletService;
import org.springframework.transaction.annotation.Transactional;





@Service
public class WalletServiceImpl implements WalletService {
    
    // @Autowired
    WalletRepository walletRepository;


    public WalletServiceImpl(WalletRepository walletRepository) {
        this.walletRepository = walletRepository;
    }

    @Override
    @Transactional
    public Wallet updateWalletBalance(Integer id, String action, Integer amount) {
        Wallet wallet = walletRepository.findByIdForUpdate(id).orElseGet(() -> {
            
            Wallet newWallet = new Wallet();
            newWallet.setUser_id(id);
            newWallet.setBalance(0);
            return walletRepository.save(newWallet);
        });
        System.out.println("");
        System.out.print("  Current balance = "+wallet.getBalance());
        System.out.print("  Amount = "+amount+action);
        Integer newBalance;
        if ("debit".equalsIgnoreCase(action)) {
            if (wallet.getBalance() < amount) {
              System.out.print("Insufficient balance");
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Insufficient balance");
            }
            newBalance = wallet.getBalance() - amount;
            wallet.setBalance(newBalance);
            System.out.print("  After debit balance = "+wallet.getBalance());
        } else if ("credit".equalsIgnoreCase(action)) {
            newBalance = wallet.getBalance() + amount;
            wallet.setBalance(newBalance);
            System.out.print("  After credit balance = "+wallet.getBalance());
        } else {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid action");
        }
        walletRepository.save(wallet);
        return wallet;
    }



    @Override
    public String deleteWallet(Integer id) {
        System.out.println("Recived delete request for id: " + id);
        if (!walletRepository.existsById(id)) {
            System.out.println("Wallet not found");
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Wallet not found");
        }
        walletRepository.deleteById(id);
        System.out.println("Deleted account with id: "+id);
        return "Account deleted successfully";
    }

    @Override
    public Wallet getWallet(Integer id) {
        
        return walletRepository.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Wallet not found"));
    }

    @Override
    public List<Wallet> getAllWallets() {
        return walletRepository.findAll();
    }

    @Override
    public String deleteAll() {
        walletRepository.deleteAll();
        return "All wallets deleted successfully";
    }
    
}

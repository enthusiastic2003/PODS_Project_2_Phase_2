package com.saikiran.demo_onlycrud.controller;
import com.saikiran.demo_onlycrud.model.Wallet;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import com.saikiran.demo_onlycrud.service.WalletService;
import org.springframework.web.bind.annotation.PathVariable;
import java.util.List;
import org.springframework.transaction.annotation.Transactional;




@RestController
@RequestMapping("/wallets")
public class WalletController {

    // WalletService walletService;
    private final WalletService walletService;
    Integer i=0;
    Integer failed = 0;
    Integer success = 0;

    public WalletController(WalletService walletService) {
        this.walletService = walletService;
    }

    @GetMapping("/{id}")
    public Wallet getAccount(@PathVariable("id") Integer id) {
        return walletService.getWallet(id);
    }

    @GetMapping()
    public List<Wallet> getAllAccount() {
        return walletService.getAllWallets();
    }

    @PutMapping("/{id}")
    public ResponseEntity<Wallet> putAccount(@PathVariable("id") Integer id, @RequestBody WalletUpdateRequest request) {
        Wallet updatedAccount = walletService.updateWalletBalance(id, request.getAction(), request.getAmount());
        return new ResponseEntity<>(updatedAccount, HttpStatus.OK);
    }

    @DeleteMapping("/{id}")
    @Transactional
    public String deleteAccount(@PathVariable("id") Integer id) {
        System.out.println("Deleting account with id: "+id);
        walletService.deleteWallet(id);
        return "Account deleted successfully";
    }

    @DeleteMapping()
    @Transactional
    public String deleteAll() {
        walletService.deleteAll();
        return "All accounts deleted successfully";
    }

    public static class WalletUpdateRequest {
        private String action;
        private Integer amount;

        // Getters and setters
        public String getAction() {
            return action;
        }

        public void setAction(String action) {
            this.action = action;
        }

        public Integer getAmount() {
            return amount;
        }

        public void setAmount(Integer amount) {
            this.amount = amount;
        }
    }
    

}

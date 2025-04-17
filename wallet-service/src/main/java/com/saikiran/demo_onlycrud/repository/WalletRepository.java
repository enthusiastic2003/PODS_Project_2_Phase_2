package com.saikiran.demo_onlycrud.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import com.saikiran.demo_onlycrud.model.Wallet;

import jakarta.persistence.LockModeType;

import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
public interface WalletRepository extends JpaRepository<Wallet, Integer> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT w FROM Wallet w WHERE w.user_id = :id")
    Optional<Wallet> findByIdForUpdate(@Param("id") Integer id);

    
} 
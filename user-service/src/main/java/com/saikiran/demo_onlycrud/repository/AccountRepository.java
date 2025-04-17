package com.saikiran.demo_onlycrud.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.saikiran.demo_onlycrud.model.Account;

import jakarta.persistence.LockModeType;
public interface AccountRepository extends JpaRepository<Account, Integer> {
    boolean existsByEmail(String email);

    @Query("SELECT a FROM Account a WHERE a.id = :id")
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    Optional<Account> findByIdWithLock(@Param("id") Integer id);
    
} 
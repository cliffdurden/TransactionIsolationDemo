package com.github.cliffdurden.transactionisolationdemo.repository;


import com.github.cliffdurden.transactionisolationdemo.entity.Book;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface BookRepository extends JpaRepository<Book, Long> {

    List<Book> findAllByRatingGreaterThan(Integer rating);
}

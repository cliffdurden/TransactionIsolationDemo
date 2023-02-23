package com.github.cliffdurden.transactionisolationdemo.service;

import com.github.cliffdurden.transactionisolationdemo.entity.Book;
import com.github.cliffdurden.transactionisolationdemo.repository.BookRepository;
import jakarta.persistence.EntityManager;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.*;

import java.util.*;
import java.util.concurrent.CountDownLatch;

@Slf4j
@Service
@AllArgsConstructor
public class BookServiceDemoImpl {

    private final BookRepository repository;

    private final EntityManager entityManager;

    @SneakyThrows
    @Transactional(isolation = Isolation.READ_UNCOMMITTED)
    public Book dirtyReadWhenIsolationLevelIsReadUncommittedT1(CountDownLatch latch, CountDownLatch latchT1aux, Long id) {
        return dirtyRead(latch, latchT1aux, id);
    }

    @SneakyThrows
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public Book dirtyReadWhenIsolationLevelIsReadCommittedT1(CountDownLatch latch, CountDownLatch latchT1aux, Long id) {
        return dirtyRead(latch, latchT1aux, id);
    }

    @SneakyThrows
    @Transactional(isolation = Isolation.REPEATABLE_READ)
    public Book dirtyReadWhenIsolationLevelIsRepeatableReadT1(CountDownLatch latch, CountDownLatch latchT1aux, Long id) {
        return dirtyRead(latch, latchT1aux, id);
    }

    @SneakyThrows
    @Transactional(isolation = Isolation.SERIALIZABLE)
    public Book dirtyReadWhenIsolationLevelIsSerializableT1(CountDownLatch latch, CountDownLatch latchT1aux, Long id) {
        return dirtyRead(latch, latchT1aux, id);
    }

    @SneakyThrows
    @Transactional(isolation = Isolation.READ_UNCOMMITTED)
    public Book nonRepeatableReadWhenIsolationLevelIsReadUncommittedT1(CountDownLatch latch, CountDownLatch latchT1Aux, Long id) {
        return nonRepeatableRead(latch, latchT1Aux, id);
    }

    @SneakyThrows
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public Book nonRepeatableReadWhenIsolationLevelIsReadCommittedT1(CountDownLatch latch, CountDownLatch latchT1Aux, Long id) {
        return nonRepeatableRead(latch, latchT1Aux, id);
    }

    @SneakyThrows
    @Transactional(isolation = Isolation.REPEATABLE_READ)
    public Book nonRepeatableReadWhenIsolationLevelIsRepeatableReadT1(CountDownLatch latch, CountDownLatch latchT1Aux, Long id) {
        return nonRepeatableRead(latch, latchT1Aux, id);
    }

    @SneakyThrows
    @Transactional(isolation = Isolation.READ_UNCOMMITTED)
    public List<Book> phantomReadWhenIsolationLevelIsReadUncommittedT1(CountDownLatch latch, CountDownLatch latchT1Aux) {
        return phantomRead(latch, latchT1Aux);
    }

    @SneakyThrows
    @Transactional(isolation = Isolation.SERIALIZABLE)
    public Book nonRepeatableReadWhenIsolationLevelIsSerializableT1(CountDownLatch latch, CountDownLatch latchT1Aux, Long id) {
        return nonRepeatableRead(latch, latchT1Aux, id);
    }

    @SneakyThrows
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public List<Book> phhantomReadWhenIsolationLevelIsReadCommittedT1(CountDownLatch latch, CountDownLatch latchT1Aux) {
        return phantomRead(latch, latchT1Aux);
    }

    @SneakyThrows
    @Transactional(isolation = Isolation.REPEATABLE_READ)
    public List<Book> phantomReadWhenIsolationLevelIsRepeatableReadT1(CountDownLatch latch, CountDownLatch latchT1Aux) {
        return phantomRead(latch, latchT1Aux);
    }

    @SneakyThrows
    @Transactional(isolation = Isolation.SERIALIZABLE)
    public List<Book> phantomReadWhenIsolationLevelIsSerializableT1(CountDownLatch latch, CountDownLatch latchT1Aux) {
        return phantomRead(latch, latchT1Aux);
    }

    @SneakyThrows
    @Transactional
    public void updateRatingT2(Long id, Integer rating) {
        log.info("{}, T2 started.", threadName());
        var book = repository.findById(id).orElseThrow();
        log.info("{}, T2 selected from db... ", threadName());
        book.setRating(rating);
        log.info("{}, T2 save to db... ", threadName());
        repository.save(book);
    }

    @SneakyThrows
    @Transactional
    public void updateRatingT2(CountDownLatch latch2, Long id, Integer rating) {
        updateRatingT2(id, rating);
        latch2.await(); // for dirty read examples.
    }

    @SneakyThrows
    @Transactional
    public void addBooksT2(Book... newBooks) {
        log.info("{}, T2 started.", threadName());
        log.info("{}, T2 save to db... ", threadName());
        repository.saveAll(Arrays.asList(newBooks));
    }

    private Book dirtyRead(CountDownLatch latch, CountDownLatch latchT1aux, Long id) throws InterruptedException {
        log.info("{}, T1 started.", threadName());
        latchT1aux.countDown();
        latch.await(); // waiting until another transaction will have made changes
        log.info("{}, T1 select from db... ", threadName());
        return repository.findById(id).orElseThrow();
    }

    private List<Book> phantomRead(CountDownLatch latch, CountDownLatch latchT1Aux) throws InterruptedException {
        log.info("{}, T1 started.", threadName());
        var booksInTheBeginningOfTransaction = repository.findAll();
        log.debug("{}, T1 book in the beginning. value: {}", threadName(), booksInTheBeginningOfTransaction);
        latchT1Aux.countDown();
        latch.await(); // waiting until another transaction has been finished
        log.info("{}, T1 select from db... ", threadName());
        return repository.findAll();
    }

    private Book nonRepeatableRead(CountDownLatch latch, CountDownLatch latchT1Aux, Long id) throws InterruptedException {
        log.info("{}, T1 started.", threadName());
        val bookInTheBeginningOfTransaction = repository.findById(id).orElseThrow();
        log.info("{}, T1 book in the beginning. value: {}", threadName(), bookInTheBeginningOfTransaction);
        entityManager.clear();
        latchT1Aux.countDown();
        latch.await(); // waiting until another transaction has been finished
        log.info("{}, T1 select from db... ", threadName());
        return repository.findById(id).orElseThrow();
    }

    public Book save(Book book) {
        return repository.save(book);
    }

    public void deleteAll() {
        repository.deleteAll();
    }

    private String threadName() {
        return Thread.currentThread().getName();
    }

}

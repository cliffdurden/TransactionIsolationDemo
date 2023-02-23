package com.github.cliffdurden.transactionisolationdemo;

import com.github.cliffdurden.transactionisolationdemo.entity.Book;
import com.github.cliffdurden.transactionisolationdemo.service.BookServiceDemoImpl;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.*;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.*;

import java.util.concurrent.CountDownLatch;

import static java.util.concurrent.CompletableFuture.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
@SpringBootTest
@Testcontainers
class TransactionIsolationDemoApplicationTests {

    @Container
    static PostgreSQLContainer<?> db = new PostgreSQLContainer<>("postgres:15.2")
            .withLogConsumer(new Slf4jLogConsumer(log));

    @Autowired
    private BookServiceDemoImpl testSubject;

    private Book book1;

    @BeforeEach
    void setUp() {
        book1 = testSubject.save(book1());
    }

    @AfterEach
    void tearDown() {
        testSubject.deleteAll();
    }

    @DynamicPropertySource
    static void registerPgProperties(DynamicPropertyRegistry registry) {
        registry.add("DB_URL", () -> db.getJdbcUrl());
        registry.add("DB_USERNAME", () -> db.getUsername());
        registry.add("DB_PASSWORD", () -> db.getPassword());
    }

    @DisplayName("[Isolation.READ_UNCOMMITTED / Dirty read] shouldn't read non-committed changes because Postgres does not support this Isolation Level.")
    @Test
    @SneakyThrows
    void testDirtyReadWhenIsolationLevelIsReadUncommitted() {
        final var newRating = 10;
        CountDownLatch latchT1 = new CountDownLatch(1);
        CountDownLatch latchT1aux = new CountDownLatch(1);
        CountDownLatch latchT2 = new CountDownLatch(1);

        var future1 = supplyAsync(
                () -> testSubject.dirtyReadWhenIsolationLevelIsReadUncommittedT1(latchT1, latchT1aux, book1.getId())
        );
        latchT1aux.await(); // wait until T1 has been started
        runAsync(() -> testSubject.updateRatingT2(latchT2, book1.getId(), newRating));

        latchT1.countDown();
        val result = future1.get();
        latchT2.countDown();

        assertEquals(book1.getRating(), result.getRating(), "Rating should the same as before transaction");
    }

    @DisplayName("[Isolation.READ_COMMITTED / Dirty read] shouldn't read non-committed changes.")
    @Test
    @SneakyThrows
    void testDirtyReadWhenIsolationLevelIsReadCommitted() {
        final var newRating = 10;
        CountDownLatch latchT1 = new CountDownLatch(1);
        CountDownLatch latchT1aux = new CountDownLatch(1);
        CountDownLatch latchT2 = new CountDownLatch(1);

        var future1 = supplyAsync(
                () -> testSubject.dirtyReadWhenIsolationLevelIsReadCommittedT1(latchT1, latchT1aux, book1.getId())
        );
        latchT1aux.await(); // wait T1 has been started
        runAsync(() -> testSubject.updateRatingT2(latchT2, book1.getId(), newRating));

        latchT1.countDown();
        val result = future1.get();
        latchT2.countDown();

        assertEquals(book1.getRating(), result.getRating(), "Rating should the same as before transaction");
    }

    @DisplayName("[Isolation.REPEATABLE_READ / Dirty read] shouldn't read non-committed changes.")
    @Test
    @SneakyThrows
    void testDirtyReadWhenIsolationLevelIsRepeatableRead() {
        final var newRating = 10;
        CountDownLatch latchT1 = new CountDownLatch(1);
        CountDownLatch latchT1aux = new CountDownLatch(1);
        CountDownLatch latchT2 = new CountDownLatch(1);

        var future1 = supplyAsync(
                () -> testSubject.dirtyReadWhenIsolationLevelIsRepeatableReadT1(latchT1, latchT1aux, book1.getId())
        );
        latchT1aux.await(); // wait until T1 has been started
        runAsync(() -> testSubject.updateRatingT2(latchT2, book1.getId(), newRating));

        latchT1.countDown();
        val result = future1.get();
        latchT2.countDown();

        assertEquals(book1.getRating(), result.getRating(), "Rating should the same as before transaction");
    }

    @DisplayName("[Isolation.SERIALIZABLE / Dirty read] shouldn't read non-committed changes. Isolation.SERIALIZABLE")
    @Test
    @SneakyThrows
    void testDirtyReadWhenIsolationLevelIsSerializable() {
        final var newRating = 10;
        CountDownLatch latchT1 = new CountDownLatch(1);
        CountDownLatch latchT1aux = new CountDownLatch(1);
        CountDownLatch latchT2 = new CountDownLatch(1);

        var future1 = supplyAsync(
                () -> testSubject.dirtyReadWhenIsolationLevelIsSerializableT1(latchT1, latchT1aux, book1.getId())
        );
        latchT1aux.await(); // wait until T1 has been started
        runAsync(() -> testSubject.updateRatingT2(latchT2, book1.getId(), newRating));

        latchT1.countDown();
        val result = future1.get();
        latchT2.countDown();

        assertEquals(book1.getRating(), result.getRating(), "Rating should the same as before transaction");
    }

    @DisplayName("[Isolation.READ_UNCOMMITTED / Non-repeatable read] should read committed changes has been made by another transaction.")
    @Test
    @SneakyThrows
    void testNonRepeatableReadWhenIsolationLevelIsReadUncommitted() {
        final var newRating = 10;
        CountDownLatch latch = new CountDownLatch(1);
        CountDownLatch latchT1Aux = new CountDownLatch(1);

        var future1 = supplyAsync(
                () -> testSubject.nonRepeatableReadWhenIsolationLevelIsReadUncommittedT1(latch, latchT1Aux, book1.getId())
        );
        latchT1Aux.await(); //wait until T1 has been started
        runAsync(() -> testSubject
                .updateRatingT2(book1.getId(), newRating))
                .thenAccept((__) -> latch.countDown());
        val result = future1.get();

        assertEquals(newRating, result.getRating(), "Rating should be from committed transaction");
    }

    @DisplayName("[Isolation.READ_COMMITTED / Non-repeatable read] should read committed changes has been made by another transaction.")
    @Test
    @SneakyThrows
    void testNonRepeatableReadWhenIsolationLevelIsReadCommitted() {
        final var newRating = 10;
        CountDownLatch latch = new CountDownLatch(1);
        CountDownLatch latchT1Aux = new CountDownLatch(1);

        var future1 = supplyAsync(
                () -> testSubject.nonRepeatableReadWhenIsolationLevelIsReadCommittedT1(latch, latchT1Aux, book1.getId())
        );
        latchT1Aux.await(); // wait until T1 has been started
        runAsync(() -> testSubject
                .updateRatingT2(book1.getId(), newRating))
                .thenAccept((__) -> latch.countDown());
        val result = future1.get();

        assertEquals(newRating, result.getRating(), "Rating should be from committed transaction");
    }

    @DisplayName("[Isolation.REPEATABLE_READ / Non-repeatable read] shouldn't read committed changes has been made by another transaction.")
    @Test
    @SneakyThrows
    void testNonRepeatableReadWhenIsolationLevelIsRepeatableRead() {
        final var expectedRating = 10;
        CountDownLatch latchT1 = new CountDownLatch(1);
        CountDownLatch latchT1Aux = new CountDownLatch(1);

        var future1 = supplyAsync(
                () -> testSubject.nonRepeatableReadWhenIsolationLevelIsRepeatableReadT1(latchT1, latchT1Aux, book1.getId())
        );
        latchT1Aux.await(); // wait until T1 has been started
        runAsync(() -> testSubject
                .updateRatingT2(book1.getId(), expectedRating))
                .thenAccept((__) -> latchT1.countDown());
        val result = future1.get();

        assertEquals(book1.getRating(), result.getRating(), "Rating should be from committed transaction");
    }

    @DisplayName("[Isolation.Isolation.SERIALIZABLE / Non-repeatable read] shouldn't read committed changes has been made by another transaction.")
    @Test
    @SneakyThrows
    void testNonRepeatableReadWhenIsolationLevelIsSerializable() {
        final var expectedRating = 10;
        CountDownLatch latchT1 = new CountDownLatch(1);
        CountDownLatch latchT1Aux = new CountDownLatch(1);

        var future1 = supplyAsync(
                () -> testSubject.nonRepeatableReadWhenIsolationLevelIsSerializableT1(latchT1, latchT1Aux, book1.getId())
        );
        latchT1Aux.await(); // wait until T1 has been started
        runAsync(() -> testSubject
                .updateRatingT2(book1.getId(), expectedRating))
                .thenAccept((__) -> latchT1.countDown());
        val result = future1.get();

        assertEquals(book1.getRating(), result.getRating(), "Rating should be from committed transaction");
    }

    @DisplayName("[Isolation.READ_UNCOMMITTED / Phantom read] should read rows has been inserted by another transaction.")
    @Test
    @SneakyThrows
    void testPhantomReadReadWhenIsolationLevelIsReadUncommitted() {
        CountDownLatch latchT1 = new CountDownLatch(1);
        CountDownLatch latchT1Aux = new CountDownLatch(1);
        var book2 = book2();
        var book3 = book3();

        var future1 = supplyAsync(
                () -> testSubject.phantomReadWhenIsolationLevelIsReadUncommittedT1(latchT1, latchT1Aux)
        );
        latchT1Aux.await(); // wait until T1 has been started
        runAsync(() -> testSubject
                .addBooksT2(book2, book3))
                .thenAccept((__) -> latchT1.countDown());
        val result = future1.get();

        assertEquals(3, result.size(), "Count should has taken into consideration insertion from another transaction");
    }

    @DisplayName("[Isolation.READ_COMMITTED / Phantom read] should read rows has been inserted by another transaction.")
    @Test
    @SneakyThrows
    void testPhantomReadReadWhenIsolationLevelIsReadCommitted() {
        CountDownLatch latchT1 = new CountDownLatch(1);
        CountDownLatch latchT1Aux = new CountDownLatch(1);
        var book2 = book2();
        var book3 = book3();

        var future1 = supplyAsync(
                () -> testSubject.phhantomReadWhenIsolationLevelIsReadCommittedT1(latchT1, latchT1Aux)
        );
        latchT1Aux.await(); // wait until T1 has been started
        runAsync(() -> testSubject
                .addBooksT2(book2, book3))
                .thenAccept((__) -> latchT1.countDown());
        val result = future1.get();

        assertEquals(3, result.size(), "Count should has taken into consideration insertion from another transaction");
    }

    @DisplayName("[Isolation.REPEATABLE_READ / Phantom read] shouldn't read rows has been inserted by another transaction. (Allowed, but not in PG)")
    @Test
    @SneakyThrows
    void testPhantomReadReadWhenIsolationLevelIsRepeatableRead() {
        CountDownLatch latchT1 = new CountDownLatch(1);
        CountDownLatch latchT1Aux = new CountDownLatch(1);
        var book2 = book2();
        var book3 = book3();

        var future1 = supplyAsync(() -> testSubject.phantomReadWhenIsolationLevelIsRepeatableReadT1(latchT1, latchT1Aux));
        latchT1Aux.await(); // wait until T1 has been started
        runAsync(() -> testSubject
                .addBooksT2(book2, book3))
                .thenAccept((__) -> latchT1.countDown());
        latchT1.countDown();
        val result = future1.get();

        assertEquals(1, result.size(), "Count should has taken into consideration insertion from another transaction");
    }

    @DisplayName("[Isolation.SERIALIZABLE / Phantom read] shouldn't read rows has been inserted by another transaction.")
    @Test
    @SneakyThrows
    void testPhantomReadReadWhenIsolationLevelIsSerializable() {
        CountDownLatch latchT1 = new CountDownLatch(1);
        CountDownLatch latchT1Aux = new CountDownLatch(1);
        var book2 = book2();
        var book3 = book3();

        var future1 = supplyAsync(() -> testSubject.phantomReadWhenIsolationLevelIsSerializableT1(latchT1, latchT1Aux));
        latchT1Aux.await(); // wait until T1 has been started
        runAsync(() -> testSubject
                .addBooksT2(book2, book3))
                .thenAccept((__) -> latchT1.countDown());
        val result = future1.get();

        assertEquals(1, result.size(), "Another transaction shouldn't affect the result count");
    }

    private Book book1() {
        return Book.builder()
                .author("Donald Knuth")
                .title("Art of Computer Programming, Volume 1: Fundamental Algorithms")
                .rating(0)
                .build();
    }

    private Book book2() {
        return Book.builder()
                .author("Donald Knuth")
                .title("Art of Computer Programming, Volume 2: Seminumerical Algorithms")
                .rating(5)
                .build();
    }

    private Book book3() {
        return Book.builder()
                .author("Donald Knuth")
                .title("Art of Computer Programming, Volume 3: Sorting and Searching")
                .rating(5)
                .build();
    }
}

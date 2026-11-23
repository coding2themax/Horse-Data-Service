package com.coding2themax.horse.order.infrastructure;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import com.coding2themax.horse.order.domain.Customer;

public interface CustomerRepository extends ReactiveCrudRepository<Customer, Integer> {
}

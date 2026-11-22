package com.coding2themax.horse.order.application;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import com.coding2themax.horse.order.api.dto.CustomerRequest;
import com.coding2themax.horse.order.domain.Customer;
import com.coding2themax.horse.order.infrastructure.CustomerRepository;

import reactor.core.publisher.Mono;

@Service
public class CustomerService {

  private final CustomerRepository customers;

  public CustomerService(CustomerRepository customers) {
    this.customers = customers;
  }

  public Mono<Customer> create(CustomerRequest request) {
    Customer customer = new Customer();
    customer.setFirstName(request.firstName());
    customer.setLastName(request.lastName());
    customer.setEmail(request.email());
    customer.setPhone(request.phone());
    return customers.save(customer).onErrorMap(DataIntegrityViolationException.class,
        error -> new ResponseStatusException(HttpStatus.CONFLICT, "Email is already registered"));
  }

  public Mono<Customer> findById(int id) {
    return customers.findById(id).switchIfEmpty(Mono.error(
        () -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Customer " + id + " not found")));
  }
}

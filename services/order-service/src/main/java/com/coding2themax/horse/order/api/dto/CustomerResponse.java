package com.coding2themax.horse.order.api.dto;

import com.coding2themax.horse.order.domain.Customer;

public record CustomerResponse(Integer id, String firstName, String lastName, String email, String phone) {

  public static CustomerResponse from(Customer customer) {
    return new CustomerResponse(customer.getId(), customer.getFirstName(), customer.getLastName(),
        customer.getEmail(), customer.getPhone());
  }
}

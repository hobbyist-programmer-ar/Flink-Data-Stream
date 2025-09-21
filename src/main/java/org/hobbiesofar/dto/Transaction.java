package org.hobbiesofar.dto;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class Transaction {
    private String transactionId;
    private String productId;
    private String productName;
    private String productCategory;
    private double productPrice;
    private int productQuantity;
    private double totalPrice;
    private String productBrand;
    private String currency;
    private String customerId;
    private String transactionDate;
    private String paymentMethod;
}

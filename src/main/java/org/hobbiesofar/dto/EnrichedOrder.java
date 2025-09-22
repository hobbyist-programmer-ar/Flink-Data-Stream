package org.hobbiesofar.dto;

import lombok.Data;

@Data
public class EnrichedOrder {
    private String productId;
    private String productName;
    private String productCategory;
    private double productPrice;
    private int productQuantity;
    private double totalPrice;
    private String productBrand;
    private String currency;
    private double supplyAndDemandRatio;
}

package org.hobbiesofar.dto;


import lombok.Data;

@Data
public class BOL {
    private String shipmentId;
    private String productId;
    private String productName;
    private String productCategory;
    private int productQuantity;
    private String productBrand;
}

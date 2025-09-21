package org.hobbiesofar.mapper;

import org.apache.flink.api.common.functions.MapFunction;
import org.hobbiesofar.dto.Order;
import org.hobbiesofar.dto.Transaction;

public class OrderMapper implements MapFunction<Transaction, Order> {

    @Override
    public Order map(Transaction transaction) throws Exception {
        Order order = new Order();
        order.setCurrency(transaction.getCurrency());
        order.setProductBrand(transaction.getProductBrand());
        order.setProductCategory(transaction.getProductCategory());
        order.setProductId(transaction.getProductId());
        order.setProductPrice(transaction.getProductPrice());
        order.setProductName(transaction.getProductName());
        order.setTotalPrice(transaction.getTotalPrice());
        return order;
    }
}

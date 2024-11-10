package com.example.bigdata;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AmountPriceValue implements Writable {
    private int amount;
    private double price;

    public AmountPriceValue() {}

    public AmountPriceValue(int amount, double totalPrice) {
        this.amount = amount;
        this.price = totalPrice;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public double getTotalPrice() {
        return price;
    }

    public void setTotalPrice(double totalPrice) {
        this.price = totalPrice;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(amount);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        amount = in.readInt();
        price = in.readDouble();
    }

    @Override
    public String toString() {
        return amount + "\t" + price;
    }
}

package com.example.bigdata;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.*;

public class GeoProducerKey implements WritableComparable<GeoProducerKey> {
    private String geoId;
    private String producer;

    public GeoProducerKey() {}

    public GeoProducerKey(String geoId, String manufacturer) {
        this.geoId = geoId;
        this.producer = manufacturer;
    }

    public String getGeoId() {
        return geoId;
    }

    public String getProducer() {
        return producer;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, geoId);
        WritableUtils.writeString(out, producer);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        geoId = WritableUtils.readString(in);
        producer = WritableUtils.readString(in);
    }

    @Override
    public int compareTo(GeoProducerKey other) {
        int result = geoId.compareTo(other.geoId);
        if (result == 0) {
            result = producer.compareTo(other.producer);
        }
        return result;
    }

    @Override
    public String toString() {
        return geoId + "\t" + producer;
    }

    @Override
    public int hashCode() {
        return geoId.hashCode() * 163 + producer.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        GeoProducerKey other = (GeoProducerKey) obj;
        return geoId.equals(other.geoId) && producer.equals(other.producer);
    }
}


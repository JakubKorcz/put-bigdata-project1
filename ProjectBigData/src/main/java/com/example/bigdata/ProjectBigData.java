package com.example.bigdata;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class ProjectBigData extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new ProjectBigData(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "Car Sales Analysis");
        job.setJarByClass(this.getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(CarSalesMapper.class);
        job.setReducerClass(CarSalesReducer.class);

        job.setMapperClass(CarSalesMapper.class);
        job.setReducerClass(CarSalesReducer.class);

        job.setMapOutputKeyClass(GeoProducerKey.class);
        job.setMapOutputValueClass(AmountPriceValue.class);
        job.setOutputKeyClass(GeoProducerKey.class);
        job.setOutputValueClass(AmountPriceValue.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class CarSalesMapper extends Mapper<Object, Text, GeoProducerKey, AmountPriceValue> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\\^");
            if (fields.length >= 18) {
                String geoId = fields[17].trim();
                String producer = fields[2].trim();
                double price = Double.parseDouble(fields[0].trim());

                GeoProducerKey geoProducerKey = new GeoProducerKey(geoId, producer);
                AmountPriceValue amountPriceValue = new AmountPriceValue(1,price);
                context.write(geoProducerKey, amountPriceValue);
            }
        }
    }

    public static class CarSalesReducer extends Reducer<GeoProducerKey, AmountPriceValue, GeoProducerKey, AmountPriceValue> {
        private AmountPriceValue resultValue = new AmountPriceValue();

        @Override
        protected void reduce(GeoProducerKey key, Iterable<AmountPriceValue> values, Context context) throws IOException, InterruptedException {
            int carCount = 0;
            double totalPrice = 0.0;

            for (AmountPriceValue value : values) {
                carCount += value.getAmount();
                totalPrice += value.getTotalPrice();
            }

            if (carCount >= 10) {
                resultValue.setAmount(carCount);
                resultValue.setTotalPrice(totalPrice);
                context.write(key, resultValue);
            }
        }
    }

}
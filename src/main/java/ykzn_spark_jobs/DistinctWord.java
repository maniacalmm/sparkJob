package ykzn_spark_jobs;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.*;
import org.apache.spark.sql.*;

import java.io.*;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

public class DistinctWord {

    private static void currentTime(double start) {
        System.out.println("elapsed time: " + (System.currentTimeMillis() - start) / 1_000);
    }

    public static void compute(String[] url) {
        System.out.println("-------------------------STARTING------------------------------");
        double start = System.currentTimeMillis();

        SparkSession sparkSession = SparkSession.builder()
                .appName("generic job")
                .master("local")
                .getOrCreate();


        sparkSession.sparkContext().setLogLevel("WARN");

        System.out.println("-----------------starting get blob list----------------------");
        Storage storage = StorageOptions.getDefaultInstance().getService();
        Bucket bucket = storage.get("sparkybucket");
        Page<Blob> buckets = bucket.list();
        for (Blob b : buckets.iterateAll()) {
            System.out.println(b.getBlobId().getName());
        }
        currentTime(start);


        System.out.println("--------------------starting read gs file-------------------");
        Dataset<Row> df = sparkSession.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(url[0] + url[1]);
                //.csv("FileDump/TempFile");
        currentTime(start);

        System.out.println("-------------------starting to distinct and count---------------------");
        Column regionCol = df.col(df.columns()[0]);
        Dataset<Row> distinct = df.select(regionCol).distinct();
        System.out.println("unique number if Id: " + distinct.count());
        currentTime(start);

        System.out.println("-------------------Starting saving to datasource...--------------------");
        distinct.write().csv(url[0] + "output");
        System.out.println("finished writing");
        currentTime(start);

    }
}

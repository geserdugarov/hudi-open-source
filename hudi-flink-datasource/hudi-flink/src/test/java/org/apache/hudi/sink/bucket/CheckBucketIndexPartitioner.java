package org.apache.hudi.sink.bucket;

import org.apache.hudi.common.util.Functions;
import org.apache.hudi.common.util.hash.BucketIndexUtil;
import org.apache.hudi.index.bucket.BucketIdentifier;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CheckBucketIndexPartitioner {

  final static boolean isSkewedKeys = false;

  final static int keysNumber = isSkewedKeys ? 67300 : 250000;
  final static int bucketsNumber = 250;
  final static int writersNumber = 100;
  final static LocalDate start = LocalDate.of(2024, 1, 1);
  final static LocalDate end = LocalDate.of(2024, 12, 31);

  public static void main(String[] args) {
    List<String> days = Stream
        .iterate(start, date -> date.plusDays(1))
        .limit(ChronoUnit.DAYS.between(start, end) + 1)
        .map(date -> {
          String month;
          if (date.getMonthValue() < 10) {
            month = "0" + date.getMonthValue();
          } else {
            month = String.valueOf(date.getMonthValue());
          }
          String day;
          if (date.getDayOfMonth() < 10) {
            day = "0" + date.getDayOfMonth();
          } else {
            day = String.valueOf(date.getDayOfMonth());
          }
          return String.valueOf(date.getYear()) + month + day;
        })
        .collect(Collectors.toList());

    Functions.Function2 partitionFunc = BucketIndexUtil.getPartitionIndexFunc(writersNumber);
    Functions.Function2 partitionFuncOldWay = BucketIndexUtil.getPartitionIndexFuncOldWay(bucketsNumber, writersNumber);

    List<Integer> writerCounter = new ArrayList<>(Collections.nCopies(writersNumber, 0));
    List<Integer> writerCounterOldWay = new ArrayList<>(Collections.nCopies(writersNumber, 0));

    for (int i = 0; i < keysNumber; i++) {
      int bucket = BucketIdentifier.getBucketId(String.valueOf(i), "", bucketsNumber);
      days.stream().forEach(day -> {
        int writerId = (int) partitionFunc.apply(day, bucket);
        writerCounter.set(writerId, writerCounter.get(writerId) + 1);
        writerId = (int) partitionFuncOldWay.apply(day, bucket);
        writerCounterOldWay.set(writerId, writerCounterOldWay.get(writerId) + 1);
      });
    }

    if (isSkewedKeys) {
      for (int i = keysNumber; i < keysNumber * 100; i++) {
        int bucket = BucketIdentifier.getBucketId(String.valueOf(i), "", bucketsNumber);
        days.subList(days.size() - 10, days.size()).stream().forEach(day -> {
          int writerId = (int) partitionFunc.apply(day, bucket);
          writerCounter.set(writerId, writerCounter.get(writerId) + 1);
          writerId = (int) partitionFuncOldWay.apply(day, bucket);
          writerCounterOldWay.set(writerId, writerCounterOldWay.get(writerId) + 1);
        });
      }
    }

    System.out.println("Skewed data: " + isSkewedKeys);
    System.out.println("Fixed distribution: " + writerCounter);
    System.out.println("Current distribution: " + writerCounterOldWay);
  }
}

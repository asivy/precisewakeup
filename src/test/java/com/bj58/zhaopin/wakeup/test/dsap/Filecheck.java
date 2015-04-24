package com.bj58.zhaopin.wakeup.test.dsap;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;

import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;

public class Filecheck {
    public static final SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Test
    public void checkold() {
        try {
            CounterLine counter = new CounterLine();
            Files.readLines(new File("D:/dsap/old.txt"), Charsets.UTF_8, counter);
            System.out.println();
            System.out.println(counter.getResult());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void checkNew() {
        try {
            CounterLine counter = new CounterLine();
            Files.readLines(new File("D:\\dsap\\20150420\\part-r-00004"), Charsets.UTF_8, counter);
            System.out.println();
            System.out.println(counter.getResult());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static class CounterLine implements LineProcessor<Integer> {
        private int rowNum = 0;

        @Override
        public boolean processLine(String line) throws IOException {
            String[] arys = line.split("_");
            if (arys.length != 14) {
                System.out.println(line);
            } else {
                if (!"null".equals(arys[0])) {
                    System.out.println(line);
                }
                try {
                    Long.valueOf(arys[1]);//uid 
                } catch (Exception e) {
                    System.out.println(line);
                }

                try {
                    Integer.valueOf(arys[2]);//cate 
                } catch (Exception e) {
                    System.out.println(line);
                }

                try {
                    Integer.valueOf(arys[3]);//area
                } catch (Exception e) {
                    System.out.println(line);
                }
                try {
                    Integer.valueOf(arys[4]);//time
                } catch (Exception e) {
                    System.out.println(line);
                }
                try {
                    Integer.valueOf(arys[5]);//计数
                } catch (Exception e) {
                    System.out.println(line);
                }
                try {
                    Integer.valueOf(arys[6]);//类别
                } catch (Exception e) {
                    System.out.println(line);
                }

                try {
                    Long.valueOf(arys[7]);//bbid
                } catch (Exception e) {
                    System.out.println(line);
                }

                if (".000000".equals(arys[8])) {
                    System.out.println(line);
                }

                if (".000000".equals(arys[9])) {
                    System.out.println(line);
                }

                try {
                    sdf1.parse(arys[10]);
                } catch (Exception e) {
                    System.out.println(line);
                }

            }

            rowNum++;
            return true;
        }

        @Override
        public Integer getResult() {
            return rowNum;
        }
    }
}

package com.bj58.zhaopin.wakeup.test.data;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import com.google.common.io.Files;

public class FileTest {

    @Test
    public void test() {
        try {
            File file = new File("data/precise/active.log");
            File file1 = new File("data/precise/done.log");
            if (file.exists()) {
                Files.touch(file);
            } else {
                Files.createParentDirs(file);
                file.createNewFile();
            }
            if (file1.exists()) {
                Files.touch(file1);
            } else {
                Files.createParentDirs(file);
                file1.createNewFile();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

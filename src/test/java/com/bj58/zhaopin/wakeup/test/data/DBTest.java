package com.bj58.zhaopin.wakeup.test.data;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;
import java.util.concurrent.ConcurrentNavigableMap;

import org.apache.poi.ss.formula.functions.T;
import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import com.google.common.io.Files;

public class DBTest {

    @Test
    public void mapdb() {
        try {
            DB db = DBMaker.newFileDB(new File("D:\\test.txt")).make();
            ConcurrentNavigableMap map = db.getTreeMap("precise");
            map.put("userid", "sb");
            System.out.println("finish");
            db.commit();
            db.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void update() {
        try {
            DB db = DBMaker.newFileDB(new File("D:\\test.txt")).make();
            ConcurrentNavigableMap map = db.getTreeMap("test");
            map.put("userid", "bj58");
            System.out.println("finish");
            db.commit();
            db.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void get() {
        try {
            DB db = DBMaker.newFileDB(new File("D:\\test.txt")).make();
            ConcurrentNavigableMap map = db.getTreeMap("test");
            System.out.println(String.format("resume is: %s", map.get("userid")));
            db.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void delete() {
        try {
            DB db = DBMaker.newFileDB(new File("D:\\test.txt")).make();
            ConcurrentNavigableMap map = db.getTreeMap("test");
            map.remove("userid");
            System.out.println("finish remove");
            db.commit();
            db.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testtouch() {
        try {
            Files.touch(new File("data\\dmeo\\active.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getallactive() {
        try {
            File file = new File("data\\fulltest\\active.log");
            DB db = DBMaker.newFileDB(file).commitFileSyncDisable().asyncWriteFlushDelay(100).asyncWriteEnable().asyncWriteQueueSize(100).closeOnJvmShutdown().make();
            ConcurrentNavigableMap<T, Long> map = db.getTreeMap("fulltest");
            for (Object t : map.keySet()) {
                System.out.println(t + " " + map.get(t));
            }
            System.out.println();
            System.out.println(map.size());
            db.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getalldone() {
        try {
            DB db = DBMaker.newFileDB(new File("data\\dmeo\\done.log")).closeOnJvmShutdown().make();
            ConcurrentNavigableMap map = db.getTreeMap("precise");
            for (Object t : map.keySet()) {
                System.out.println(t + " " + map.get(t));
            }
            db.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void time() {
        System.out.println(Calendar.getInstance().get(Calendar.MINUTE));
    }

}

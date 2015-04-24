package wu.tong.precise.wakeup.data;

import java.io.File;
import java.util.Calendar;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import com.google.common.io.Files;

public class DBClient {
    static final Logger logger = Logger.getLogger(DBClient.class);

    public static String datafile = "data";
    public static String activefile = "active";
    public static String donefile = "done";
    public static ReentrantLock activeLock = new ReentrantLock();
    public static ReentrantLock doneLock = new ReentrantLock();
    static ReentrantLock lock = new ReentrantLock();

    static final ConcurrentHashMap<String, DB> topicMap = new ConcurrentHashMap<String, DB>();

    //    public static DB getMapDB(String topic, String subTopic, boolean active) {
    //        DB activeDB = null;
    //
    //        StringBuilder filename = new StringBuilder(50);
    //        filename.append(datafile).append(File.separator).append(topic).append(File.separator).append(subTopic).append(File.separator);
    //        if (active) {
    //            filename.append(activefile).append(".log");
    //        } else {
    //            filename.append(donefile).append(".log");
    //        }
    //        String name = filename.toString();
    //        if (topicMap.get(name) != null) {
    //            return topicMap.get(name);
    //        }
    //        try {
    //            lock.tryLock(1, TimeUnit.SECONDS);
    //            File file = new File(name);
    //            if (file.exists()) {
    //                Files.touch(file);
    //            } else {
    //                Files.createParentDirs(file);
    //                file.createNewFile();
    //            }
    //            activeDB = DBMaker.newFileDB(file).asyncWriteFlushDelay(1000).asyncWriteEnable().asyncWriteQueueSize(100).make();
    //            topicMap.put(name, activeDB);
    //            return activeDB;
    //        } catch (Exception e) {
    //            logger.error("active file", e);
    //        } finally {
    //            lock.unlock();
    //        }
    //
    //        return null;
    //    }
    //
    //    public static <T> boolean isExist(T t, String topic, String subTopic, boolean active) {
    //        boolean b = false;
    //        DB db = null;
    //        try {
    //            db = getMapDB(topic, subTopic, active);
    //            if (db == null) {
    //                return b;
    //            }
    //            ConcurrentNavigableMap<T, Long> map = db.getTreeMap(topic);
    //            b = map.containsKey(t);
    //        } catch (Exception e) {
    //            logger.error(" is exist ", e);
    //        } finally {
    //            //            if (db != null) {
    //            //                db.close();
    //            //            }
    //        }
    //        return b;
    //    }
    //
    //    public static <T> void putActive(T t, long removeTime, String topic, String subTopic, boolean active) {
    //        DB db = null;
    //        try {
    //            db = getMapDB(topic, subTopic, active);
    //            if (db == null) {
    //                return;
    //            }
    //            ConcurrentNavigableMap<T, Long> map = db.getTreeMap(topic);
    //            map.put(t, removeTime);
    //            int min = Calendar.getInstance().get(Calendar.MINUTE);
    //            //            if (min % 2 == 0) {
    //            //                activeLock.tryLock(200, TimeUnit.MILLISECONDS);
    //            //                db.commit();
    //            //                db.compact();
    //            //            }
    //        } catch (Exception e) {
    //            logger.error(" put active   ", e);
    //        } finally {
    //            //            activeLock.unlock();
    //            //            if (db != null) {
    //            //                db.close();
    //            //            }
    //        }
    //    }
    //
    //    public static <T> void removeActive(T t, String topic, String subTopic, boolean active) {
    //        DB db = null;
    //        try {
    //            db = getMapDB(topic, subTopic, active);
    //            if (db == null) {
    //                return;
    //            }
    //            ConcurrentNavigableMap<T, Long> map = db.getTreeMap(topic);
    //            map.remove(t);
    //        } catch (Exception e) {
    //            logger.error(" remove active ", e);
    //        } finally {
    //            //            if (db != null) {
    //            //                db.close();
    //            //            }
    //        }
    //    }
    //
    //    //获取所有未完成的活跃数据  初始化时使用
    //    public static <T> Map<T, Long> getAllActive(String topic, String subTopic, boolean active) {
    //        DB db = null;
    //        try {
    //            db = getMapDB(topic, subTopic, active);
    //            if (db == null) {
    //                return null;
    //            }
    //            ConcurrentNavigableMap<T, Long> map = db.get(topic);
    //            return new TreeMap<T, Long>(map);
    //        } catch (Exception e) {
    //            logger.error(" all active ", e);
    //        } finally {
    //            //            if (db != null) {
    //            //                db.close();
    //            //            }
    //        }
    //        return null;
    //    }
    //
    //    public static <T> void putDone(T t, long removeTime, String topic, String subTopic, boolean active) {
    //        DB db = null;
    //        try {
    //            db = getMapDB(topic, subTopic, active);
    //            if (db == null) {
    //                return;
    //            }
    //            ConcurrentNavigableMap<T, Long> map = db.getTreeMap(topic);
    //            map.put(t, removeTime);
    //            int min = Calendar.getInstance().get(Calendar.MINUTE);
    //
    //        } catch (Exception e) {
    //            logger.error(" put done ", e);
    //        } finally {
    //            //            if (db != null) {
    //            //                db.close();
    //            //            }
    //        }
    //    }
}

package wu.tong.precise.wakeup.core;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import wu.tong.precise.wakeup.handler.DefaultHandler;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;

/**
 * 消息的存储及处理类
 * 遵循Reactor模型
 * 
 * 初始化时启动一个单线程  读取消息   具体的处理过程 在handler 中
 * @author Ivy
 * @version 1.0
 * @date  2015年4月14日 下午7:10:03
 * @see 
 * @since
 * @param <K>
 * @param <V>
 */
public class LiveCache<T> {

    private static final Logger logger = Logger.getLogger(LiveCache.class);

    private DelayQueue<LiveEntity<T>> queue;
    static final ExecutorService reactor = Executors.newSingleThreadExecutor();//读取消息线程池
    static final ExecutorService worker = Executors.newCachedThreadPool();//消息的处理线程池

    static final ReentrantLock activeLock = new ReentrantLock();
    static final ReentrantLock doneLock = new ReentrantLock();
    public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    MessageHandler handler = null;
    private String topic;
    private String subTopic;
    private CountDownLatch latch;

    volatile boolean hasretry = false;

    DB activeDB = null;//
    DB doneDB = null;//

    public LiveCache(MessageHandler handler, int size, String topic, String subTopic, CountDownLatch latch) {
        if (handler == null) {
            handler = new DefaultHandler();
        }
        this.topic = topic;
        this.subTopic = subTopic;
        this.handler = handler;
        this.latch = latch;
        queue = new DelayQueue<LiveEntity<T>>();
        Thread t = new Thread() {
            @Override
            public void run() {
                readMessage();
            }
        };
        t.setDaemon(true);
        reactor.submit(t);
        activeDB = getMapDB(true);
        doneDB = getMapDB(false);
        latch.countDown();
        logger.info(String.format("LiveCache %s %s init %s ", topic, subTopic, sdf.format(new Date())));
    }

    /**
     * 放入一个延时实体类
     * 需要同时更新本地磁盘缓存
     * @param entity
     */
    public void put(LiveEntity<T> entity, boolean fromFile) {
        try {
            if (!entity.isValid()) {
                logger.info(String.format(" [invalid] data %s ", entity.toString()));
                return;
            }
            if (!fromFile) {
                try {
                    activeLock.tryLock(200, TimeUnit.MILLISECONDS);//此处不一定能获取到锁  再unlock时可能出错
                    if (isExist(entity.getT())) {
                        queue.remove(entity);
                        removeActive(entity.getT());
                    }
                    putActive(entity.getT(), entity.getRemoveTime());
                    queue.put(entity);
                } catch (Exception e) {
                    logger.error(e);
                } finally {
                    activeLock.unlock();
                }
            } else {
                queue.put(entity);
                logger.info(String.format(" [recover] data %s ", entity.toString()));
            }
        } catch (Exception e) {
            logger.error("put", e);
        }
    }

    /**
     * 阻塞读取消息 并处理
     * 
     */
    public void readMessage() {
        while (true) {
            final LiveEntity<T> liveEntity = queue.poll();//poll会阻塞
            if (liveEntity != null) {
                worker.submit(new Runnable() {
                    @Override
                    public void run() {
                        handler.handle(liveEntity);
                        try {
                            activeLock.tryLock(100, TimeUnit.MILLISECONDS);
                            removeActive(liveEntity.getT());
                        } catch (Exception e) {
                            e.printStackTrace();
                        } finally {
                            activeLock.unlock();
                        }
                        try {
                            doneLock.tryLock(100, TimeUnit.MILLISECONDS);
                            putDone(liveEntity.getT(), liveEntity.getRemoveTime());
                        } catch (Exception e) {
                            e.printStackTrace();
                        } finally {
                            doneLock.unlock();
                        }
                    }
                });
            }
        }
    }

    //数据处理部分
    public static String datafile = "data";
    public static String activefile = "active";
    public static String donefile = "done";

    private DB getMapDB(boolean active) {
        DB db = null;
        StringBuilder filename = new StringBuilder(50);
        filename.append(datafile).append(File.separator).append(topic).append(File.separator).append(subTopic).append(File.separator);
        if (active) {
            filename.append(activefile).append(".log");
        } else {
            filename.append(donefile).append(".log");
        }
        String name = filename.toString();
        File file = null;
        try {
            file = new File(name);
            if (file.exists()) {
                Files.touch(file);
            } else {
                Files.createParentDirs(file);
                file.createNewFile();
            }
            db = DBMaker.newFileDB(file).asyncWriteFlushDelay(100).asyncWriteQueueSize(100).make();
        } catch (Throwable e) {
            //e.printStackTrace();
            //logger.error("active file", e);
            reBuildDataFile(file);
        }

        return db;
    }

    //可能因kill -9 pid 导致文件格式错误   此时就把原文件删除 并新建一个
    private void reBuildDataFile(File file) {
        try {
            logger.info("[Warnning] rebuild file " + file.getPath() + File.separator + file.getName());
            file.delete();
            file.createNewFile();
            synchronized (LiveCache.class) {
                if (!hasretry) {
                    hasretry = true;
                    getMapDB(true);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public <T> boolean isExist(T t) {
        boolean b = false;
        Preconditions.checkNotNull(activeDB, String.format(" %s %s active is null", topic, subTopic));
        try {
            ConcurrentNavigableMap<T, Long> map = activeDB.getTreeMap(topic);
            b = map.containsKey(t);
        } catch (Exception e) {
            logger.error(" is exist ", e);
        }
        return b;
    }

    public <T> void putActive(T t, long removeTime) {
        Preconditions.checkNotNull(activeDB, String.format(" %s %s active is null", topic, subTopic));
        try {
            ConcurrentNavigableMap<T, Long> map = activeDB.getTreeMap(topic);
            map.put(t, removeTime);
        } catch (Exception e) {
            logger.error(" put active   ", e);
        }
    }

    public <T> void removeActive(T t) {
        Preconditions.checkNotNull(activeDB, String.format(" %s %s active is null", topic, subTopic));
        try {
            ConcurrentNavigableMap<T, Long> map = activeDB.getTreeMap(topic);
            map.remove(t);
        } catch (Exception e) {
            logger.error(" remove active ", e);
        }
    }

    //获取所有未完成的活跃数据  初始化时使用
    public <T> Map<T, Long> getAllActive() {
        Preconditions.checkNotNull(activeDB, String.format(" %s %s active is null", topic, subTopic));
        try {
            ConcurrentNavigableMap<T, Long> map = activeDB.get(topic);
            return new TreeMap<T, Long>(map);
        } catch (Exception e) {
            logger.error(" all active ", e);
        }
        return null;
    }

    public <T> void putDone(T t, long removeTime) {
        Preconditions.checkNotNull(doneDB, String.format(" %s %s active is null", topic, subTopic));
        try {
            ConcurrentNavigableMap<T, Long> map = doneDB.getTreeMap(topic);
            map.put(t, removeTime);
        } catch (Exception e) {
            logger.error(" put done ", e);
        }
    }

    public String getSubTopic() {
        return subTopic;
    }

    public String getTopic() {
        return topic;
    }

    public int getSize() {
        return queue.size();
    }

    public DB getActiveDB() {
        return activeDB;
    }

    public DB getDoneDB() {
        return doneDB;
    }

}

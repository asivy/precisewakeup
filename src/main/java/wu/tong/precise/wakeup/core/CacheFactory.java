package wu.tong.precise.wakeup.core;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.mapdb.DB;

import wu.tong.precise.wakeup.main.PreciseWakeUpMain;

/**
 * 提供静态工厂   维护所有cache 
 * 一个主题 对应4 8 16 32个cache 这样可以搞高QPS
 * 
 * 
 * @author Ivy
 * @version 1.0
 * @date  2015年4月15日 下午1:45:56
 * @see 
 * @since
 */
public class CacheFactory<T> {

    private String topic = "demo";
    private int coreSize = 8;
    private MessageHandler handler;
    volatile boolean hasStart = false;

    public static final Logger logger = Logger.getLogger(PreciseWakeUpMain.class);
    public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    List<LiveCache> cacheList = new ArrayList<LiveCache>();
    List<DB> dbList = new ArrayList<DB>();

    protected CacheFactory<T> getThis() {
        return (CacheFactory<T>) this;
    }

    public CacheFactory<T> topic(String topic) {
        this.topic = topic;
        return getThis();
    }

    public CacheFactory<T> handler(MessageHandler handler) {
        this.handler = handler;
        return getThis();
    }

    public CacheFactory<T> coreSize(int size) {
        this.coreSize = size;
        return getThis();
    }

    //
    @SuppressWarnings("unchecked")
    public void putFromMQ(LiveEntity entity) throws Exception {
        //        Preconditions.checkArgument(hasStart, "factory not start");
        //        Preconditions.checkNotNull(entity, "entity is null");
        //        Preconditions.checkNotNull(entity.getT(), " T is null");
        int index = index((T) entity.getT());
        cacheList.get(index).put(entity, false);
    }

    @SuppressWarnings("unchecked")
    public void putFromDB(LiveEntity entity) throws Exception {
        //        Preconditions.checkArgument(hasStart, "factory not start");
        //        Preconditions.checkNotNull(entity, "entity is null");
        //        Preconditions.checkNotNull(entity.getT(), " T is null");
        int index = index((T) entity.getT());
        cacheList.get(index).put(entity, true);
    }

    //启动真正的服务  只能启动一次
    public synchronized void start() {
        if (hasStart) {
            return;
        }
        CountDownLatch latch = new CountDownLatch(coreSize);
        try {
            logger.info(String.format(" [start] CacheFactory topic = %s  init at  %s ", topic, sdf.format(new Date())));
            for (int i = 0; i < coreSize; i++) {
                LiveCache<T> cache = new LiveCache<T>(handler, 16, topic, topic + i, latch);
                cacheList.add(cache);
                //                dbList.add(cache.getActiveDB());
                //                dbList.add(cache.getDoneDB());
            }
            latch.await();
            hasStart = true;
            //            recoverDate();
            autoCommit();
            addhook();
            logger.info(String.format(" [end] CacheFactory started %s ", sdf.format(new Date())));
        } catch (Exception e) {
            logger.error(" start error ", e);
        }
    }

    //检查是否有些主题的历史数据  有的话就先恢复
    //    private void recoverDate() {
    //        //判断topic对应的目录是否存在
    //        //如果存在 则
    //        List<String> topics = new ArrayList<String>();
    //        for (String top : topics) {
    //            Map<Long, Long> map = DBClient.getAllActive(top);
    //            if (map != null) {
    //                for (Long t : map.keySet()) {
    //                    LiveEntity<Long> entity = new LiveEntity<Long>(t, map.get(t), true);
    //                    try {
    //                        putFromDB(entity);
    //                    } catch (Exception e) {
    //                        e.printStackTrace();
    //                    }
    //                }
    //                logger.info(" finish recover data from file " + map.keySet().size());
    //            }
    //        }
    //    }

    //定时commit数据
    private void autoCommit() {
        Random r = new Random();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(coreSize);
        for (LiveCache cache : cacheList) {
            scheduler.scheduleAtFixedRate(new commitThread(cache), Math.abs(r.nextInt()) % coreSize, 30l, TimeUnit.SECONDS);
        }
    }

    public int index(T t) {
        int val = (t.hashCode() & 0x7FFFFFFF) % coreSize;
        return val;
    }

    /**
     *  程序结束时清理现场
     * 1 程序正常退出
     * 2 使用system.exit退出
     * 3 使用ctrl+c 退出
     * 4 系统关闭
     * 5 使用kill pid 关掉进程  kill -9 pid除外
     */
    private void addhook() {
        for (LiveCache cache : cacheList) {
            Runtime.getRuntime().addShutdownHook(new ClearThread(cache));
        }
    }

    class ClearThread extends Thread {
        LiveCache myCache;

        public ClearThread(LiveCache cache) {
            this.myCache = cache;
        }

        @Override
        public void run() {
            logger.info("执行 hook ");
            try {
                DB db = myCache.getActiveDB();
                if (db != null) {
                    db.commit();
                    db.compact();
                    db.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            try {
                DB cdb = myCache.getDoneDB();
                if (cdb != null) {
                    cdb.commit();
                    cdb.compact();
                    cdb.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    class commitThread implements Runnable {
        LiveCache myCache;

        public commitThread(LiveCache cache) {
            this.myCache = cache;
        }

        @Override
        public void run() {
            try {
                DB db = myCache.getActiveDB();
                if (db != null) {
                    db.commit();
                    db.compact();
                    //  logger.info(String.format(" %s %s activedb commit %s", myCache.getTopic(), myCache.getSubTopic(), sdf.format(new Date())));
                }
            } catch (Exception e) {
                logger.error(myCache.getSubTopic() + " active ", e);
            }
            try {
                DB db = myCache.getDoneDB();
                if (db != null) {
                    db.commit();
                    db.compact();
                    //   logger.info(String.format(" %s %s doneDB commit %s ", myCache.getTopic(), myCache.getSubTopic(), sdf.format(new Date())));
                }
            } catch (Exception e) {
                logger.error(myCache.getSubTopic() + " done ", e);
            }
        }
    }

}

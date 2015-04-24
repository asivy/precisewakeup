package wu.tong.precise.wakeup.main;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import wu.tong.precise.wakeup.core.CacheFactory;
import wu.tong.precise.wakeup.core.LiveEntity;

import com.bj58.zhaopin.app.engine.context.AppContext;
import com.bj58.zhaopin.app.engine.task.AppTask;

public class PreciseWakeUpMain implements AppTask {

    public static final Logger logger = Logger.getLogger(PreciseWakeUpMain.class);
    public static final SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static final SimpleDateFormat sdfSimpl = new SimpleDateFormat("HH:mm:ss");

    final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    static final CountDownLatch countdown = new CountDownLatch(1);
    static final ExecutorService fixExecutor = Executors.newFixedThreadPool(50);

    static CacheFactory factory;

    @Override
    public void init(AppContext context) {
        //初始化配置    恢复之前缓存的数据
    }

    @Override
    public void destory() {

    }

    @Override
    public void start() {
    }

    public static void main(String[] args) {
        try {
            factory = new CacheFactory<Long>().coreSize(8).topic("test");
            factory.start();

            Thread.sleep(3000);

            PreciseWakeUpMain precise = new PreciseWakeUpMain();
            precise.datamock();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //从文件中恢复数据

    private void bigTest() {
        final Random r = new Random();
        final AtomicInteger cnt = new AtomicInteger();
        for (int i = 0; i < 80; i++) {
            fixExecutor.execute(new Runnable() {
                @SuppressWarnings({ "unchecked", "rawtypes" })
                @Override
                public void run() {
                    while (true) {
                        //                    if (cnt.get() > 200) {
                        //                        System.exit(1);
                        //                        return;
                        //                    }
                        LiveEntity<Long> entity = new LiveEntity(Math.abs(r.nextLong() % 10000000), Math.abs(r.nextLong() % 1000) * 1000l, false);
                        logger.info(String.format(" %d %s %s ", cnt.get(), sdf1.format(Calendar.getInstance().getTime()), entity.toString()));
                        cnt.getAndIncrement();
                        System.out.println(String.format(" %d %s %s %s ", cnt.get(), Thread.currentThread().getName(), sdf1.format(Calendar.getInstance().getTime()), entity.toString()));
                        try {
                            factory.putFromMQ(entity);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }
    }

    // 生成一些测试数据
    private void datamock() {
        System.out.println("开始压测");
        final Random r = new Random();
        final AtomicInteger cnt = new AtomicInteger();
        try {
            for (int i = 0; i < 1; i++) {
                scheduler.scheduleAtFixedRate(new Runnable() {
                    @SuppressWarnings({ "unchecked", "rawtypes" })
                    @Override
                    public void run() {
                        LiveEntity<Long> entity = new LiveEntity(Math.abs(r.nextLong() % 10000000), Math.abs(r.nextLong() % 1000) * 1000l, false);
                        cnt.getAndIncrement();
                        if (cnt.get() % 1000 == 0) {
                            System.out.println(String.format(" %d %s %s ", cnt.get(), Thread.currentThread().getName(), sdf1.format(Calendar.getInstance().getTime())));
                        }
                        try {
                            factory.putFromMQ(entity);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        //  System.out.println(String.format(" %d %s %s %s ", cnt.get(), Thread.currentThread().getName(), sdf1.format(Calendar.getInstance().getTime()), entity.toString()));
                        //                        if (cnt.get() > 5000) {
                        //                            System.exit(1);
                        //                        }
                    }
                }, 0l, 1l, TimeUnit.MILLISECONDS);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

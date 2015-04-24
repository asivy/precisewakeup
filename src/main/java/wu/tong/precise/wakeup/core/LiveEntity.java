package wu.tong.precise.wakeup.core;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * 存活的时间实体对象    处理实体的类在livecache中解耦
 * 
 * @author Ivy
 * @version 1.0
 * @date  2015年4月14日 下午4:04:32
 * @see 
 * @since
 * @param <T>  
 */
public class LiveEntity<T> implements Delayed {

    public static final SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private T t; //key 以long int string 为主
    private long liveTime; //存活多长时间,初始化时会用到这个时间 
    private long removeTime;//移除时间=当前时间+存活时间，这个是真正起作用的时间，当存到本地时只需要存这个时间就可以
    private boolean valid = true;

    public LiveEntity(T t, long time, boolean bfile) {
        if (!bfile) {
            this.liveTime = time;
            this.removeTime = TimeUnit.MILLISECONDS.convert(time, TimeUnit.MILLISECONDS) + System.currentTimeMillis();
        } else {
            long ttl = time - System.currentTimeMillis();
            if (ttl < 0) {
                this.valid = false;
            }
            this.liveTime = ttl;
            this.removeTime = time;
        }
        this.setT(t);
    }

    @SuppressWarnings("unchecked")
    @Override
    public int compareTo(Delayed o) {
        if (o == null)
            return 1;
        if (o == this)
            return 0;
        if (o instanceof LiveEntity) {
            LiveEntity<T> tmpDelayedItem = (LiveEntity<T>) o;
            if (liveTime > tmpDelayedItem.liveTime) {
                return 1;
            } else if (liveTime == tmpDelayedItem.liveTime) {
                return 0;
            } else {
                return -1;
            }
        }
        long diff = getDelay(TimeUnit.MILLISECONDS) - o.getDelay(TimeUnit.MILLISECONDS);
        return diff > 0 ? 1 : diff == 0 ? 0 : -1;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(removeTime - System.currentTimeMillis(), unit);
    }

    public T getT() {
        return t;
    }

    public void setT(T t) {
        this.t = t;
    }

    public long getLiveTime() {
        return liveTime;
    }

    public void setLiveTime(long liveTime) {
        this.liveTime = liveTime;
    }

    public long getRemoveTime() {
        return removeTime;
    }

    public void setRemoveTime(long removeTime) {
        this.removeTime = removeTime;
    }

    @Override
    public int hashCode() {
        return t.hashCode();
    }

    public boolean isValid() {
        return valid;
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof LiveEntity) {
            return object.hashCode() == hashCode() ? true : false;
        }
        return false;
    }

    @Override
    public String toString() {
        return String.format("LiveEntity [t=%s, liveTime= %s , removeTime =%s  ]", t, liveTime, sdf1.format(new Date(removeTime)));
    }

}

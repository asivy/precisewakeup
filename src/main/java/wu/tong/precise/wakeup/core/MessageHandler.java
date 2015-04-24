package wu.tong.precise.wakeup.core;

import java.text.SimpleDateFormat;

import org.apache.log4j.Logger;

public abstract class MessageHandler {
    public static final SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static final Logger logger = Logger.getLogger(MessageHandler.class);

    public abstract <T> void handle(final LiveEntity<T> entity);

}

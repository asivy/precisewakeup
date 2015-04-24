package wu.tong.precise.wakeup.handler;

import java.util.Calendar;

import wu.tong.precise.wakeup.core.LiveEntity;
import wu.tong.precise.wakeup.core.MessageHandler;

public class DefaultHandler extends MessageHandler {

    @Override
    public <T> void handle(LiveEntity<T> entity) {
        //        logger.info(String.format("default handle message %s %s ", sdf1.format(Calendar.getInstance().getTime()), entity.toString()));
    }

}

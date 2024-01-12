package Order;

import java.text.SimpleDateFormat;

public class KeyUtil {
    public static String buildKey(Order order) {
        String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(order.getOrderTime());
        return order.getUsername()+"--" + order.getProduct()+"--" + order.getOrderId() + "--下单时间:" + date;
        // 注释代码是模拟同一个key 仅存在一个定时器，执行时间后覆盖前
        //return order.getUsername();
    }
}

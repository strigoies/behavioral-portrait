package Order;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class MyOrderSource extends RichSourceFunction<Order> {
    private Boolean flag = true;
    private final String[] products = new String[]{"黄焖鸡米饭", "北京烤鸭", "桥头排骨"};
    private final String[] users = new String[]{"马邦德", "黄四郎", "张麻子"};
    AtomicInteger num;

    @Override
    public void run(SourceContext<Order> ctx) throws Exception {
//        while (flag) {
//            Order order = Order.builder()
//                    .product(products[3])
//                    .username(users[3])
//                    .orderId(UUID.randomUUID().toString().replace("-", "."))
//                    .orderTime(System.currentTimeMillis())
//                    .build();
//            Thread.sleep(5000);
//            // 注释代码是模拟同一个key 仅存在一个定时器，执行时间后覆盖前
//            //if (num.get()<4) {
//            Thread.sleep(5000);
//        }else {
//            Thread.sleep(500000);
//        }
//        num.incrementAndGet();
//        ctx.collect(order);
    }


    @Override
    public void cancel() {
        flag = false;
    }
}

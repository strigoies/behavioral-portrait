package Order;

import Order.Order;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TimeTest {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Order> streamSource = env.addSource(new MyOrderSource());
        DataStream<Object> stream = streamSource
                //根据订单分组
                .keyBy(KeyUtil::buildKey)
                .process(new OrderSettlementProcess(120 * 1000L));
        stream.printToErr();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


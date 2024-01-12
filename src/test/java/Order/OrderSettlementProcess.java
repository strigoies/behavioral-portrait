package Order;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Map;

/**
 * @author lei
 * @version 1.0
 * @date 2021/3/21 22:17
 * @desc 模拟订单自动评价流程计算
 */
public class OrderSettlementProcess extends KeyedProcessFunction<String, Order, Object> {
    private final Long overTime;
    MapState<String, Long> productState;
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public OrderSettlementProcess(Long overTime) {
        this.overTime = overTime;
    }

    /**
     * 数据处理
     *
     * @param currentOrder
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processElement(Order currentOrder, Context ctx, Collector<Object> out) throws Exception {
        long time = currentOrder.getOrderTime() + this.overTime;
        //注册一个处理时间定时器 定时器触发时间为 value.getOrderTime() + this.overTime
        ctx.timerService().registerProcessingTimeTimer(time);
        // 注释代码是模拟同一个key 仅存在一个定时器，执行时间后覆盖前(将前边的定时器移除)
        //if (productState.contains(ctx.getCurrentKey())) {
        //    ctx.timerService().deleteProcessingTimeTimer(productState.get(ctx.getCurrentKey()));
        //}
        productState.put(ctx.getCurrentKey(), time);
        System.out.println(KeyUtil.buildKey(currentOrder) + " 订单过期时间为:" + time + " :" + df.format(time));
    }

    /**
     * 定时任务触发
     *
     * @param timestamp 就是上文设置的    ctx.timerService().registerProcessingTimeTimer(time); 时间戳
     * @param ctx       上下文环境  可获取当前Process的 分组key /设置定时器等
     * @param out       数据收集
     * @throws Exception
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        System.out.println("定时任务执行:" + timestamp + ":" + ctx.getCurrentKey());
        Iterator<Map.Entry<String, Long>> orderIterator = productState.iterator();
        if (orderIterator.hasNext()) {
            Map.Entry<String, Long> orderEntry = orderIterator.next();
            String key = orderEntry.getKey();
            Long expire = orderEntry.getValue();
            //模拟调用查询订单状态
            if (!isEvaluation(key) && expire == timestamp) {
                //todo 数据收集
                System.err.println(key + ">>>>> 超过订单未评价且超过最大评价时间，默认设置五星好评！");
            } else {
                System.out.println(key + "订单已评价！");
            }

        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //定义map存储状态
        MapStateDescriptor<String, Long> mapStateDescriptor = new MapStateDescriptor<>("productState",
                TypeInformation.of(String.class),
                TypeInformation.of(Long.class));
        productState = getRuntimeContext().getMapState(mapStateDescriptor);
    }

    public Boolean isEvaluation(String orderKey) {
        //todo 查询订单状态
        return false;
    }
}


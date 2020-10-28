import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.Collector;
import org.lionsoul.ip2region.DbConfig;
import org.lionsoul.ip2region.DbMakerConfigException;
import org.lionsoul.ip2region.DbSearcher;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Snowson
 * @date 2020/10/16 17:54
 */
public class Start {
    public static void main(String[] args) throws Exception {
        unKeyedTask();
    }

    private static void unKeyedTask() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(5);

        final TransactionSource source = new TransactionSource();
        DataStream<Transaction> transactions = env
                .addSource(source)
                .name("transactions");

        transactions.map(new Calculate())
                .setParallelism(5)
                .name("count");

        env.execute();
    }

    private static void keyedTask() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(5);

        final TransactionSource source = new TransactionSource();
        DataStream<Transaction> transactions = env
                .addSource(source)
                .name("transactions");

        transactions
                .keyBy(Transaction::getAccountId)
                .process(new FraudDetector())
                .addSink(new PrintSinkFunction<>())
                .name("fraud-detector");

        env.execute("Fraud Detection");
    }
}

/**
 * non-keyed stream with state
 */
class Calculate extends RichMapFunction<Transaction, Integer> implements CheckpointedFunction {


    private ListState<Tuple2<String, Integer>> listState;
    private List<Tuple2<String, Integer>> bufferedElements = new ArrayList<>();

    @Override
    public Integer map(Transaction value) throws Exception {
        System.out.println(Thread.currentThread().getName() + ": " + listState.get());
        listState.add(new Tuple2<>(value.getAccountId(), value.getValue()));
        return 0;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        System.out.println("snapshotState.");
        listState.clear();
        for (Tuple2<String, Integer> element : bufferedElements) {
            listState.add(element);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        System.out.println("initState.");
        final ListStateDescriptor<Tuple2<String, Integer>> stateDescriptor = new ListStateDescriptor<>(
                "buffered-elements",
                TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                }));
        listState = context.getOperatorStateStore().getListState(stateDescriptor);
        if (context.isRestored()) {
            for (Tuple2<String, Integer> item : listState.get()) {
                bufferedElements.add(item);
            }
        }
    }
}

/**
 * keyed stream with state
 */
class FraudDetector extends KeyedProcessFunction<String, Transaction, String> {

    private DbSearcher searcher;
    private ValueState<Integer> record;
    private Integer stateNormal;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        final ValueStateDescriptor<Integer> value = new ValueStateDescriptor<>("record", Integer.class);
        record = getRuntimeContext().getState(value);
        System.out.println(Thread.currentThread().getName() + ": init");


//        if (searcher == null) {
//            initCache();
//        }
    }

    @Override
    public void processElement(Transaction value, Context ctx, Collector<String> out) throws Exception {
        if (record.value() == null) {
            record.update(0);
        }
        if (stateNormal == null) {
            stateNormal = 0;
        }
        record.update(record.value() + 1);

        System.out.println(Thread.currentThread().getName() + ", record: " + record.value() + ", stateNormal: " + ++stateNormal);


        //        System.out.println(Thread.currentThread().getName() + ": " + searcher.memorySearch("172.17.162.101"));
    }


    private void initCache() throws IOException, DbMakerConfigException {
        System.out.println(Thread.currentThread().getName() + "  init ip2region.");
        final InputStream is = getClass().getResourceAsStream("ip2region.db");
        byte[] bytes = new byte[is.available()];
        is.read(bytes);
        searcher = new DbSearcher(new DbConfig(), bytes);
    }
}

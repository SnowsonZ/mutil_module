import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @author Snowson
 * @date 2020/10/16 18:00
 */
public class TransactionSource implements SourceFunction<Transaction> {
    private static final long serialVersionUID = -1021342604647022268L;
    private boolean isRunning = true;

    @Override
    public void run(SourceContext<Transaction> ctx) throws Exception {
        final Random random = new Random();
        while (isRunning) {
            ctx.collect(new Transaction(String.valueOf(random.nextInt(10)), random.nextInt(100)));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}

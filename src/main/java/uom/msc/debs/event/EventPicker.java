package uom.msc.debs.event;

import com.google.common.base.Splitter;
import org.apache.commons.collections.IteratorUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.wso2.siddhi.core.event.Event;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.List;

public class EventPicker {

    private static final Logger logger = LogManager.getLogger(EventPicker.class);

    private static final Splitter SPLITTER = Splitter.on(',');

    private EventStore eventStore;
    private String filePath;
    private long pointer;
    private final int chunkCount = 10;

    public EventPicker(String filePath, EventStore eventStore) {
        this.filePath = filePath;
        this.eventStore = eventStore;
    }

    public void pick() throws IOException {
        pointer = 0;
        try(RandomAccessFile memoryMappedFile = new RandomAccessFile(filePath, "r");
            FileChannel fileChannel = memoryMappedFile.getChannel()) {

            long bufferSize = fileChannel.size() / chunkCount;
            MappedByteBuffer buffer;
            // This 'lineBuilder' should create here because inner for loop might have pending characters from previous chunk
            StringBuilder lineBuilder = new StringBuilder();

            for (int i = 0; i <= chunkCount; i++) {
                logger.info("Starting chunk number [{}] ", i);
                long position = i * bufferSize;
                if(i == chunkCount) {
                    if(logger.isDebugEnabled()) {
                        logger.debug("Map count equal to i : [{}]", chunkCount);
                    }
                    bufferSize = fileChannel.size() - (position);
                }
                buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, position, bufferSize);
                for (int j = 0; j < buffer.limit(); j++) {
                    char c = (char) buffer.get();
                    if(c == '\n') {
                        String line = lineBuilder.toString();
                        line = line.replace("\r", "");
                        Object[] data = generateEvent(line);
                        eventStore.pushInputEvent(new Event(System.currentTimeMillis(), data));
                        pointer++;
                        lineBuilder = new StringBuilder();
                    } else {
                        lineBuilder = lineBuilder.append(c);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error occurred while picking events from the file [{}]", filePath);
        }
    }

    private Object[] generateEvent(String line) {
        Iterator<String> iterator = SPLITTER.split(line).iterator();
        List<String> dataStr = IteratorUtils.toList(iterator);

        double v_kmh = Double.valueOf(dataStr.get(5)) * 60 * 60 / 1000000000;
        double a_ms = Double.valueOf(dataStr.get(6)) / 1000000;

        long time = Long.valueOf(dataStr.get(1));

        return new Object[]{
                dataStr.get(0).intern(),
                time,
                Integer.valueOf(dataStr.get(2)),
                Integer.valueOf(dataStr.get(3)),
                Integer.valueOf(dataStr.get(4)),
                v_kmh,
                a_ms,
                Integer.valueOf(dataStr.get(7)),
                Integer.valueOf(dataStr.get(8)),
                Integer.valueOf(dataStr.get(9)),
                Integer.valueOf(dataStr.get(10)),
                Integer.valueOf(dataStr.get(11)),
                Integer.valueOf(dataStr.get(12)),
                System.nanoTime(),
                ((Double) (time * Math.pow(10, -9))).longValue()};
    }

    /*long end = System.currentTimeMillis();
    long millis = end - start;
    String value = String.format("%d min, %d sec, %d msec",
            TimeUnit.MILLISECONDS.toMinutes(millis),
            TimeUnit.MILLISECONDS.toSeconds(millis) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis)),
            millis - TimeUnit.SECONDS.toMillis(TimeUnit.MILLISECONDS.toSeconds(millis)));
    final long tsMs = (DATA_END_TIME_PS - DATA_START_TIME_PS) / 1000000000;
    double expectedThroughput = count * 1000.0f / tsMs;
    double speedup = (double)tsMs / (double)millis;

            logger.info("EventConsume [" + UseCaseRunner.testConfigurations +
            "|TimeMs=" + millis + "|ThroughputEPS=" + DECIMAL_FORMAT.format(1000.0f * count / millis) +
            "|RealtimeMs=" + tsMs + "|RealThroughputEPS=" + DECIMAL_FORMAT.format(expectedThroughput) +
            "|Speedup=" + DECIMAL_FORMAT.format(speedup) + "]");*/
}

package ru.smi.test.kafka;

import ch.qos.logback.classic.Level;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Collections;
import java.util.Iterator;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.AbstractCoordinator;
import org.apache.kafka.clients.consumer.internals.ConsumerCoordinator;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static Logger log = LoggerFactory.getLogger(Main.class);

    private static final DecimalFormat df = (DecimalFormat) DecimalFormat.getInstance(Locale.US);

    static {
        df.setGroupingUsed(false);
        df.setDecimalSeparatorAlwaysShown(false);
    }

    private static final Option BOOTSTRAP_SERVERS = Option.builder().longOpt("bootstrap-servers").hasArg(true).required(true).numberOfArgs(1).build();
    private static final Option GROUP = Option.builder().longOpt("group").hasArg(true).required(true).numberOfArgs(1).build();
    private static final Option TOPIC = Option.builder().longOpt("topic").hasArg(true).required(true).numberOfArgs(1).build();
    private static final Option MESSAGES = Option.builder().longOpt("messages").hasArg(true).required(false).numberOfArgs(1).type(Number.class).build();
    private static final Option POLL_TIMEOUT = Option.builder().longOpt("poll-timeout").hasArg(true).required(false).numberOfArgs(1).type(Number.class).build();
    private static final Option STATS_INTERVAL = Option.builder().longOpt("stats-interval").hasArg(true).required(false).numberOfArgs(1).type(Number.class)
            .build();
    private static final Option CONFIG = Option.builder().longOpt("config").hasArg(true).required(false).numberOfArgs(1).build();

    private static final Options OPTIONS = new Options() {

        private static final long serialVersionUID = 1L;

        {
            addOption(BOOTSTRAP_SERVERS);
            addOption(GROUP);
            addOption(TOPIC);
            addOption(MESSAGES);
            addOption(POLL_TIMEOUT);
            addOption(STATS_INTERVAL);
            addOption(CONFIG);
        }
    };

    private static final ExecutorService executor = Executors.newSingleThreadExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, Thread.currentThread().getName() + "-consumer");
        }
    });

    private static final CountDownLatch shutdownSignal = new CountDownLatch(1);

    static {
        setLoggingLevel(Level.INFO, Metrics.class);
        setLoggingLevel(Level.INFO, ConsumerCoordinator.class);
        setLoggingLevel(Level.INFO, AbstractCoordinator.class);
        setLoggingLevel(Level.INFO, NetworkClient.class);
        setLoggingLevel(Level.INFO, Metadata.class);
    }

    private static final AtomicLong totalMessages = new AtomicLong(0);
    private static final AtomicReference<Double> minSpeed = new AtomicReference<>(Double.MAX_VALUE);
    private static final AtomicReference<Double> maxSpeed = new AtomicReference<>(0.0);
    private static final AtomicInteger iterationCount = new AtomicInteger(0);
    private static final AtomicReference<Double> totalSpeed = new AtomicReference<>(0.0);

    public static void main(String... args) {
        log.info("Parsing arguments...");
        CommandLine commandLine = parseArgs(args);
        if (commandLine == null) {
            return;
        }
        log.info("...done");

        log.info("Creating consumer...");
        Properties properties = new Properties();
        if (commandLine.hasOption(CONFIG.getLongOpt())) {
            try {
                properties.load(new FileReader((commandLine.getOptionValue(CONFIG.getLongOpt()))));
            } catch (IOException exc) {
                log.error("Could not load properties", exc);
                return;
            }
        }
        properties.put("bootstrap.servers", commandLine.getOptionValue(BOOTSTRAP_SERVERS.getLongOpt()));
        properties.put("group.id", commandLine.getOptionValue(GROUP.getLongOpt()));
        KafkaConsumer consumer = createConsumer(properties);

        final String topicName = commandLine.getOptionValue(TOPIC.getLongOpt());
        final int pollTimeout = getValue(commandLine, POLL_TIMEOUT, 500);
        final int statsInterval = getValue(commandLine, STATS_INTERVAL, 100000);
        final int maxMessages = getValue(commandLine, MESSAGES, 10000000);

        log.info("...done");

        executor.submit(new ConsumerTask(consumer, topicName, pollTimeout, statsInterval, maxMessages));
        try {
            shutdownSignal.await();
        } catch (InterruptedException e) {
            //nothing to do
        }
        executor.shutdown();
        log.info("Total messages: {}", totalMessages.get());
        log.info("nMsgs/sec: min={}, max={}, avg={}", df.format(minSpeed.get()), df.format(maxSpeed.get()), df.format(totalSpeed.get() / iterationCount
                .decrementAndGet()));
        log.info("...exit");
    }

    private static KafkaConsumer createConsumer(Properties properties) {
        properties.put("security.protocol", SecurityProtocol.PLAINTEXT.name);
        properties.put("enable.auto.commit", "false");
        properties.put("auto.offset.reset", OffsetResetStrategy.EARLIEST.name().toLowerCase());
        KafkaConsumer consumer = new KafkaConsumer<>(properties, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        return consumer;
    }

    private static CommandLine parseArgs(String... args) {
        CommandLineParser parser = new DefaultParser();
        try {
            return parser.parse(OPTIONS, args);
        } catch (MissingOptionException exc) {
            log.error("{}", exc.getMessage());
            return null;
        } catch (ParseException exc) {
            log.error("Command parser error.", exc);
            return null;
        }
    }

    private static int getValue(final CommandLine commandLine, final Option option, int defaultValue) {
        if (commandLine.hasOption(option.getLongOpt())) {
            try {
                return ((Number) commandLine.getParsedOptionValue(option.getLongOpt())).intValue();
            } catch (ParseException exc) {
                log.error("Coult not parse option: {}", option.getLongOpt(), exc);
                System.exit(1);
            }
        }
        return defaultValue;
    }

    public static void setLoggingLevel(Level level, Class clazz) {
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(clazz);
        root.setLevel(level);
    }

    private static class ConsumerTask implements Runnable {

        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final KafkaConsumer consumer;
        private final String topicName;
        private final int pollTimeout;
        private final int statsInterval;
        private final int maxMessages;

        private ConsumerTask(KafkaConsumer consumer, String topicName, int pollTimeout, int statsInterval, int maxMessages) {
            this.consumer = consumer;
            this.topicName = topicName;
            this.pollTimeout = pollTimeout;
            this.statsInterval = statsInterval;
            this.maxMessages = maxMessages;
        }

        @Override
        public void run() {
            long messageCount = 0;
            int retryCount = 0;

            try {
                log.info("Starting consumer...");
                consumer.subscribe(Collections.singletonList(topicName));
                log.info("...started...");
                long start = System.currentTimeMillis();
                while (!closed.get()) {
                    ConsumerRecords records = consumer.poll(pollTimeout);
                    if (records.isEmpty()) {
                        if (++retryCount > 10) {
                            log.info("No messages");
                            break;
                        }
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException e) {
                            //nothing to do
                        }
                        continue;
                    }
                    retryCount = 0;
                    Iterator<ConsumerRecord> iterator = records.iterator();
                    while(iterator.hasNext()) {
                        iterator.next();
                        messageCount += 1;
                        if (messageCount % statsInterval == 0) {
                            print(messageCount, System.currentTimeMillis() - start);
                            if (totalMessages.get() >= maxMessages) {
                                closed.set(true);
                                break;
                            }
                            start = System.currentTimeMillis();
                            messageCount = 0;
                        }
                    }
                }
                log.info("...finished...");
            } catch (WakeupException exc) {
                if (!closed.get()) {
                    throw exc;
                }
            } finally {
                consumer.close();
                log.info("...consumer closed");
                shutdownSignal.countDown();
            }
        }

        public void shutdown() {
            closed.set(true);
            consumer.wakeup();
        }

        private void print(long messageCount, long elapsedTime) {
            double elapsedSecs = elapsedTime / 1000.0;
            double speed = messageCount/elapsedSecs;
            totalMessages.getAndAdd(messageCount);
            log.info("nMsgs: {}, ms: {}, nMsgs/sec: {}", totalMessages.get(), elapsedTime, df.format(speed));
            iterationCount.getAndIncrement();
            if (iterationCount.get() > 1) {
                totalSpeed.set(totalSpeed.get() + speed);
                if (minSpeed.get() > speed) {
                    minSpeed.set(speed);
                }
                if (maxSpeed.get() < speed) {
                    maxSpeed.set(speed);
                }
            }
        }

    }

}

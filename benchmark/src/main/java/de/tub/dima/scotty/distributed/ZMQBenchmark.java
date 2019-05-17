package de.tub.dima.scotty.distributed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.infra.ThreadParams;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.zeromq.SocketType;
import org.zeromq.Utils;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

@State(Scope.Benchmark)
@Measurement(iterations = 5, time = 5)
public class ZMQBenchmark {
    List<ZMQ.Socket> pushers;
    List<ZMQ.Socket> pullers;
    ZContext zContext;

    private final int NUM_PARALLEL_SOCKETS = 2;

    @Setup(Level.Trial)
    public void setupTrial(Blackhole bh) throws IOException {
        zContext = new ZContext();

        pullers = new ArrayList<>(NUM_PARALLEL_SOCKETS);
        pushers = new ArrayList<>(NUM_PARALLEL_SOCKETS);

        setupNetworkPushRun(bh);
        setupNetworkPullRun();
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
        pushers.forEach(Socket::close);
        pullers.forEach(Socket::close);
        zContext.destroy();
    }

    private void setupNetworkPushRun(Blackhole bh) throws IOException {
        Consumer<Socket> pull = (receiver) -> {
            while (true) {
                receiver.recvStr();
                receiver.recvStr(ZMQ.DONTWAIT);
                Object value = DistributedUtils.bytesToObject(receiver.recv(ZMQ.DONTWAIT));
                bh.consume(value);
            }
        };

        for (int i = 0; i < NUM_PARALLEL_SOCKETS; i++) {
            final int port = Utils.findOpenPort();
            final String url = "tcp://localhost:" + port;

            ZMQ.Socket receiver = zContext.createSocket(SocketType.PULL);
            receiver.bind(url);
            Thread pullThread = new Thread(() -> pull.accept(receiver));
            pullThread.start();

            ZMQ.Socket sender = zContext.createSocket(SocketType.PUSH);
            sender.connect(url);
            pushers.add(sender);
        }

    }

    private void setupNetworkPullRun() throws IOException {
        Consumer<ZMQ.Socket> push = (eventSender) -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            while (true) {
                Integer value = 10;
                eventSender.sendMore(String.valueOf(0));
                eventSender.sendMore(String.valueOf(1000));
                eventSender.send(DistributedUtils.objectToBytes(value), ZMQ.DONTWAIT);
            }
        };

        List<Thread> senders = new ArrayList<>(NUM_PARALLEL_SOCKETS);
        for (int i = 0; i < NUM_PARALLEL_SOCKETS; i++) {
            int port = Utils.findOpenPort();
            String url = "tcp://localhost:" + port;

            ZMQ.Socket sender = zContext.createSocket(SocketType.PUSH);
            sender.connect(url);
            Thread sendThread = new Thread(() -> push.accept(sender));
            senders.add(sendThread);

            ZMQ.Socket receiver = zContext.createSocket(SocketType.PULL);
            receiver.bind(url);
            pullers.add(receiver);
        }

        senders.forEach(Thread::start);
    }

    @Threads(NUM_PARALLEL_SOCKETS)
    @Benchmark()
    public void benchmarkZMQPush(ThreadParams params) {
        ZMQ.Socket sender = pushers.get(params.getThreadIndex());

        Integer value = 10;
        sender.sendMore(String.valueOf(0));
        sender.sendMore(String.valueOf(1000));
        sender.send(DistributedUtils.objectToBytes(value), ZMQ.DONTWAIT);
    }

    @Threads(NUM_PARALLEL_SOCKETS)
    @Benchmark()
    public void benchmarkZMQPull(Blackhole bh, ThreadParams params) {
        ZMQ.Socket receiver = pullers.get(params.getThreadIndex());

        String id = receiver.recvStr();
        String window = receiver.recvStr(ZMQ.DONTWAIT);
        Object value = DistributedUtils.bytesToObject(receiver.recv(ZMQ.DONTWAIT));

        bh.consume(id);
        bh.consume(window);
        bh.consume(value);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ZMQBenchmark.class.getName())
                .forks(1)
                .warmupIterations(10)
                .build();

        new Runner(opt).run();
    }


}

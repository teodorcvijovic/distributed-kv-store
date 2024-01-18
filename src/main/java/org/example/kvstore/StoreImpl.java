package org.example.kvstore;

import org.example.kvstore.cmd.*;
import org.example.kvstore.distribution.ConsistentHash;
import org.example.kvstore.distribution.RoundRobin;
import org.example.kvstore.distribution.Strategy;
import org.jgroups.*;

import java.util.Map;
import java.util.concurrent.*;

public class StoreImpl<K,V> extends ReceiverAdapter implements Store<K,V> {

    public enum StrategyType {
        ConstHashing,
        RoundRobin
    }

    private String name;
    private Strategy strategy;
    private Map<K,V> data;
    private CommandFactory<K,V> factory;

    private JChannel channel;

    private ExecutorService workers;

    // result is the value stored under key k
    private CompletableFuture<V> pending;

    StrategyType strategyType;

    class CmdHandler implements Callable<Void> {

        private Address callerAdr;
        private Command cmd;

        public CmdHandler(Address callerAdr, Command cmd) {
            this.callerAdr = callerAdr;
            this.cmd = cmd;
        }

        @Override
        public Void call() throws Exception {
            // do the computation required by the command

            K k = (K) cmd.getKey();
            V v;

            if (cmd.getClass().equals(Get.class)) {
                v = data.get(k);
            }
            else if (cmd.getClass().equals(Put.class)) {
                v = (V) cmd.getValue();
                v = data.put(k, v);
            }
            else if (cmd.getClass().equals(Reply.class)) {
                v = (V) cmd.getValue();
                synchronized (pending) {
                    pending.complete(v);
                }
                // we don't send the reply
                return null;
            }
            else {
                throw new Exception("command not supported");
            }

            // creates a Reply from the command and sends it back to the caller

            Reply reply = factory.newReplyCmd(k, v);
            send(callerAdr, reply);

            return null;
        }
    }

    public StoreImpl(String name, StrategyType strategyType) throws Exception {
        this.name = name;
        this.strategyType = strategyType;
    }

    public void viewAccepted(View newView) {
        if (strategyType.equals(StrategyType.ConstHashing))
            strategy = new ConsistentHash(newView);
        else if (strategyType.equals(StrategyType.RoundRobin))
            strategy = new RoundRobin(newView);

        // migrate data upon a view change
        migrateData();
    }

    @Override
    public void open() throws Exception{
        this.data = new ConcurrentHashMap<>();
        this.factory = new CommandFactory<>();
        this.pending = null;
        this.workers = Executors.newCachedThreadPool();
        this.channel = new JChannel();
        channel.setReceiver(this);
        channel.connect(this.name);
    }

    @Override
    public V get(K k) {
        V v;
        Address memberAddr = strategy.lookup(k);
        if (memberAddr.equals(this.channel.getAddress())) {
            v = data.get(k);
        }
        else {
            Get getCmd = factory.newGetCmd(k);
//            pending = new CompletableFuture<>();
//            send(memberAddr, getCmd);
//            try {
//                v = (V) this.pending.get();
//            } catch (Exception e) {
//                v = null;
//                e.printStackTrace();
//            }

            v = execute(memberAddr, getCmd);
        }
        return v;
    }

    @Override
    public V put(K k, V v) {
        V prevV;
        Address memberAddr = strategy.lookup(k);
        if (memberAddr.equals(this.channel.getAddress())) {
            prevV = data.put(k, v);
        }
        else {
            Put putCmd = factory.newPutCmd(k, v);
//            pending = new CompletableFuture<>();
//            send(memberAddr, putCmd);
//            try {
//                prevV = (V) this.pending.get();
//            } catch (Exception e) {
//                prevV = null;
//                e.printStackTrace();
//            }

            prevV = execute(memberAddr, putCmd);
        }
        return prevV;
    }

    private synchronized V execute(Address dst, Command cmd) {
        try {
            pending = new CompletableFuture<>();
            send(dst, cmd);
            V v = (V) pending.get();
            return v;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public String toString(){
        return "Store#"+name+"{"+data.toString()+"}";
    }

    @Override
    public void close(){
        this.channel.close();
    }

    public void send(Address dst, Command command) {
        // pushes a command to dst by bundling it inside a JGroups message
        try {
            Message msg = new Message(dst, null, command);
            channel.send(msg);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void receive(Message msg) {
        // we need to check if we should perform data migration
        if (msg.getObject().getClass().equals(ConcurrentHashMap.class)) {
            Map<K, V> receivedData = (Map<K, V>) msg.getObject();
            data.putAll(receivedData);
            int dataSize = receivedData.size();
            System.out.println("received migrated data of size " + dataSize);
            return;
        }

        // this method retrieves the command from the payload of the message,
        // then submits a new CmdHandler for this command to the workers

        Command cmd = (Command) msg.getObject();
        Address callerAddr = msg.getSrc();
        CmdHandler handler = new CmdHandler(callerAddr, cmd);
        workers.submit(handler);
    }

    private void migrateData() {
        try {
            // iterate through our current data and check if migration is needed

            Address myAddress = channel.getAddress();
            Map<K, V> mapToSend = new ConcurrentHashMap<>();
            Address targetAddr = null;

            for (Map.Entry<K, V> KVpair : data.entrySet()) {
                K k = KVpair.getKey();
                V v = KVpair.getValue();

                Address addr = strategy.lookup(k);
                if (!myAddress.equals(addr)) {
                    // not responsible for the key
                    data.remove(k);
                    mapToSend.put(k, v);

                    if (targetAddr == null) targetAddr = addr;
                }
            }

            if (mapToSend.size() > 0) {
                Message msg = new Message(targetAddr, null, mapToSend);
                this.channel.send(msg);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

}

package org.example.kvstore;

import org.junit.Test;
import org.junit.experimental.theories.suppliers.TestedOn;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class StoreTest {

    @Test
    public void baseOperations() {
        StoreManager manager = new StoreManager();
        Store<Integer, Integer> store = manager.newStore();

        assert store.get(1) == null;

        store.put(42, 1);
        assert store.get(42).equals(1);

        assert store.put(42, 2).equals(1);

        manager.stop();
	
    }

    @Test
    public void multipleStores(){
        int NCALLS = 1000;
        Random rand = new Random(System.nanoTime());

        StoreManager manager = new StoreManager();
        Store<Integer, Integer> store1 = manager.newStore("store", StoreImpl.StrategyType.ConstHashing);
        Store<Integer, Integer> store2 = manager.newStore("store", StoreImpl.StrategyType.ConstHashing);
        Store<Integer, Integer> store3 = manager.newStore("store", StoreImpl.StrategyType.ConstHashing);

        for (int i=0; i<NCALLS; i++) {
            int k = rand.nextInt();
            int v = rand.nextInt();
            store1.put(k, v);
            assert rand.nextBoolean() ? store2.get(k).equals(v) : store3.get(k).equals(v);
        }

	manager.stop();
    }

    @Test
    public void dataMigration(){
        int NCALLS = 1000;
        Random rand = new Random(System.nanoTime());

        StoreManager manager = new StoreManager();
        Store<Integer, Integer> store1 = manager.newStore("store", StoreImpl.StrategyType.ConstHashing);
        Store<Integer, Integer> store2 = manager.newStore("store", StoreImpl.StrategyType.ConstHashing);
        Store<Integer, Integer> store3 = manager.newStore("store", StoreImpl.StrategyType.ConstHashing);

        Map<Integer, Integer> map = new HashMap<>();
        for (int i=0; i<NCALLS; i++) {
            int k = rand.nextInt();
            int v = rand.nextInt();
            store1.put(k, v);
            map.put(k, v);
            assert rand.nextBoolean() ? store2.get(k).equals(v) : store3.get(k).equals(v);
        }

        // we add another KV store node
        Store<Integer, Integer> store4 = manager.newStore("store", StoreImpl.StrategyType.ConstHashing);

        // delay for scheme reconfiguration
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (Map.Entry<Integer, Integer> KVpair : map.entrySet()) {
        	int k = KVpair.getKey();
        	int v = KVpair.getValue();
        	assert rand.nextBoolean() ? store2.get(k).equals(v) : store3.get(k).equals(v);
        }
    }

    @Test
    public void strategyComparison(){
        int NCALLS = 1000;
        Random rand = new Random(System.nanoTime());
        StoreManager manager = new StoreManager();

        /******** CH ************/
        System.out.println("Consistent Hashing:");

        Store<Integer, Integer> store1node1 = manager.newStore("store1", StoreImpl.StrategyType.ConstHashing);
        Store<Integer, Integer> store1node2 = manager.newStore("store1", StoreImpl.StrategyType.ConstHashing);
        Store<Integer, Integer> store1node3 = manager.newStore("store1", StoreImpl.StrategyType.ConstHashing);

        for (int i=0; i<NCALLS; i++) {
            int k = rand.nextInt();
            int v = rand.nextInt();
            store1node1.put(k, v);
            assert rand.nextBoolean() ? store1node2.get(k).equals(v) : store1node3.get(k).equals(v);
        }

        Store<Integer, Integer> store1node4 = manager.newStore("store1", StoreImpl.StrategyType.ConstHashing);

        // delay for scheme reconfiguration
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        /********** RR **********/
        System.out.println("Round Robin:");

        Store<Integer, Integer> store2node1 = manager.newStore("store2", StoreImpl.StrategyType.RoundRobin);
        Store<Integer, Integer> store2node2 = manager.newStore("store2", StoreImpl.StrategyType.RoundRobin);
        Store<Integer, Integer> store2node3 = manager.newStore("store2", StoreImpl.StrategyType.RoundRobin);

        for (int i=0; i<NCALLS; i++) {
            int k = rand.nextInt();
            int v = rand.nextInt();
            store2node1.put(k, v);
            assert rand.nextBoolean() ? store2node2.get(k).equals(v) : store2node3.get(k).equals(v);
        }

        Store<Integer, Integer> store2node4 = manager.newStore("store2", StoreImpl.StrategyType.RoundRobin);

        // delay for scheme reconfiguration
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


}

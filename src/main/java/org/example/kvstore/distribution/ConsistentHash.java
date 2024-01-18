package org.example.kvstore.distribution;

import org.jgroups.Address;
import org.jgroups.View;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class ConsistentHash implements Strategy {

    // idea of consistent hashing is that keys and KV store node addresses
    // map to the same ID space

    private TreeSet<Integer> ring;
    private Map<Integer, Address> addresses;

    public ConsistentHash(View view){
        this.ring = new TreeSet<>();
        this.addresses = new HashMap<>();

        List<Address> members = view.getMembers();
        for (Address memberAddr : members) {
            Integer hashcode = memberAddr.hashCode();

            // add hashed member address to the ring
            ring.add(hashcode);

            // assign key space to each member
            addresses.put(hashcode, memberAddr);
        }
    }

    @Override
    public Address lookup(Object k){
        Integer key = (Integer) k;

        // return min value from ring bigger than key
        Integer memberAdrHashcode = ring.ceiling(key);

        // if key > maxHashcodeFromRing use the first hashcode
        if (memberAdrHashcode == null) memberAdrHashcode = ring.first();

        Address memberAdr = addresses.get(memberAdrHashcode);
        return memberAdr;
    }

}

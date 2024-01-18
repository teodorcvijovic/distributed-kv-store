package org.example.kvstore.distribution;

import org.jgroups.Address;
import org.jgroups.View;

import java.util.List;

public class RoundRobin implements Strategy {

    private List<Address> addresses;

    public RoundRobin(View view) {
        this.addresses = view.getMembers();
    }

    @Override
    public Address lookup(Object k) {
        Integer key = (Integer) k;
        int index = Math.abs((Integer) key  % addresses.size());
        Address addr = addresses.get(index);
        return addr;
    }
}

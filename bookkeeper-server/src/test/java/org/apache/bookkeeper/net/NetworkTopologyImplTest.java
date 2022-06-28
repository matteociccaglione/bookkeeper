package org.apache.bookkeeper.net;

import org.apache.commons.lang3.builder.ToStringExclude;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class NetworkTopologyImplTest {
    private Node node1;
    private Node node2;
    private NetworkTopologyImpl impl;
    public NetworkTopologyImplTest(Node node1, Node node2){
        this.node1 = node1;
        this.node2 = node2;
        this.impl=new NetworkTopologyImpl();
    }
    @Parameterized.Parameters
    public static Collection configure(){
        return Arrays.asList(new Object[][] {
                {new NodeBase("127.0.0.1:3456","/root"),new NodeBase("127.0.0.1:3457","/root")},
                {new NodeBase("127.0.0.1:3456","/root/rack1"),new NodeBase("127.0.0.1:3457","/root/rack2")},
                {null,new NodeBase("127.0.0.1:3457","/root/rack2")},
                {null,null}
        });

    }

    @Test
    public void testIsOnSameRack(){
        this.impl.add(this.node1);
        this.impl.add(this.node2);
        boolean result = this.impl.isOnSameRack(this.node1,this.node2);
        boolean expected = this.node1.getNetworkLocation().equals(this.node2.getNetworkLocation());
        Assert.assertEquals(expected,result);
    }
}

package org.apache.bookkeeper.net;

import org.apache.commons.lang3.builder.ToStringExclude;
import org.junit.Assert;
import org.junit.Ignore;
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
                {null,null},
                //{new NodeBase("127.0.0.1:6789","/root/rack1/rack2/rack3"),new NodeBase("127.0.0.1:6788","/root/rack1/rack4/rack5")},
                //{new NodeBase("127.0.0.1:6789","/root/rack1/rack2/rack3"),new NodeBase("127.0.0.1:6789","/root/rack1/rack2/rack3")}

        });

    }

    @Test
    public void testIsOnSameRack(){
        this.impl.add(this.node1);
        this.impl.add(this.node2);
        boolean result = this.impl.isOnSameRack(this.node1,this.node2);
        if(this.node1 == null || this.node2 == null){
            Assert.assertTrue(true);
            return;
        }
        boolean expected = this.node1.getNetworkLocation().equals(this.node2.getNetworkLocation());
        Assert.assertEquals(expected,result);
    }

    @Ignore
    @Test
    public void testGetDistance() {
        this.impl.add(this.node1);
        this.impl.add(this.node2);
        try {
            int result = this.impl.getDistance(this.node1, this.node2);
            //Now compute my value following documentation spec
            int expected = 0;
            if (!this.node1.equals(this.node2)) {
                int node1Distance = 1;
                int node2Distance = 1;
                Node n = this.node1.getParent();
                Node n1 = this.node2.getParent();
                while (n != null && n1 != null) {
                    if (n.equals(n1)) {
                        break;
                    }
                    n = n.getParent();
                    n1 = n1.getParent();
                }
                Node parent = n;
                if (parent == null) {
                    //No common ancestor
                    expected = Integer.MAX_VALUE;
                } else {
                    n = this.node1.getParent();
                    while (!n.equals(parent)) {
                        n = n.getParent();
                        node1Distance++;
                    }
                    n1 = this.node2.getParent();
                    while (!n1.equals(parent)) {
                        n1 = n1.getParent();
                        node2Distance++;
                    }
                    expected = node1Distance + node2Distance;
                }
            }
            Assert.assertEquals(expected, result);
        }catch(NullPointerException e){
            if(this.node1==null || this.node2 == null){
                Assert.assertTrue(true);
            }
            else{
                Assert.assertTrue(false);
            }
        }
    }
}

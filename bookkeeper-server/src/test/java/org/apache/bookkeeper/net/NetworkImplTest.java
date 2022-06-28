package org.apache.bookkeeper.net;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class NetworkImplTest {
    private NodeBase node;
    private Type type;
    private NetworkTopologyImpl networkTopology;
    enum Type{
        ADD,
        ADD_EX,
        ADD_EX_2,
        REMOVE,
        REMOVE_EX
    }

    public NetworkImplTest(NodeBase node,Type type){
        this.node = node;
        this.type = type;
        this.networkTopology = new NetworkTopologyImpl();
    }

    @Parameterized.Parameters
    public static Collection configure(){
        return Arrays.asList(new Object[][] {
                {buildNode("127.0.0.1:5555","/root"), Type.ADD} ,
                {buildNode("no_leaf","/"), Type.ADD_EX},
                {null,Type.ADD},
                {new NetworkTopologyImpl.InnerNode("/root/inner_node"),Type.ADD_EX},
                {buildNode("127.0.0.1:67893","/root/second"), Type.ADD_EX_2},
                {buildNode("127.0.0.1:67893","/root"),Type.REMOVE},
                {new NetworkTopologyImpl.InnerNode("/root/inner_node"), Type.REMOVE_EX}
        });
    }

    public static NodeBase buildNode(String name, String path){
        return new NodeBase(name,path);
    }

    @Test
    public void testAdd(){
        Assume.assumeTrue(type==Type.ADD);
        boolean isCorrect = !this.networkTopology.contains(this.node);
        this.networkTopology.add(this.node);
        if(this.node==null){
            isCorrect = true;
        }
        else {
            isCorrect = isCorrect && this.networkTopology.contains(this.node);
        }
        Assert.assertTrue(isCorrect);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAddEx(){
        Assume.assumeTrue(type==Type.ADD_EX);
        this.networkTopology.add(this.node);
    }

    @Test(expected = NetworkTopologyImpl.InvalidTopologyException.class)
    public void testAddExInvalid(){
        Assume.assumeTrue(type==Type.ADD_EX_2);
        String location = this.node.getNetworkLocation();
        String rack = location + "/rack/127.0.0.1:45678";
        this.networkTopology.add(new NodeBase(rack));
        this.networkTopology.add(this.node);
    }

    @Test
    public void testRemove(){
        Assume.assumeTrue(type==Type.REMOVE);
        if(!this.networkTopology.contains(this.node))
            this.networkTopology.add(this.node);
        this.networkTopology.remove(this.node);
        Assert.assertFalse(this.networkTopology.contains(this.node));
    }
    @Test(expected = IllegalArgumentException.class)
    public void testRemoveIllegal(){
        Assume.assumeTrue(type==Type.REMOVE_EX);
        this.networkTopology.remove(this.node);
    }


}

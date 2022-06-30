package org.apache.bookkeeper.net;

import org.apache.bookkeeper.util.HardLink;
import org.checkerframework.common.initializedfields.qual.EnsuresInitializedFields;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
@Ignore
@RunWith(Parameterized.class)
public class NetworkTopologyGetTest {
    private String location;
    private Type type;
    private NetworkTopologyImpl impl;
    private boolean empty;
    enum Type{
        TAKE_RACK,
        DIVIDE,
        GET_NODE
    }
    @Before
    public void startUpImpl(){
        this.impl = new NetworkTopologyImpl();
    }
    public NetworkTopologyGetTest(String location,boolean empty,  Type type){
        this.location=location;
        this.type = type;
        this.empty=empty;
    }

    @Parameterized.Parameters
    public static Collection configure(){
        return Arrays.asList(new Object[][] {
                {"/root",false,Type.TAKE_RACK},
                {"/root",true,Type.TAKE_RACK},
                {"/root/subroot/subsub",false,Type.DIVIDE},
                {"/root/127.0.0.1:33215",false,Type.GET_NODE},
                {"/root/127.0.0.1:33215",true,Type.GET_NODE}
        });
    }

    @Ignore
    @Test
    public void testGetDatanodesInRack(){
        Assume.assumeTrue(type==Type.TAKE_RACK);
        boolean isCorrect=false;
        if(!empty) {
            NodeBase node1 = new NodeBase(this.location + "/127.0.0.1:54673");
            NodeBase node2 = new NodeBase(this.location + "/127.0.0.1:54672");
            List<Node> nodes = this.impl.getDatanodesInRack(this.location);
            isCorrect = nodes.size() == 2;
            isCorrect = isCorrect && nodes.contains(node1) && nodes.contains(node2);
        }
        else{
            List<Node> nodes = this.impl.getDatanodesInRack(this.location);
            isCorrect= nodes.isEmpty();
        }
        Assert.assertTrue(isCorrect);
    }


    @Test
    public void testGetFirstHalf(){
        Assume.assumeTrue(type==Type.DIVIDE);
        String[] parts = this.location.split("/");
        StringBuilder expected = new StringBuilder();
        expected.append(parts[0]);
        for (int i = 1; i < parts.length-1; i++){
            expected.append("/");
            expected.append(parts[i]);

        }
        String result = NetworkTopologyImpl.getFirstHalf(this.location);
        Assert.assertEquals(expected.toString(),result);
    }

    @Test
    public void testGetLastHalf(){
        Assume.assumeTrue(type==Type.DIVIDE);
        String[] parts = this.location.split("/");
        String expected = "/"+parts[parts.length-1];
        String result = NetworkTopologyImpl.getLastHalf(this.location);
        Assert.assertEquals(expected,result);
    }

    @Test
    public void testGetLeaves(){
        Assume.assumeTrue(type==Type.TAKE_RACK);
        boolean isCorrect = false;
        if(!empty){
            NodeBase node1 = new NodeBase(this.location+"/127.0.0.1:43261");
            NodeBase node2 = new NodeBase(this.location+"/127.0.0.1:43262");
            this.impl.add(node1);
            this.impl.add(node2);
            Set<Node> nodes = this.impl.getLeaves(this.location);
            System.out.println(nodes.size());
            isCorrect = nodes.size()==2;
            isCorrect = isCorrect&&nodes.contains(node1) && nodes.contains(node2);
        }
        else{
            isCorrect = this.impl.getLeaves(this.location).isEmpty();
        }
        Assert.assertTrue(isCorrect);
    }

    @Test
    public void testGetNode(){
        Assume.assumeTrue(type==Type.GET_NODE);
        boolean isCorrect = false;
        if(!empty){
            NodeBase node = new NodeBase(this.location);
            this.impl.add(node);
            Node result = this.impl.getNode(this.location);
            isCorrect = node.equals(result);
        }
        else{
            Node result = this.impl.getNode(this.location);
            isCorrect = result==null;
        }
        Assert.assertTrue(isCorrect);
    }

    @Test
    public void getNumOfLeaves(){
        Assume.assumeTrue(type==Type.TAKE_RACK);
        if(!empty){
            this.impl.add(new NodeBase(this.location+"/127.0.0.1:33255"));
            int result = this.impl.getNumOfLeaves();
            Assert.assertEquals(1,result);
        }
        else{
            Assert.assertEquals(0,this.impl.getNumOfLeaves());
        }
    }

    @Test
    public void getNumOfRacks(){
        Assume.assumeTrue(type==Type.TAKE_RACK);
        if(!empty){
            this.impl.add(new NodeBase(this.location+"/127.0.0.1:33255"));
            int result = this.impl.getNumOfRacks();
            Assert.assertEquals(1,result);
        }
        else{
            Assert.assertEquals(0,this.impl.getNumOfRacks());
        }
    }



}

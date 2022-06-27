package org.apache.bookkeeper.client;


import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.bookkeeper.versioning.Versioned;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;

@RunWith(Parameterized.class)
public class BookKeeperTest {
    private static LocalBookKeeper bookKeeper;
    private static BookKeeper client;
    private int ensSize;
    private int writeQuorumSize;
    private int ackQuorumSize;
    private BookKeeper.DigestType digestType;
    private byte[] passwd;
    private Map<String,byte[]> customMetadata;
    private Type type;

    enum Type{
        CREATE,
        CREATE_EX,
        DELETE_OK,
        DELETE_FAIL
    }

    private static void cleanDirectory(File dir){
        File[] files = dir.listFiles();
        if(files==null){
            return;
        }
        if(files.length!=0){
            for (File file: files){
                if(file.isFile()){
                    file.delete();
                }
                else if (file.isDirectory()){
                    cleanDirectory(file);
                }
            }
        }
        dir.delete();
    }

    @BeforeClass
    public static void startServer() throws Exception {
        ServerConfiguration configuration = new ServerConfiguration();
        configuration.setAllowLoopback(true);
        bookKeeper = LocalBookKeeper.getLocalBookies("127.0.0.1",34567,3,true,configuration);
        bookKeeper.start();

    }
    public BookKeeperTest(int ensSize, int writeQuorumSize, int ackQuorumSize,BookKeeper.DigestType digestType, byte[] passwd, Map<String,byte[]> customMetadata, Type type ) throws BKException, IOException, InterruptedException {
        this.ensSize=ensSize;
        this.writeQuorumSize=writeQuorumSize;
        this.ackQuorumSize=ackQuorumSize;
        this.digestType=digestType;
        this.passwd=passwd;
        this.customMetadata=customMetadata;
        this.type=type;
        client = new BookKeeper("127.0.0.1:34567");
    }


    @Parameterized.Parameters
    public static Collection configure(){
        Map<String, byte[]> nonEmptyMetadata = new HashMap<>();
        nonEmptyMetadata.put("myMetadata", "MyCustomMetadata".getBytes());
        byte[] data = {};
        return Arrays.asList(new Object[][] {

                {3, 2, 1, BookKeeper.DigestType.MAC, "1010".getBytes(), nonEmptyMetadata, Type.CREATE},
                {2, 2, 1, BookKeeper.DigestType.CRC32, "1010".getBytes(), null, Type.CREATE},
                //{1, 2, 1, BookKeeper.DigestType.MAC, new byte[]{}, null, Type.CREATE_EX}

                {2, 1, 1, BookKeeper.DigestType.MAC, "1010".getBytes(), null, Type.CREATE},
                {1, 1, 1, BookKeeper.DigestType.CRC32, new byte[]{}, new HashMap<String, byte[]>(), Type.CREATE},
                //{0, 1, 1, BookKeeper.DigestType.MAC, "1010".getBytes(), null, Type.CREATE_EX},
                {1, 0, 1, BookKeeper.DigestType.MAC, "1010".getBytes(), null, Type.CREATE_EX},
                {0, 0, 1, BookKeeper.DigestType.CRC32, "1010".getBytes(), null, Type.CREATE_EX},
                //{-1, 0, 1, BookKeeper.DigestType.MAC, "1010".getBytes(), null, Type.CREATE_EX},
                {2, 1, 0, BookKeeper.DigestType.MAC, new byte[]{}, new HashMap<String, byte[]>(), Type.CREATE},
                {1, 1, 0, BookKeeper.DigestType.MAC, "1010".getBytes(), null, Type.CREATE},
                //{0, 1, 0, BookKeeper.DigestType.MAC, "1010".getBytes(), null, Type.CREATE_EX},
                {1, 0, 0, BookKeeper.DigestType.MAC, "1010".getBytes(), null, Type.CREATE},
                {0, 0, 0, BookKeeper.DigestType.MAC, "1010".getBytes(), nonEmptyMetadata, Type.CREATE},
                //{-1, 0, 0, BookKeeper.DigestType.MAC, new byte[]{}, null, Type.CREATE_EX},
                {0, -1, 0, BookKeeper.DigestType.MAC, "1010".getBytes(), null, Type.CREATE_EX},
                //{-1, -1, 0, BookKeeper.DigestType.MAC, "1010".getBytes(), null, Type.CREATE_EX},
                //{-2, -1, 0, BookKeeper.DigestType.MAC, new byte[]{}, null, Type.CREATE_EX},
                {1, 0, -1, BookKeeper.DigestType.MAC, "1010".getBytes(), null, Type.CREATE},
                {0, 0, -1, BookKeeper.DigestType.MAC, "1010".getBytes(), nonEmptyMetadata, Type.CREATE},
                //{-1, 0, -1, BookKeeper.DigestType.MAC, "1010".getBytes(), null, Type.CREATE_EX},
                {0, -1, -1, BookKeeper.DigestType.MAC, "1010".getBytes(), null, Type.CREATE},
                //{-1, -1, -1, BookKeeper.DigestType.MAC, new byte[]{}, null, Type.CREATE_EX},
                //{-2, -1, -1, BookKeeper.DigestType.MAC, "1010".getBytes(), null, Type.CREATE_EX},
                //{-1, -2, -1, BookKeeper.DigestType.MAC, "1010".getBytes(), null, Type.CREATE_EX},
                //{-2, -2, -1, BookKeeper.DigestType.MAC, "1010".getBytes(), new HashMap<String, byte[]>(), Type.CREATE_EX},
                //{-3, -2, -1, BookKeeper.DigestType.MAC, "1010".getBytes(), null, Type.CREATE_EX}


                {1,0,0,BookKeeper.DigestType.MAC,"1010".getBytes(StandardCharsets.UTF_8),null,Type.DELETE_OK}
                //{1,0,0,BookKeeper.DigestType.MAC,"1010".getBytes(StandardCharsets.UTF_8),null,Type.DELETE_FAIL}

        });
    }


    @AfterClass
    public static void closeServer() throws Exception {
        client.close();
        //this.bookKeeper.shutdownBookies();
        bookKeeper.close();
    }

    @Test
    public void testCreateLedger() throws BKException, InterruptedException {
        Assume.assumeTrue(type==Type.CREATE);
        LedgerHandle ledger = client.createLedger(ensSize,writeQuorumSize,ackQuorumSize,digestType,passwd,customMetadata);
        Assert.assertTrue(isValidLedger(ledger.getLedgerMetadata()));
    }

    private boolean isValidLedger(LedgerMetadata metadata){
        boolean isCorrectPasswd = metadata.getPassword().length == passwd.length;
        if(!isCorrectPasswd){
            return false;
        }
        for(int i = 0; i < passwd.length; i++){
            if(metadata.getPassword()[i]!=passwd[i]){
                isCorrectPasswd=false;
                break;
            }
        }
        boolean isCorrectMetadata = this.customMetadata==null || metadata.getCustomMetadata().equals(customMetadata);
        return metadata.getEnsembleSize()==ensSize && metadata.getWriteQuorumSize()==writeQuorumSize && metadata.getAckQuorumSize()==ackQuorumSize && isCorrectPasswd && isCorrectMetadata;
    }

    @Test
    public void testCreateLedgerEx() throws BKException, InterruptedException {
        Assume.assumeTrue(type==Type.CREATE_EX);
        boolean isPassed=false;
        try {
            client.createLedger(ensSize, writeQuorumSize, ackQuorumSize, digestType, passwd, customMetadata);
        }catch(Exception e){
            isPassed=true;
        }
        Assert.assertTrue(isPassed);
    }

    @Test
    public void testDelete(){
        Assume.assumeTrue(type==Type.DELETE_OK || type==Type.DELETE_FAIL);
        long lId = 55555;
        boolean isPassed=false;
        LedgerHandle handle = null;
        if(type==Type.DELETE_OK){
            try {
                handle = client.createLedger(1, 0, 0, BookKeeper.DigestType.MAC, "1010".getBytes(StandardCharsets.UTF_8), null);

                lId = handle.getId();
            }catch(Exception e){
                Assert.fail();
            }
        }
        try{
            client.deleteLedger(lId);
            if(handle!=null){
                CompletableFuture<Versioned<LedgerMetadata>> future = this.client.getLedgerManager().readLedgerMetadata(lId);
                try{
                    SyncCallbackUtils.waitForResult(future);
                }catch(BKException.BKNoSuchLedgerExistsOnMetadataServerException e){
                    isPassed=true;
                }
            }
        }catch(BKException | InterruptedException e ){
            if(type==Type.DELETE_FAIL){
                isPassed=true;
            }
        }
        Assert.assertTrue(isPassed);
    }

}

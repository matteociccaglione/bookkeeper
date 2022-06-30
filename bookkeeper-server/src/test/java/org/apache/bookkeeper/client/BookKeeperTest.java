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
public class BookKeeperTest extends BookKeeperTestBaseClass{
    private int ensSize;
    private int writeQuorumSize;
    private int ackQuorumSize;
    private BookKeeper.DigestType digestType;
    private byte[] passwd;
    private Map<String,byte[]> customMetadata;






    public BookKeeperTest(int ensSize, int writeQuorumSize, int ackQuorumSize,BookKeeper.DigestType digestType, byte[] passwd, Map<String,byte[]> customMetadata) throws BKException, IOException, InterruptedException {
        this.ensSize=ensSize;
        this.writeQuorumSize=writeQuorumSize;
        this.ackQuorumSize=ackQuorumSize;
        this.digestType=digestType;
        this.passwd=passwd;
        this.customMetadata=customMetadata;

    }


    @Parameterized.Parameters
    public static Collection configure(){
        Map<String, byte[]> nonEmptyMetadata = new HashMap<>();
        nonEmptyMetadata.put("myMetadata", "MyCustomMetadata".getBytes());
        byte[] data = {};
        return Arrays.asList(new Object[][] {

                {3, 2, 1, BookKeeper.DigestType.MAC, "1010".getBytes(), nonEmptyMetadata},
                {2, 2, 1, BookKeeper.DigestType.CRC32, "1010".getBytes(), null},


                {2, 1, 1, BookKeeper.DigestType.MAC, "1010".getBytes(), null},
                {1, 1, 1, BookKeeper.DigestType.CRC32, new byte[]{}, new HashMap<String, byte[]>()},

                {2, 1, 0, BookKeeper.DigestType.MAC, new byte[]{}, new HashMap<String, byte[]>()},
                {1, 1, 0, BookKeeper.DigestType.MAC, "1010".getBytes(), null},

                {1, 0, 0, BookKeeper.DigestType.MAC, "1010".getBytes(), null},
                {0, 0, 0, BookKeeper.DigestType.MAC, "1010".getBytes(), nonEmptyMetadata},

                {1, 0, -1, BookKeeper.DigestType.MAC, "1010".getBytes(), null},
                {0, 0, -1, BookKeeper.DigestType.MAC, "1010".getBytes(), nonEmptyMetadata},

                {0, -1, -1, BookKeeper.DigestType.MAC, "1010".getBytes(), null},


        });
    }




    @Test
    public void testCreateLedger() throws BKException, InterruptedException {
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





}

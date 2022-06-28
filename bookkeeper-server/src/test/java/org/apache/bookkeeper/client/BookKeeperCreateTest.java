package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
@Ignore
@RunWith(Parameterized.class)
public class BookKeeperCreateTest extends BookKeeperTestBaseClass{
    private int ensSize;
    private int writeQuorumSize;
    private BookKeeper.DigestType digestType;
    private byte[] passwd;
    private int ackQuorumSize;
    private Type type;
    enum Type{
        CREATE_4,
        CREATE,
        CREATE_5,
        CREATE_EX
    }

    public BookKeeperCreateTest(int ensSize, int writeQuorumSize,  int ackQuorumSize,BookKeeper.DigestType digestType, byte[] passwd, Type type) {
        this.ensSize = ensSize;
        this.writeQuorumSize = writeQuorumSize;
        this.digestType = digestType;
        this.passwd = passwd;
        this.ackQuorumSize = ackQuorumSize;
        this.type = type;
    }


    @Parameterized.Parameters
    public static Collection configure(){
        Map<String, byte[]> nonEmptyMetadata = new HashMap<>();
        nonEmptyMetadata.put("myMetadata", "MyCustomMetadata".getBytes());
        byte[] data = {};
        return Arrays.asList(new Object[][] {

                {3, 2, 1, BookKeeper.DigestType.MAC, "1010".getBytes(), BookKeeperCreateTest.Type.CREATE},
                {2, 2, 1, BookKeeper.DigestType.CRC32, "1010".getBytes(), BookKeeperCreateTest.Type.CREATE},
                {2, 1, 1, BookKeeper.DigestType.MAC, "1010".getBytes(), BookKeeperCreateTest.Type.CREATE},
                {1, 1, 1, BookKeeper.DigestType.CRC32, new byte[]{}, BookKeeperCreateTest.Type.CREATE},
                {1, 0, 1, BookKeeper.DigestType.MAC, "1010".getBytes(),BookKeeperCreateTest.Type.CREATE_EX},
                {0, 0, 1, BookKeeper.DigestType.CRC32, "1010".getBytes(), BookKeeperCreateTest.Type.CREATE_EX},
                {2, 1, 0, BookKeeper.DigestType.MAC, new byte[]{},  BookKeeperCreateTest.Type.CREATE},
                {1, 1, 0, BookKeeper.DigestType.MAC, "1010".getBytes(), BookKeeperCreateTest.Type.CREATE},
                {1, 0, 0, BookKeeper.DigestType.MAC, "1010".getBytes(), BookKeeperCreateTest.Type.CREATE},
                {0, 0, 0, BookKeeper.DigestType.MAC, "1010".getBytes(),  BookKeeperCreateTest.Type.CREATE},
                {0, -1, 0, BookKeeper.DigestType.MAC, "1010".getBytes(), BookKeeperCreateTest.Type.CREATE_EX},
                {-1, -1, 0, BookKeeper.DigestType.MAC, "1010".getBytes(),  BookKeeperCreateTest.Type.CREATE_EX},
                {1, 0, -1, BookKeeper.DigestType.MAC, "1010".getBytes(), BookKeeperCreateTest.Type.CREATE},
                {0, 0, -1, BookKeeper.DigestType.MAC, "1010".getBytes(),  BookKeeperCreateTest.Type.CREATE},
                {0, -1, -1, BookKeeper.DigestType.MAC, "1010".getBytes(),BookKeeperCreateTest.Type.CREATE},
                {-1, -1, -1, BookKeeper.DigestType.MAC, new byte[]{},BookKeeperCreateTest.Type.CREATE_EX},
                {-1, -2, -1, BookKeeper.DigestType.MAC, "1010".getBytes(),  BookKeeperCreateTest.Type.CREATE_EX},
                {-2, -2, -1, BookKeeper.DigestType.MAC, "1010".getBytes(), BookKeeperCreateTest.Type.CREATE_EX}

        });
    }

    @Test
    public void testCreateLedger() throws BKException, InterruptedException {
        Assume.assumeTrue(type!=Type.CREATE_EX);
        LedgerHandle ledger = client.createLedger(ensSize,writeQuorumSize,digestType,passwd);
        Assert.assertTrue(isValidLedger(ledger.getLedgerMetadata(),Type.CREATE_4));
    }
    @Test
    public void testCreateLedger1() throws BKException, InterruptedException {
        Assume.assumeTrue(type!=Type.CREATE_EX);
        LedgerHandle ledger = client.createLedger(digestType,passwd);
        Assert.assertTrue(isValidLedger(ledger.getLedgerMetadata(),Type.CREATE));
    }
    @Test
    public void testCreateLedger2() throws BKException, InterruptedException {
        Assume.assumeTrue(type!=Type.CREATE_EX);
        LedgerHandle ledger = client.createLedger(ensSize,writeQuorumSize,ackQuorumSize,digestType,passwd);
        Assert.assertTrue(isValidLedger(ledger.getLedgerMetadata(),Type.CREATE_5));
    }
    private boolean isValidLedger(LedgerMetadata metadata,Type type){
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
        boolean isCorrect =  isCorrectPasswd;
        if(type==Type.CREATE_4){
            isCorrect = isCorrect && metadata.getEnsembleSize()==ensSize && metadata.getWriteQuorumSize()==writeQuorumSize;
        }
        if(type==Type.CREATE_5){
            isCorrect = isCorrect && metadata.getEnsembleSize()==ensSize && metadata.getWriteQuorumSize()==writeQuorumSize && metadata.getAckQuorumSize()==ackQuorumSize ;
        }
        return  isCorrect;
    }

    @Test
    public void testCreateLedgerEx() throws BKException, InterruptedException {
        Assume.assumeTrue(type==Type.CREATE_EX);
        boolean isPassed=false;
        try {
            client.createLedger(ensSize, writeQuorumSize, ackQuorumSize, digestType, passwd);
        }catch(Exception e){
            isPassed=true;
        }
        Assert.assertTrue(isPassed);
    }
}

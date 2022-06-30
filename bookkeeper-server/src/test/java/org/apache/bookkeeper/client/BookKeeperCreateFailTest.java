package org.apache.bookkeeper.client;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@RunWith(Parameterized.class)
public class BookKeeperCreateFailTest extends BookKeeperTestBaseClass{
    private int ensSize;
    private int writeQuorumSize;
    private int ackQuorumSize;
    private BookKeeper.DigestType digestType;
    private byte[] passwd;
    private Map<String,byte[]> customMetadata;

    public BookKeeperCreateFailTest(int ensSize, int writeQuorumSize, int ackQuorumSize, BookKeeper.DigestType digestType, byte[] passwd, Map<String, byte[]> customMetadata) {
        this.ensSize = ensSize;
        this.writeQuorumSize = writeQuorumSize;
        this.ackQuorumSize = ackQuorumSize;
        this.digestType = digestType;
        this.passwd = passwd;
        this.customMetadata = customMetadata;
    }

    @Parameterized.Parameters
    public static Collection configure(){
        return Arrays.asList(new Object[][] {
                //{1, 2, 1, BookKeeper.DigestType.MAC, new byte[]{}, null}
                //{0, 1, 1, BookKeeper.DigestType.MAC, "1010".getBytes(), null},
                {1, 0, 1, BookKeeper.DigestType.MAC, "1010".getBytes(), null},
                {0, 0, 1, BookKeeper.DigestType.CRC32, "1010".getBytes(), null},
                //{-1, 0, 1, BookKeeper.DigestType.MAC, "1010".getBytes(), null},
                //{0, 1, 0, BookKeeper.DigestType.MAC, "1010".getBytes(), null},
                //{-1, 0, 0, BookKeeper.DigestType.MAC, new byte[]{}, null},
                {0, -1, 0, BookKeeper.DigestType.MAC, "1010".getBytes(), null},
                {-1, -1, 0, BookKeeper.DigestType.MAC, "1010".getBytes(), null},
                //{-2, -1, 0, BookKeeper.DigestType.MAC, new byte[]{}, null},
                //{-1, 0, -1, BookKeeper.DigestType.MAC, "1010".getBytes(), null},
                {-1, -1, -1, BookKeeper.DigestType.MAC, new byte[]{}, null},
                //{-2, -1, -1, BookKeeper.DigestType.MAC, "1010".getBytes(), null},
                {-1, -2, -1, BookKeeper.DigestType.MAC, "1010".getBytes(), null},
                {-2, -2, -1, BookKeeper.DigestType.MAC, "1010".getBytes(), new HashMap<String, byte[]>()},
                //{-3, -2, -1, BookKeeper.DigestType.MAC, "1010".getBytes(), null}
        });
    }

    @Test
    public void testCreateLedgerEx() throws BKException, InterruptedException {
        boolean isPassed=false;
        try {
            client.createLedger(ensSize, writeQuorumSize, ackQuorumSize, digestType, passwd, customMetadata);
        }catch(Exception e){
            isPassed=true;
        }
        Assert.assertTrue(isPassed);
    }
}

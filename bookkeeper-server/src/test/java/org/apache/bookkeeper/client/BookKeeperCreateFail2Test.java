package org.apache.bookkeeper.client;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@Ignore
@RunWith(Parameterized.class)
public class BookKeeperCreateFail2Test extends BookKeeperTestBaseClass{
    private int ensSize;
    private int writeQuorumSize;
    private BookKeeper.DigestType digestType;
    private byte[] passwd;
    private int ackQuorumSize;

    public BookKeeperCreateFail2Test(int ensSize, int writeQuorumSize, BookKeeper.DigestType digestType, byte[] passwd, int ackQuorumSize) {
        this.ensSize = ensSize;
        this.writeQuorumSize = writeQuorumSize;
        this.digestType = digestType;
        this.passwd = passwd;
        this.ackQuorumSize = ackQuorumSize;
    }

    @Parameterized.Parameters
    public static Collection configure(){
        return Arrays.asList(new Object[][] {
                {1, 0, 1, BookKeeper.DigestType.MAC, "1010".getBytes()},
                {0, 0, 1, BookKeeper.DigestType.CRC32, "1010".getBytes()},
                {0, -1, 0, BookKeeper.DigestType.MAC, "1010".getBytes()},
                {-1, -1, 0, BookKeeper.DigestType.MAC, "1010".getBytes()},
                {-1, -1, -1, BookKeeper.DigestType.MAC, new byte[]{}},
                {-1, -2, -1, BookKeeper.DigestType.MAC, "1010".getBytes()},
                {-2, -2, -1, BookKeeper.DigestType.MAC, "1010".getBytes()}
        });
    }

    @Test
    public void testCreateLedgerEx() throws BKException, InterruptedException {
        boolean isPassed=false;
        try {
            client.createLedger(ensSize, writeQuorumSize, ackQuorumSize, digestType, passwd);
        }catch(Exception e){
            isPassed=true;
        }
        Assert.assertTrue(isPassed);
    }
}

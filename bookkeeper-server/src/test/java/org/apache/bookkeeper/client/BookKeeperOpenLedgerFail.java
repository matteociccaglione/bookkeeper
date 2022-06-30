package org.apache.bookkeeper.client;


import org.junit.Assert;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;


@RunWith(Parameterized.class)
public class BookKeeperOpenLedgerFail extends BookKeeperTestBaseClass{
    private BookKeeper.DigestType digestType;
    private byte[] passwd;

    public BookKeeperOpenLedgerFail(BookKeeper.DigestType digestType, byte[] passwd, BookKeeperLedgerTest.Type type) {
        this.digestType = digestType;
        this.passwd = passwd;
        this.type = type;
    }

    @Parameterized.Parameters
    public static Collection configure() {
        return Arrays.asList(new Object[][]{
                {BookKeeper.DigestType.MAC, "1010".getBytes(StandardCharsets.UTF_8), BookKeeperLedgerTest.Type.LEDGER_NEX},
                {BookKeeper.DigestType.MAC, "1010".getBytes(StandardCharsets.UTF_8), BookKeeperLedgerTest.Type.BAD_PASS},
                {BookKeeper.DigestType.CRC32, "1010".getBytes(StandardCharsets.UTF_8), BookKeeperLedgerTest.Type.BAD_PASS}
        });
    }



    private BookKeeperLedgerTest.Type type;
    @Test
    public void testOpenLedgerFailNoRecovery() throws BKException, InterruptedException {
        Assume.assumeTrue(type== BookKeeperLedgerTest.Type.LEDGER_NEX);
        try {
            LedgerHandle handle = client.openLedgerNoRecovery(-10, this.digestType, this.passwd);
            System.out.println(handle.ledgerId);
        }catch(Exception e){
            Assert.assertTrue(true);
            return;
        }
        Assert.fail();
    }

    @Test
    public void testOpenLedgerPassBad() throws BKException, InterruptedException {
        Assume.assumeTrue(type== BookKeeperLedgerTest.Type.BAD_PASS);
        LedgerHandle handle = client.createLedger(this.digestType,"bad_pass".getBytes(StandardCharsets.UTF_8));
        try{
            client.openLedger(handle.ledgerId,this.digestType,this.passwd);
        }catch(Exception e){
            Assert.assertTrue(true);
            return;
        }
        Assert.fail();
    }
    @Test
    public void testOpenLedgerFail() throws BKException, InterruptedException {
        Assume.assumeTrue(type== BookKeeperLedgerTest.Type.LEDGER_NEX);
        try {
            LedgerHandle handle = client.openLedger(-10, this.digestType, this.passwd);
            System.out.println(handle.ledgerId);
        }catch(Exception e){
            Assert.assertTrue(true);
            return;
        }
        Assert.fail();
    }

    @Test
    public void testOpenLedgerPassBadNoRecovery() throws BKException, InterruptedException {
        Assume.assumeTrue(type== BookKeeperLedgerTest.Type.BAD_PASS);
        LedgerHandle handle = client.createLedger(this.digestType,"bad_pass".getBytes(StandardCharsets.UTF_8));
        try{
            client.openLedgerNoRecovery(handle.ledgerId,this.digestType,this.passwd);
        }catch(Exception e){
            Assert.assertTrue(true);
            return;
        }
        Assert.fail();
    }
}

package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.api.DigestType;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
@Ignore
@RunWith(Parameterized.class)
public class BookKeeperLedgerTest extends BookKeeperTestBaseClass{
    private BookKeeper.DigestType digestType;
    private byte[] passwd;
    private Type type;
    enum Type{
        LEDGER_EXIST,
        LEDGER_NEX,
        BAD_PASS
    }
    public BookKeeperLedgerTest(BookKeeper.DigestType digestType, byte[] passwd, Type type) throws BKException, IOException, InterruptedException {
        this.digestType = digestType;
        this.passwd = passwd;
        this.type = type;

    }

    @Parameterized.Parameters
    public static Collection configure(){
        return Arrays.asList(new Object[][] {
                {BookKeeper.DigestType.MAC,"1010".getBytes(StandardCharsets.UTF_8),Type.LEDGER_EXIST},
                {BookKeeper.DigestType.CRC32,"1010".getBytes(StandardCharsets.UTF_8),Type.LEDGER_EXIST},
                {BookKeeper.DigestType.MAC,"1010".getBytes(StandardCharsets.UTF_8),Type.LEDGER_NEX},
                {BookKeeper.DigestType.MAC,"1010".getBytes(StandardCharsets.UTF_8),Type.BAD_PASS},
                {BookKeeper.DigestType.CRC32,"1010".getBytes(StandardCharsets.UTF_8),Type.BAD_PASS}
        });
    }

    @Test
    public void testOpenLedger() throws BKException, InterruptedException {
        Assume.assumeTrue(type == Type.LEDGER_EXIST);
        LedgerHandle handle = client.createLedger(this.digestType,this.passwd);
        byte[] entryValue = "test".getBytes(StandardCharsets.UTF_8);
        handle.addEntry("test".getBytes(StandardCharsets.UTF_8));
        long id = handle.getId();
        handle.close();
        LedgerHandle handle1 = client.openLedger(id,this.digestType,this.passwd);
        try{
            LedgerEntry entry = handle1.readLastEntry();
            byte[] result = entry.getEntry();
            boolean isCorrect = result.length == entryValue.length;
            if(isCorrect){
                for (int i = 0; i < result.length; i++){
                    if(result[i]!=entryValue[i]){
                        isCorrect = false;
                        break;
                    }
                }
            }
            Assert.assertTrue(isCorrect);
        }catch(Exception e){
            Assert.fail();
        }
    }
    @Test
    public void testOpenLedgerNoRecovery() throws BKException, InterruptedException {
        Assume.assumeTrue(type == Type.LEDGER_EXIST);
        LedgerHandle handle = client.createLedger(this.digestType,this.passwd);
        byte[] entryValue = "test".getBytes(StandardCharsets.UTF_8);
        handle.addEntry("test".getBytes(StandardCharsets.UTF_8));
        long id = handle.getId();
        handle.close();
        LedgerHandle handle1 = client.openLedgerNoRecovery(id,this.digestType,this.passwd);
        try{
            LedgerEntry entry = handle1.readLastEntry();
            byte[] result = entry.getEntry();
            boolean isCorrect = result.length == entryValue.length;
            if(isCorrect){
                for (int i = 0; i < result.length; i++){
                    if(result[i]!=entryValue[i]){
                        isCorrect = false;
                        break;
                    }
                }
            }
            Assert.assertTrue(isCorrect);
        }catch(Exception e){
            Assert.fail();
        }
    }

    @Test
    public void testOpenLedgerFail() throws BKException, InterruptedException {
        Assume.assumeTrue(type==Type.LEDGER_NEX);
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
    public void testOpenLedgerFailNoRecovery() throws BKException, InterruptedException {
        Assume.assumeTrue(type==Type.LEDGER_NEX);
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
        Assume.assumeTrue(type==Type.BAD_PASS);
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
    public void testOpenLedgerPassBadNoRecovery() throws BKException, InterruptedException {
        Assume.assumeTrue(type==Type.BAD_PASS);
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

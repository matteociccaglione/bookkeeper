package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.versioning.Versioned;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

@RunWith(Parameterized.class)
public class BookKeeperDeleteTest extends BookKeeperTestBaseClass{
    private Type type;
    enum Type{
        DELETE_OK,
        DELETE_FAIL
    }

    public BookKeeperDeleteTest(Type type){
        this.type=type;
    }

    @Parameterized.Parameters
    public static Collection configure(){
        return Arrays.asList(new Object[][] {
                {Type.DELETE_OK},
                //{Type.DELETE_FAIL}
        });
    }

    @Test
    public void testDelete(){

        long lId = 55555;
        boolean isPassed=false;
        LedgerHandle handle = null;
        if(type== Type.DELETE_OK){
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
                CompletableFuture<Versioned<LedgerMetadata>> future = client.getLedgerManager().readLedgerMetadata(lId);
                try{
                    SyncCallbackUtils.waitForResult(future);
                }catch(BKException.BKNoSuchLedgerExistsOnMetadataServerException e){
                    isPassed=true;
                }
            }
        }catch(BKException | InterruptedException e ){
            if(type== Type.DELETE_FAIL){
                isPassed=true;
            }
        }
        Assert.assertTrue(isPassed);
    }
}

package org.apache.bookkeeper;

import org.apache.bookkeeper.client.BKException;
import org.junit.Assert;
import org.junit.Test;
import org.apache.bookkeeper.client.BookKeeper;

import java.io.IOException;

public class TestCaso {
    @Test
    public void testCaso()  {
        BKException exc = BKException.create(10);
        Assert.assertEquals(-999,exc.getCode());
    }
}

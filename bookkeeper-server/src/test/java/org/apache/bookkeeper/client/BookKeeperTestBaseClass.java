package org.apache.bookkeeper.client;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class BookKeeperTestBaseClass {
    protected static LocalBookKeeper bookKeeper;
    protected static BookKeeper client;
    @Before
    public  void startServer() throws Exception {
        ServerConfiguration configuration = new ServerConfiguration();
        configuration.setAllowLoopback(true);
        bookKeeper = LocalBookKeeper.getLocalBookies("127.0.0.1",34567,3,true,configuration);
        bookKeeper.start();
        client = new BookKeeper("127.0.0.1:34567");
    }
    @After
    public void closeServer() throws Exception {
        client.close();
        //this.bookKeeper.shutdownBookies();
        bookKeeper.close();
    }
}

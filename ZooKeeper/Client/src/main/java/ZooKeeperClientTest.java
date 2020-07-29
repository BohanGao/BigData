import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class ZooKeeperClientTest {
    private ZooKeeper zooKeeperCli;
    private static final String CONNECTION_STRING = "51.143.127.112:2181,52.148.148.162:2181,52.137.105.52:2181";
    private static final int SESSION_TIMEOUT = 2000;

    @Before
    public void before() throws IOException {
//        zooKeeperCli = new ZooKeeper(CONNECTION_STRING, SESSION_TIMEOUT, new Watcher() {
//            public void process(WatchedEvent watchedEvent) {
//                System.out.println("Default callback for of Watcher.");
//            }
//        });
        zooKeeperCli = new ZooKeeper(CONNECTION_STRING, SESSION_TIMEOUT, (p) -> {
            System.out.println("Default callback for of Watcher.");
        });
    }

    @Test
    public void ls() throws KeeperException, InterruptedException {
        //zooKeeperCli.getChildren("/", true);
        final List<String> children = zooKeeperCli.getChildren("/", p -> {
            System.out.println("Customized callback for of Watcher.");
        });
        for (String child : children) {
            System.out.println(child);
        }

        Thread.sleep(Long.MAX_VALUE);//main thread sleeps to demo watch event
    }

    @Test
    public void create() throws KeeperException, InterruptedException {
        final String s = zooKeeperCli.create("/clientTest", "create test".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        System.out.println(s);
    }

    @Test
    public void get() throws KeeperException, InterruptedException {
        final byte[] data = zooKeeperCli.getData("/clientTest", true, new Stat());

        System.out.println(new String(data));
    }

    @Test
    public void set() throws KeeperException, InterruptedException {
        final Stat stat = zooKeeperCli.setData("/clientTest", "some new value".getBytes(), 0);

        System.out.println(stat.getDataLength());
    }

    @Test
    public void stat() throws KeeperException, InterruptedException {
        final Stat exists = zooKeeperCli.exists("/clientTest", false);
        if (exists == null) {
            System.out.println("znode does not exist");
        } else {
            System.out.println(exists.getDataLength());
        }
    }

    @Test
    public void delete() throws KeeperException, InterruptedException {
        final Stat exists = zooKeeperCli.exists("clientTest", false);
        if (exists != null) {
            zooKeeperCli.delete("/clientTest", exists.getVersion());
        }
    }
}

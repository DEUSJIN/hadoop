package pers.jin.zookeeper01;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: DEUSJIN
 * @Date: 2021/3/17 20:04
 */
public class DistributeClient {
    private ZooKeeper zk;
    private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int sessionTimeout = 2000;

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        DistributeClient distributeClient = new DistributeClient();
        distributeClient.getConn();
        distributeClient.regist();
        distributeClient.business();
    }

    private void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }

    private void regist() throws KeeperException, InterruptedException {
        ArrayList<String> strings = new ArrayList<>();
        List<String> children = zk.getChildren("/servers", true);
        for (String child : children) {
            byte[] data = zk.getData("/servers/" + child, false, null);
            strings.add(new String(data));
        }
        System.out.println("==========start=========");
        System.out.println(strings);
        System.out.println("==========stop=========");
    }

    private void getConn() throws IOException {
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                try {
                    regist();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}

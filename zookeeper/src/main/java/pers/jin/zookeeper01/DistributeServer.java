package pers.jin.zookeeper01;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * @Author: DEUSJIN
 * @Date: 2021/3/17 19:50
 */
public class DistributeServer {
    private ZooKeeper zk;
    private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int sessionTimeout = 2000;

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        DistributeServer distributeServer = new DistributeServer();

//        1.连接
        distributeServer.getConn();

//        2.注册
        distributeServer.regist(args[0]);

//        3.事务逻辑
        distributeServer.business();
    }

    private void getConn() throws IOException {
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }

    private void regist(String hostname) throws KeeperException, InterruptedException {
        zk.create("/servers/server", hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname + " is online!");
    }

    private void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }
}

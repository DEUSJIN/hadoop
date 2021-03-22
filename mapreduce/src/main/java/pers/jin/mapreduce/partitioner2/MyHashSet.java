package pers.jin.mapreduce.partitioner2;

/**
 * @Author: DEUSJIN
 * @Date: 2021/3/12 9:28
 */
public class MyHashSet {

    Node[] set;

    class Node {
        int val;
        Node next;
        Node pre;

        @Override
        public String toString() {
            return "Node{" + "val=" + val + '}';
        }
    }

    /**
     * Initialize your data structure here.
     */
    public MyHashSet() {
        set = new Node[10000];
    }

    public void add(int key) {
        int idx = key % set.length;
        if (set[idx] == null) {
            set[idx] = new Node();
            set[idx].val = key;
            return;
        }
        Node tmp = set[idx];
        while (tmp.next != null && tmp.val != key) {
            tmp = tmp.next;
        }
        if (tmp.val != key) {
            Node newNode = new Node();
            newNode.val = key;
            tmp.next = newNode;
            newNode.pre = tmp;
        }
    }

    public void remove(int key) {
        int idx = key % set.length;
        Node node = set[idx];
        Node tmp = node;
        if(node == null){
            return;
        }
        if (node.val == key) {
            set[idx] = node.next;
        } else {
            while (tmp != null && tmp.val != key) {
                tmp = tmp.next;
            }
            if (tmp != null) {
                tmp.pre.next = tmp.next;
                if(tmp.next != null) {
                    tmp.next.pre = tmp.pre;
                }
            }
        }
    }

    /**
     * Returns true if this set contains the specified element
     */
    public boolean contains(int key) {
        Node node = set[key % set.length];
        while (node != null && node.val != key) {
            node = node.next;
        }
        return node != null;
    }

    public void show() {
        for (Node node : set) {
            if (node != null) {
                System.out.println(node.val);
            }
        }
    }

    public static void main(String[] args) {
        MyHashSet myHashSet = new MyHashSet();
    }
}

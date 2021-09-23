package edu.yu.cs.com1320.project.impl;

import edu.yu.cs.com1320.project.*;
import java.util.*;

public class TrieImpl<Value> implements Trie<Value> {
    private final int alphabetSize = 36;

    private Node root;

    public TrieImpl() {
        this.root = new Node();
    }

    private class Node<Value> {
        protected List<Value> val;
        protected Node[] links;

        public Node() {
            this.val = new ArrayList<Value>();
            this.links = new Node[alphabetSize];
        }
    }

    public void put(String key, Value val) {
        if (key == null) {
            throw new IllegalArgumentException();
        }

        if (val != null) {
            this.root = put(this.root, key, val, 0);
        }
    }

    private Node put(Node x, String key, Value val, int d) {
        key = key.toLowerCase();
        //create a new node
        if (x == null) {
            x = new Node();
        }
        //we've reached the last node in the key,
        //set the value for the key and return the node
        if (d == key.length()) {
            x.val.add(val);
            return x;
        }
        //proceed to the next node in the chain of nodes that
        //forms the desired key
        char c = key.charAt(d);
        int charIndex = c >= 'a' ? c - 97 : c - 22;
        x.links[charIndex] = this.put(x.links[charIndex], key, val, d + 1);
        return x;
    }

    //Essentially just getValue
    public List<Value> getAllSorted (String key, Comparator<Value> comparator) {
        if (key == null || comparator == null) {
            throw new IllegalArgumentException();
        }

        Node x = this.get(this.root, key.toLowerCase(), 0);
        if (x == null) {
            return new ArrayList<Value>();
        }
        Set<Value> resultSet = new HashSet<Value>(x.val);
        List<Value> results = new ArrayList<Value>(resultSet);
        Collections.sort(results, comparator);
        return results;
    }

    public List<Value> getAllWithPrefixSorted (String prefix, Comparator<Value> comparator) {
        if (prefix == null || comparator == null) {
            throw new IllegalArgumentException();
        }

        if (prefix.equals("")) {
            return new ArrayList<Value>();
        }

        Node prefixNode = this.get(this.root, prefix.toLowerCase(), 0);
        if (prefixNode == null) {
            return new ArrayList<Value>();
        }
        List<Value> children = new ArrayList<Value>(this.getAllChildren(prefixNode, new HashSet<Value>()));
        Collections.sort(children, comparator);
        return children;
    }

    private Set<Value> getAllChildren(Node node, Set<Value> childrenFound) {
        if (node == null) {
            return new HashSet<Value>();
        }

        childrenFound.addAll(node.val);

        for (int i = 0; i < node.links.length; i++) {
            if (node.links[i] != null) {
                break;
            }

            if (i == node.links.length - 1) {//made it through all links, they're all null
                return childrenFound;
            }
        }

        for (int i = 0; i < node.links.length; i++) {
            if (node.links[i] != null) {
                childrenFound.addAll(getAllChildren(node.links[i], childrenFound));
            }
        }

        return childrenFound;
    }

    private Node get(Node x, String key, int d) {
        key = key.toLowerCase();
        //link was null - return null, indicating a miss
        if (x == null) {
            return null;
        }
        //we've reached the last node in the key,
        //return the node
        if (d == key.length()) {
            return x;
        }
        //proceed to the next node in the chain of nodes that
        //forms the desired key
        char c = key.charAt(d);
        int charIndex = c >= 'a' ? c - 97 : c - 22;
        return this.get(x.links[charIndex], key, d + 1);
    }

    public Set<Value> deleteAllWithPrefix(String prefix) {
        if (prefix == null) {
            throw new IllegalArgumentException();
        }

        if (prefix.equals("")) {
            return new HashSet<Value>();
        }

        Node prefixNode = this.get(this.root, prefix.toLowerCase(), 0);
        if (prefixNode == null) {
            return new HashSet<Value>();
        }
        List<Value> children = new ArrayList<Value>(this.getAllChildren(prefixNode, new HashSet<Value>()));
        Set<Value> returnVal = new HashSet<>(children);
        prefixNode.val = new ArrayList<Value>();
        prefixNode.links = new Node[alphabetSize];
        return returnVal;
    }

    public Set<Value> deleteAll (String key) {
        if (key == null) {
            throw new IllegalArgumentException();
        }

        Set<Value> returnVal;
        Node node = this.get(this.root, key.toLowerCase(), 0);
        if (node == null) {
            returnVal = new HashSet<Value>();
        } else {
            returnVal = new HashSet<Value>(node.val);
        }
        this.root = deleteAll(this.root, key, 0);
        return returnVal;
    }

    public Value delete (String key, Value val) {
        if (key == null || val == null) {
            throw new IllegalArgumentException();
        }

        if (this.get(this.root,key.toLowerCase(), 0) != null) {
            List<Value> returnVal = this.get(this.root, key.toLowerCase(), 0).val;
            if (!returnVal.contains(val)) {
                return null;
            }
            Node node = this.get(this.root, key.toLowerCase(), 0);
            Value nodeVal = (Value) node.val.get(node.val.indexOf(val));
            node.val.remove(val);
            //System.out.println("Successfully removed " + val);
            return nodeVal;
        }
        return null;
    }

    private Node deleteAll(Node x, String key, int d) {
        key = key.toLowerCase();
        if (x == null) {
            return null;
        }
        //we're at the node to del - set the val to null
        if (d == key.length()) {
            x.val.clear();
        }
        //continue down the trie to the target node
        else {
            char c = key.charAt(d);
            int charIndex = c >= 'a' ? c - 97 : c - 22;
            x.links[charIndex] = this.deleteAll(x.links[charIndex], key, d + 1);
        }
        //this node has a val – do nothing, return the node
        if (x.val.size() > 0) {
            return x;
        }
        //remove subtrie rooted at x if it is completely empty	
        for (int c = 0; c < alphabetSize; c++) {
            if (x.links[c] != null) {
                return x; //not empty
            }
        }
        //empty - set this link to null in the parent
        return null;
    }
}
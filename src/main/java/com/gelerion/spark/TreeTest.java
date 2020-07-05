package com.gelerion.spark;

public class TreeTest {

    public static void main(String[] args) {
        Tree tree = new Tree();
        tree.root = new Node(68);
        tree.root.left = new Node(26);
        tree.root.right = new Node(42);
        tree.root.left.left = new Node(22);
        tree.root.left.right = new Node(4);
        tree.root.right.left = new Node(29);
        tree.root.right.right = new Node(13);

        System.out.println(tree.isSumTree(tree.root));
    }

    static class Tree {
        Node root;

        public void add(int n) {
            if (root == null) {
                root = new Node(n);
                return;
            }

            return;
        }

        boolean isSumTree(Node node) {
            if (node == null) {
                return false;
            }

            int val = node.value;
            if (node.left != null && node.right != null) {
                 int sum = sumChildren(node);
                 if (sum == val) {
                     //check children
                     return isSumTree(node.left) && isSumTree(node.right);
                 } else {
                     return false;
                 }
            }

            return node.left == null || node.right == null;
        }

        private int sumChildren(Node node) {
            return node.left.value + node.right.value;
        }
    }

    static class Node {
        Node left;
        Node right;
        int value;

        Node(int n) {
            value = n;
        }
    }
}



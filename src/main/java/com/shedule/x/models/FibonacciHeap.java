package com.shedule.x.models;

import java.util.ArrayList;
import java.util.List;

public class FibonacciHeap<T> {
    private FibNode<T> min;
    private int size;

    public FibNode<T> insert(T data, int key) {
        FibNode<T> node = new FibNode<>(data, key);
        min = meld(min, node);
        size++;
        return node;
    }

    public boolean isEmpty() {
        return min == null;
    }

    public T extractMin() {
        FibNode<T> oldMin = min;
        if (oldMin != null) {
            if (oldMin.child != null) {
                List<FibNode<T>> children = new ArrayList<>();
                FibNode<T> x = oldMin.child;
                do {
                    children.add(x);
                    x = x.right;
                } while (x != oldMin.child);

                for (FibNode<T> child : children) {
                    child.parent = null;
                    min = meld(min, child);
                }
            }

            removeFromList(oldMin);
            if (oldMin == oldMin.right) {
                min = null;
            } else {
                min = oldMin.right;
                consolidate();
            }
            size--;
        }
        return oldMin != null ? oldMin.data : null;
    }

    public void decreaseKey(FibNode<T> node, int newKey) {
        if (newKey > node.key) throw new IllegalArgumentException("New key is larger");
        node.key = newKey;
        FibNode<T> parent = node.parent;
        if (parent != null && node.key < parent.key) {
            cut(node, parent);
            cascadingCut(parent);
        }
        if (node.key < min.key) {
            min = node;
        }
    }

    private void consolidate() {
    }

    private void cut(FibNode<T> node, FibNode<T> parent) {
        removeFromList(node);
        parent.degree--;
        if (parent.child == node) parent.child = node.right != node ? node.right : null;
        node.parent = null;
        node.mark = false;
        min = meld(min, node);
    }

    private void cascadingCut(FibNode<T> node) {
        FibNode<T> parent = node.parent;
        if (parent != null) {
            if (!node.mark) node.mark = true;
            else {
                cut(node, parent);
                cascadingCut(parent);
            }
        }
    }

    private void removeFromList(FibNode<T> node) {
        node.left.right = node.right;
        node.right.left = node.left;
    }

    private FibNode<T> meld(FibNode<T> a, FibNode<T> b) {
        if (a == null) return b;
        if (b == null) return a;

        FibNode<T> aRight = a.right;
        a.right = b.right;
        a.right.left = a;
        b.right = aRight;
        b.right.left = b;

        return a.key < b.key ? a : b;
    }
}

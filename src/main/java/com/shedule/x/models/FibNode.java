package com.shedule.x.models;

public class FibNode<T> {
    T data;
    int key;
    FibNode<T> parent, child, left, right;
    int degree;
    boolean mark;

    public FibNode(T data, int key) {
        this.data = data;
        this.key = key;
        this.degree = 0;
        this.mark = false;
        this.left = this.right = this;
    }
}

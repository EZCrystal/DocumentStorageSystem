package edu.yu.cs.com1320.project.impl;

import edu.yu.cs.com1320.project.MinHeap;

import java.lang.reflect.Array;
import java.util.*;

import java.lang.Class;

public class MinHeapImpl<E extends Comparable<E>> extends MinHeap<E> {
	public MinHeapImpl() {
		this.elements = (E[])new Comparable[5];
	}

	public void reHeapify(E element) {
		this.upHeap(this.getArrayIndex(element));
		this.downHeap(this.getArrayIndex(element));
	}

    public int getArrayIndex(E element) {
        int index = Arrays.asList(this.elements).indexOf(element);
        if (index == -1) {
            throw new NoSuchElementException();
        }
        return index;
    }

    protected void doubleArraySize() {
    	E[] doubledArray = (E[])new Comparable[this.elements.length * 2];
    	for (int i = 0; i < this.elements.length; i++) {
    		doubledArray[i] = this.elements[i];
    	}
    	this.elements = doubledArray;
    }

    public boolean isEmpty() {
        return this.count == 0;
    }
}
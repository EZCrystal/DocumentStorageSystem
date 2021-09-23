package edu.yu.cs.com1320.project.impl;

import edu.yu.cs.com1320.project.Stack;

public class StackImpl<T> implements Stack<T> {
	private class ListElement<T> {
		private T value;
		private ListElement next;

		public ListElement (T value) {
			this.value = value;
			this.next = null;
		}

		public T value() {
			if (this.value != null) {
				return this.value;
			} else {
				return null;
			}
		}

		public ListElement next() {
			if (this.next != null) {
				return this.next;
			} else {
				return null;
			}
		}

		public void setValue(T v) {
			this.value = v;
		}

		public void setNext(ListElement next) {
			this.next = next;
		}

		public int length() {
			if (this.next == null) {
				return 1;
			} else {
				ListElement i = this.next;
				int j = 1;
				while (i.next() != null) {
					i = i.next();
					j++;
				}
				j++;

				return j;
			}
		}
	}

	private ListElement data;

	public StackImpl() {
		this.data = null;
	}

	public void push(T element) {
		if (element == null) {
			throw new IllegalArgumentException();
		}

		if (this.data == null) {
			this.data = new ListElement(element);
		} else {
			ListElement e = new ListElement(element);
			ListElement oldHeadCopy = new ListElement(this.data.value());
			ListElement oldHeadNextCopy = this.data.next();
			oldHeadCopy.setNext(oldHeadNextCopy);
			e.setNext(oldHeadCopy);
			this.data = e;
		}
	}

	public T pop() {
		if (this.data == null) {
			return null;
		} else {
			T headValue = (T) this.data.value();
			this.data = this.data.next();
			return headValue;
		}
	}

	public T peek() {
		if (this.data != null) {
			return (T) this.data.value();
		}

		return null;
	}

	public int size() {
		if (this.data != null) {
			return this.data.length();
		}

		return 0;
	}
}
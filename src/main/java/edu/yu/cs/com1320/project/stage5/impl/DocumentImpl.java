package edu.yu.cs.com1320.project.stage5.impl;

import edu.yu.cs.com1320.project.stage5.Document;
import java.net.URI;
import java.util.*;
import java.io.*;

public class DocumentImpl implements Document, Serializable {
	private URI uri;
	private String text;
	private byte[] binaryData;
	private HashMap<String, Integer> wordData;
	private transient long lastUseTime;

	public DocumentImpl(URI uri, String txt) {
		if (uri == null || txt == null || uri.toString().equals("") || txt.equals("")) {
			throw new IllegalArgumentException();
		}

		this.uri = uri;
		this.text = txt;
		this.lastUseTime = System.nanoTime();

		this.wordData = new HashMap<String, Integer>();
		String simplifiedText = txt;
		simplifiedText = simplifiedText.toLowerCase();
		simplifiedText = simplifiedText.replaceAll("[^a-z0-9 ]", "");
		simplifiedText = simplifiedText.replaceAll("\\s{2,}", " ").trim();
		String[] words = simplifiedText.split(" ");
		for (int i = 0; i < words.length; i++) {
			this.wordData.put(words[i], 1 + wordData.getOrDefault(words[i], 0));
		}
	}

	public DocumentImpl(URI uri, byte[] binaryData) {
		if (uri == null || binaryData == null || uri.toString().equals("") || binaryData.length == 0) {
			throw new IllegalArgumentException();
		}

		this.uri = uri;
		this.binaryData = binaryData;
		this.lastUseTime = System.nanoTime();
		this.wordData = new HashMap<String, Integer>();
	}

	public String getDocumentTxt() {
		if (this.text != null) {
			return this.text;
		}

		return null;
	}

	public byte[] getDocumentBinaryData() {
		if (this.binaryData != null) {
			return this.binaryData;
		}

		return null;
	}

	public URI getKey() {
		return this.uri;
	}

	public int wordCount(String word) {
		if (this.binaryData != null) {
			return 0;
		}

		word = word.toLowerCase();
		word = word.replaceAll("[^a-z0-9]", "");
		return this.wordData.getOrDefault(word, 0);
	}

	public Set<String> getWords() {
		return this.wordData.keySet();
	}

	@Override
	public boolean equals(Object other) {
		if (other == null) {
			return false;//this one def. isn't null - how else are we calling .equals on it?
		}

		return this.hashCode() == other.hashCode();
	}

	@Override
	public int hashCode() {
	    int result = uri.hashCode();
	    result = 31 * result + (text != null ? text.hashCode() : 0);
	    result = 31 * result + Arrays.hashCode(binaryData);
	    return result;
	}

	public long getLastUseTime() {
		return this.lastUseTime;
	}

    public void setLastUseTime(long timeInNanoseconds) {
    	this.lastUseTime = timeInNanoseconds;
    }

    public Map<String,Integer> getWordMap() {
    	return Collections.unmodifiableMap(this.wordData);
    }

    public void setWordMap(Map<String,Integer> wordMap) {
    	this.wordData = new HashMap<String, Integer>(wordMap);
    }

    @Override
    public int compareTo(Document doc) {
    	if (doc == null || this.getLastUseTime() < doc.getLastUseTime()) {
    		return -1;
    	} else if (this.getLastUseTime() > doc.getLastUseTime()) {
    		return 1;
    	}
    	return 0;
    }
}
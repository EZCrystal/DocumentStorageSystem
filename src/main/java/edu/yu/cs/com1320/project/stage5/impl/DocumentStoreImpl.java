package edu.yu.cs.com1320.project.stage5.impl;

import edu.yu.cs.com1320.project.*;
import edu.yu.cs.com1320.project.impl.*;
import edu.yu.cs.com1320.project.stage5.*;
import java.net.URI;
import java.io.*;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.*;

//some stage3 tests failing

public class DocumentStoreImpl implements DocumentStore {
	private BTreeImpl<URI, Document> data;
	private StackImpl<Undoable> commandStack;
	private TrieImpl<DocumentInfoHolder> trie;
	private MinHeapImpl<DocumentInfoHolder> heap;

	private Set<DocumentInfoHolder> documentHolderData;
	private File dir;

	private int maxDocumentCount;
	private int maxDocumentBytes;
	private int numDocs;
	private int numBytesUsed;

	public DocumentStoreImpl() {
		this.data = new BTreeImpl<URI, Document>();
		this.commandStack = new StackImpl<Undoable>();
		this.trie = new TrieImpl<DocumentInfoHolder>();
		this.heap = new MinHeapImpl<DocumentInfoHolder>();

		this.documentHolderData = new HashSet<DocumentInfoHolder>();
		this.dir = new File(System.getProperty("user.dir"));

		this.maxDocumentCount = -1;
		this.maxDocumentBytes = -1;
		this.numDocs = 0;
		this.numBytesUsed = 0;
	}

	public DocumentStoreImpl(File baseDir) {
		this.data = new BTreeImpl<URI, Document>();
		this.data.setPersistenceManager(new DocumentPersistenceManager(baseDir));
		this.commandStack = new StackImpl<Undoable>();
		this.trie = new TrieImpl<DocumentInfoHolder>();
		this.heap = new MinHeapImpl<DocumentInfoHolder>();

		this.documentHolderData = new HashSet<DocumentInfoHolder>();
		this.dir = baseDir;

		this.maxDocumentCount = -1;
		this.maxDocumentBytes = -1;
		this.numDocs = 0;
		this.numBytesUsed = 0;
	}

	private class DocumentInfoHolder implements Comparable<DocumentInfoHolder> {
		private Document doc;

		private DocumentInfoHolder(Document doc) {
			this.doc = doc;
		}

		private URI getKey() {
			return doc.getKey();
		}

		private long getLastUseTime() {
			return doc.getLastUseTime();
		}

		private Set<String> getWords() {
			return doc.getWords();
		}

		private int wordCount (String word) {
			return doc.wordCount(word);
		}

		private void setLastUseTime (long time) {
			doc.setLastUseTime(time);
		}

		private Document getDoc() {
			return this.doc;
		}

		@Override
		public boolean equals(Object o) {
			if (o == null) {
				return false;
			}
			return this.hashCode() == o.hashCode();
		}

		@Override
		public int hashCode() {
			return this.doc.getKey().hashCode();
		}

		@Override
		public int compareTo(DocumentInfoHolder otherDoc) {
			if (this.doc == null || this.doc.getLastUseTime() < otherDoc.getLastUseTime()) {
				return -1;
			} else if (this.doc.getLastUseTime() > otherDoc.getLastUseTime()) {
				return 1;
			}
			return 0;
		}
	}

	//completely removes least recently used doc
	private URI completelyRemoveDoc() throws IOException {
		DocumentInfoHolder removedDoc = this.heap.remove();
		this.data.moveToDisk(removedDoc.getKey());
		this.documentHolderData.remove(this.getDocumentInfoHolder(removedDoc.getKey()));
		this.numBytesUsed -= byteCount(removedDoc.getDoc());
		this.numDocs--;
		return removedDoc.getKey();
	}

	private void removeDocFromHeap(URI uri) {
		this.getDocumentInfoHolder(uri).setLastUseTime(0);
		//Reheapify and remove if in heap
		try {
			this.heap.reHeapify(this.getDocumentInfoHolder(uri));
			this.heap.remove();
		} catch (NoSuchElementException e) {}
	}

	private int byteCount(Document doc) {
		if (doc.getDocumentTxt() != null) {
			return doc.getDocumentTxt().getBytes().length;
		} else {
			return doc.getDocumentBinaryData().length;
		}
	}

	private DocumentInfoHolder getDocumentInfoHolder(URI uri) {
		for (DocumentInfoHolder doc : this.documentHolderData) {
			if (doc != null && doc.getKey().equals(uri)) {
				return doc;
			}
		}
		return null;
	}

	private int putDocumentWithoutUndo(InputStream input, URI uri, DocumentFormat format) throws IOException {
		//System.out.println("Putting " + uri);
		if (uri == null || format == null) {
			throw new IllegalArgumentException();
		}

		if (this.maxDocumentBytes != -1) {
			if (input.available() > this.maxDocumentBytes - this.numBytesUsed) {
				while (input.available() > this.maxDocumentBytes - this.numBytesUsed && this.documentHolderData.size() > 0) {
					this.completelyRemoveDoc();
				}
			}
		}

		if (this.maxDocumentCount != -1 && this.numDocs + 1 > this.maxDocumentCount && this.documentHolderData.size() > 0) {
			this.completelyRemoveDoc();
		}

		Document oldDoc = null;
		for (DocumentInfoHolder doc : documentHolderData) {
			if (doc.getKey().equals(uri)) {
				oldDoc = doc.getDoc();
			}
		}
		if (oldDoc != null) {
			if (oldDoc.getDocumentTxt() != null) {
				oldDoc = new DocumentImpl(oldDoc.getKey(), oldDoc.getDocumentTxt());
			} else {
				oldDoc = new DocumentImpl(oldDoc.getKey(), oldDoc.getDocumentBinaryData());
			}
		}
		Document finalOldDoc = oldDoc;

		int returnVal;

		if (input == null) {
			if (finalOldDoc == null) {
				returnVal = 0;
			} else {
				this.numBytesUsed -= this.byteCount(finalOldDoc);
				this.numDocs--;
				returnVal = data.put(uri, null).hashCode();
			}

			return returnVal;
		}

		byte[] contents = input.readAllBytes();

		Document newDoc = null;
		if (format == DocumentFormat.TXT) {
			newDoc = new DocumentImpl(uri, new String(contents, StandardCharsets.UTF_8));
		} else {
			newDoc = new DocumentImpl(uri, contents);
		}

		if (this.maxDocumentBytes == 0 || this.maxDocumentCount == 0) {
			Document returnDoc = this.data.put(uri, null);
			returnVal = returnDoc == null ? 0 : returnDoc.hashCode();
			(new DocumentPersistenceManager(this.dir)).serialize(uri, newDoc);
			return returnVal;
		}

		if (format == DocumentFormat.TXT) {
			if (finalOldDoc == null) {
				returnVal = 0;
				this.data.put(uri, newDoc);
				DocumentInfoHolder doc = new DocumentInfoHolder(newDoc);
				this.documentHolderData.add(doc);
				this.heap.insert(doc);
				if (newDoc != null) {
					newDoc.setLastUseTime(System.nanoTime());
					this.numDocs++;
					input.reset();
					this.numBytesUsed += input.available();
				}
			} else {
				for (String word : finalOldDoc.getWords()) {
					this.trie.delete(word, this.getDocumentInfoHolder(uri));
				}
				this.numBytesUsed -= this.byteCount(finalOldDoc);
				this.removeDocFromHeap(uri);
				this.documentHolderData.remove(this.getDocumentInfoHolder(uri));
				returnVal = this.data.put(uri, new DocumentImpl(uri, new String(contents, StandardCharsets.UTF_8))).hashCode();
				DocumentInfoHolder doc = new DocumentInfoHolder(newDoc);
				this.documentHolderData.add(doc);
				this.heap.insert(doc);
				input.reset();
				this.numBytesUsed += input.available();
			}

			if (data.get(uri) != null) {
				for (String word : data.get(uri).getWords()) {
					this.trie.put(word, this.getDocumentInfoHolder(uri));
				}
			}

			return returnVal;
		} else {
			if (finalOldDoc == null) {
				returnVal = 0;
				data.put(uri, newDoc);
				DocumentInfoHolder doc = new DocumentInfoHolder(newDoc);
				this.documentHolderData.add(doc);
				if (newDoc != null) {
					newDoc.setLastUseTime(System.nanoTime());
					this.numDocs++;
					input.reset();
					this.numBytesUsed += input.available();
				}
				this.heap.insert(doc);
			} else {
				this.numBytesUsed -= this.byteCount(finalOldDoc);
				this.removeDocFromHeap(uri);
				returnVal = this.data.put(uri, newDoc).hashCode();
				DocumentInfoHolder doc = new DocumentInfoHolder(newDoc);
				this.documentHolderData.add(doc);
				this.heap.insert(doc);
				input.reset();
				this.numBytesUsed += input.available();
			}

			if (this.data.get(uri) != null) {
				this.data.get(uri).setLastUseTime(System.nanoTime());
				this.heap.reHeapify(this.getDocumentInfoHolder(uri));
			}

			return returnVal;
		}
	}

	public int putDocument(InputStream input, URI uri, DocumentFormat format) throws IOException {
		if (uri == null || format == null) {
			throw new IllegalArgumentException();
		}

		List<URI> removedURIs = new ArrayList<URI>();

		if (this.maxDocumentBytes != -1) {
			if (input.available() > this.maxDocumentBytes - this.numBytesUsed) {
				while (input.available() > this.maxDocumentBytes - this.numBytesUsed && this.documentHolderData.size() > 0) {
					removedURIs.add(this.completelyRemoveDoc());
				}
			}
		}

		if (this.maxDocumentCount != -1 && this.numDocs + 1 > this.maxDocumentCount && this.documentHolderData.size() > 0) {
			removedURIs.add(this.completelyRemoveDoc());
		}

		Document oldDoc = null;
		for (DocumentInfoHolder doc : documentHolderData) {
			if (doc.getKey().equals(uri)) {
				oldDoc = doc.getDoc();
			}
		}
		if (oldDoc != null) {
			if (oldDoc.getDocumentTxt() != null) {
				oldDoc = new DocumentImpl(oldDoc.getKey(), oldDoc.getDocumentTxt());
			} else {
				oldDoc = new DocumentImpl(oldDoc.getKey(), oldDoc.getDocumentBinaryData());
			}
		}
		Document finalOldDoc = oldDoc;

		this.commandStack.push(new GenericCommand<URI>(uri, (URI uriVal) -> {
			for (URI uriToRemove : removedURIs) {
				this.getDocument(uriToRemove);
			}

			Document currentDoc = null;
			for (DocumentInfoHolder doc : documentHolderData) {
				if (doc.getKey().equals(uri)) {
					currentDoc = doc.getDoc();
				}
			}
			if (currentDoc != null) {
				if (currentDoc.getDocumentTxt() != null) {
					currentDoc = new DocumentImpl(currentDoc.getKey(), currentDoc.getDocumentTxt());
				} else {
					currentDoc = new DocumentImpl(currentDoc.getKey(), currentDoc.getDocumentBinaryData());
				}
			}
			Document finalCurrentDoc = currentDoc;

			if (finalCurrentDoc != null) {
				this.numBytesUsed -= this.byteCount(finalCurrentDoc);

				if (finalOldDoc == null) {
					this.numDocs--;
				}

				//System.out.println(this.getDocumentInfoHolder(finalCurrentDoc.getKey()));
				for (String word : finalCurrentDoc.getWords()) {
					//System.out.println("This should run. Uri: " + uri);
					//System.out.println("Deleted " + word);
					//System.out.println("Doc we're trying to delete: " + this.getDocumentInfoHolder(uriVal).getKey());
					//System.out.println(this.getDocumentInfoHolder(uriVal).getWords());
					//System.out.println("What was actually deleted: " + this.trie.delete(word, this.getDocumentInfoHolder(uriVal)).getKey());
					this.trie.delete(word, this.getDocumentInfoHolder(uriVal));
					/*List<Document> list = this.search(word);
					for (Document doc : list) {
						System.out.println(doc.getKey() + ": " + doc.getDocumentTxt());
					}*/
					//why is delete from trie sometimes not working??
				}
				this.removeDocFromHeap(uriVal);
				this.documentHolderData.remove(this.getDocumentInfoHolder(uriVal));
			} else {
				if (finalOldDoc != null) {
					this.numDocs++;
				}
			}
			this.data.put(uriVal, finalOldDoc);
			/*if (finalOldDoc == null && this.getDocumentInfoHolder(uri) != null) {
				for (String word : finalCurrentDoc.getWords()) {
					this.trie.delete(word, this.getDocumentInfoHolder(uri));
				}
			}*/
			if (finalOldDoc != null) {
				finalOldDoc.setLastUseTime(System.nanoTime());
				DocumentInfoHolder doc = new DocumentInfoHolder(finalCurrentDoc);
				this.documentHolderData.add(doc);
				this.heap.insert(doc);
				this.numBytesUsed += this.byteCount(finalCurrentDoc);
			}
			if (finalCurrentDoc != null) {
				for (String word : finalCurrentDoc.getWords()) {
					this.trie.put(word, this.getDocumentInfoHolder(uri));
				}
			}
			return true;
		}));

		int returnVal;

		if (input == null) {
			if (finalOldDoc == null) {
				returnVal = 0;
			} else {
				this.numBytesUsed -= this.byteCount(finalOldDoc);
				this.numDocs--;
				returnVal = data.put(uri, null).hashCode();
			}

			return returnVal;
		}

		byte[] contents = input.readAllBytes();

		Document newDoc = null;
		if (format == DocumentFormat.TXT) {
			newDoc = new DocumentImpl(uri, new String(contents, StandardCharsets.UTF_8));
		} else {
			newDoc = new DocumentImpl(uri, contents);
		}

		if (this.maxDocumentBytes == 0 || this.maxDocumentCount == 0) {
			Document returnDoc = this.data.put(uri, null);
			returnVal = returnDoc == null ? 0 : returnDoc.hashCode();
			(new DocumentPersistenceManager(this.dir)).serialize(uri, newDoc);
			return returnVal;
		}

		if (format == DocumentFormat.TXT) {
			if (finalOldDoc == null) {
				returnVal = 0;
				this.data.put(uri, newDoc);
				DocumentInfoHolder doc = new DocumentInfoHolder(newDoc);
				this.documentHolderData.add(doc);
				this.heap.insert(doc);
				if (newDoc != null) {
					newDoc.setLastUseTime(System.nanoTime());
					this.numDocs++;
					input.reset();
					this.numBytesUsed += input.available();
				}
			} else {
				for (String word : finalOldDoc.getWords()) {
					this.trie.delete(word, this.getDocumentInfoHolder(uri));
				}
				this.numBytesUsed -= this.byteCount(finalOldDoc);
				this.removeDocFromHeap(uri);
				this.documentHolderData.remove(this.getDocumentInfoHolder(uri));
				returnVal = this.data.put(uri, new DocumentImpl(uri, new String(contents, StandardCharsets.UTF_8))).hashCode();
				DocumentInfoHolder doc = new DocumentInfoHolder(newDoc);
				this.documentHolderData.add(doc);
				this.heap.insert(doc);
				input.reset();
				this.numBytesUsed += input.available();
			}

			if (data.get(uri) != null) {
				for (String word : data.get(uri).getWords()) {
					this.trie.put(word, this.getDocumentInfoHolder(uri));
				}
			}

			return returnVal;
		} else {
			if (finalOldDoc == null) {
				returnVal = 0;
				data.put(uri, newDoc);
				DocumentInfoHolder doc = new DocumentInfoHolder(newDoc);
				this.documentHolderData.add(doc);
				if (newDoc != null) {
					newDoc.setLastUseTime(System.nanoTime());
					this.numDocs++;
					input.reset();
					this.numBytesUsed += input.available();
				}
				this.heap.insert(doc);
			} else {
				this.numBytesUsed -= this.byteCount(finalOldDoc);
				this.removeDocFromHeap(uri);
				returnVal = this.data.put(uri, newDoc).hashCode();
				DocumentInfoHolder doc = new DocumentInfoHolder(newDoc);
				this.documentHolderData.add(doc);
				this.heap.insert(doc);
				input.reset();
				this.numBytesUsed += input.available();
			}

			if (this.data.get(uri) != null) {
				this.data.get(uri).setLastUseTime(System.nanoTime());
				this.heap.reHeapify(this.getDocumentInfoHolder(uri));
			}

			return returnVal;
		}
	}

	public Document getDocument(URI uri) {
		File location = new File(this.dir + File.separator + uri.getHost() + uri.getPath().substring(0, uri.getPath().lastIndexOf(File.separator) + 1));
        File diskData = new File(location, uri.getPath().substring(uri.getPath().lastIndexOf(File.separator) + 1) + ".json");
		if (diskData.exists()) {
			try {
				Document doc = this.data.get(uri);
				if (doc != null && this.maxDocumentBytes != 0 && this.maxDocumentCount != 0) {
					if (doc.getDocumentTxt() != null) {
						this.putDocumentWithoutUndo(new ByteArrayInputStream(doc.getDocumentTxt().getBytes()), uri, DocumentStore.DocumentFormat.TXT);
					} else {
						this.putDocumentWithoutUndo(new ByteArrayInputStream(doc.getDocumentBinaryData()), uri, DocumentStore.DocumentFormat.BINARY);
					}
				}
			} catch (IOException e) {}
		}
		if (this.data.get(uri) != null) {
			this.data.get(uri).setLastUseTime(System.nanoTime());
			if (this.maxDocumentCount != 0 && this.maxDocumentBytes != 0) {
				this.heap.reHeapify(this.getDocumentInfoHolder(uri));
			}
		}
		return this.data.get(uri);
	}

	public boolean deleteDocument(URI uri) {
		File location = new File(this.dir + File.separator + uri.getHost() + uri.getPath().substring(0, uri.getPath().lastIndexOf(File.separator) + 1));
        File diskData = new File(location, uri.getPath().substring(uri.getPath().lastIndexOf(File.separator) + 1) + ".json");
        if (diskData.exists()) {
        	Document currentDoc = null;
        	try {
	        	currentDoc = (new DocumentPersistenceManager(this.dir)).deserialize(uri);//deletes it and saves it to doc
	        } catch (IOException e) {}
        	DocumentInfoHolder doc = new DocumentInfoHolder(currentDoc);
			Document finalCurrentDoc = currentDoc;
			this.commandStack.push(new GenericCommand<URI>(uri, (URI uriVal) -> {
				try {
					if (finalCurrentDoc.getDocumentTxt() != null) {
						this.putDocument(new ByteArrayInputStream(finalCurrentDoc.getDocumentTxt().getBytes()), uriVal, DocumentStore.DocumentFormat.TXT);
					} else {
						this.putDocument(new ByteArrayInputStream(finalCurrentDoc.getDocumentBinaryData()), uriVal, DocumentStore.DocumentFormat.BINARY);
					}
				} catch (IOException e){}
				return true;}));
        	for (String word : doc.getWords()) {
				this.trie.delete(word, doc);
			}
			if (this.getDocumentInfoHolder(uri) != null) {
				this.removeDocFromHeap(uri);
			}
			this.documentHolderData.remove(doc);
        	return true;
        } else {
			Document currentDoc = this.data.get(uri);
			this.commandStack.push(new GenericCommand<URI>(uri, (URI uriVal) -> {
				this.data.put(uriVal, currentDoc);
				DocumentInfoHolder doc = new DocumentInfoHolder(currentDoc);
				if (this.data.get(uri) != null) {
					this.numDocs++;
					this.numBytesUsed += this.byteCount(this.data.get(uriVal));
					this.data.get(uri).setLastUseTime(System.nanoTime());
					this.documentHolderData.add(doc);
					this.heap.insert(doc);
				}
				if (data.get(uriVal) != null) {
					for (String word : data.get(uri).getWords()) {
						this.trie.put(word, this.getDocumentInfoHolder(uri));
					}
				}
				return true;}));

			if (data.get(uri) != null && data.get(uri).getDocumentTxt() != null) {
				for (String word : data.get(uri).getWords()) {
					this.trie.delete(word, this.getDocumentInfoHolder(uri));
				}
			}

			if (this.getDocument(uri) == null) {
				return false;
			}

			this.numDocs--;
			this.numBytesUsed -= this.byteCount(this.data.get(uri));
			this.removeDocFromHeap(uri);
			this.data.put(uri, null);
			this.documentHolderData.remove(this.getDocumentInfoHolder(uri));

			return true;
		}
	}

	public void undo() throws IllegalStateException {
		if (this.commandStack.size() == 0) {
			throw new IllegalStateException();
		}

		if (this.commandStack.peek() instanceof GenericCommand) {
			this.commandStack.pop().undo();
		} else {
			CommandSet commandSet = (CommandSet) this.commandStack.pop();
			commandSet.undoAll();
			Iterator itr = commandSet.iterator();
			long currentTime = System.nanoTime();
			while (itr.hasNext()) {
				if ((data.get((URI) ((GenericCommand) (itr.next())).getTarget())) != null){
					(data.get((URI) ((GenericCommand) (itr.next())).getTarget())).setLastUseTime(currentTime);
				}
			}
		}
	}

	public void undo(URI uri) throws IllegalStateException {
		if (uri == null) {
			throw new IllegalArgumentException();
		}

		if (this.commandStack.size() == 0) {
			throw new IllegalStateException();
		}

		if (this.commandStack.peek() instanceof GenericCommand) {
			if (((GenericCommand)this.commandStack.peek()).getTarget().equals(uri)) {
				this.commandStack.pop().undo();
				return;
			}
		} else {
			CommandSet cs = (CommandSet)this.commandStack.peek();
			if (cs.containsTarget(uri)) {
				cs.undo(uri);
				Iterator itr = cs.iterator();
				long currentTime = System.nanoTime();
				while (itr.hasNext()) {
					if ((data.get((URI) ((GenericCommand) (itr.next())).getTarget())) != null && (data.get((URI) ((GenericCommand) (itr.next())).getTarget())).getKey().equals(uri)){
						(data.get((URI) ((GenericCommand) (itr.next())).getTarget())).setLastUseTime(currentTime);
					}
				}
				if (cs.size() == 0) {
					this.commandStack.pop();
				}
				return;
			}
		}

		StackImpl<Undoable> helperStack = new StackImpl<Undoable>();
		helperStack.push(this.commandStack.pop());

		while (this.commandStack.peek() != null) {
			if (this.commandStack.peek() instanceof GenericCommand) {
				if (((GenericCommand)(this.commandStack.peek())).getTarget().equals(uri)) {
					this.commandStack.pop().undo();
					while (helperStack.peek() != null) {
						this.commandStack.push(helperStack.pop());
					}
					return;
				}
			} else {
				CommandSet cs = (CommandSet)this.commandStack.peek();
				if (cs.containsTarget(uri)) {
					(cs).undo(uri);
					if (cs.size() == 0) {
						this.commandStack.pop();
					}
				}	
				while (helperStack.peek() != null) {
					this.commandStack.push(helperStack.pop());
				}
				return;
			}

			helperStack.push(this.commandStack.pop());
		}

		while (helperStack.peek() != null) {
			this.commandStack.push(helperStack.pop());
		}

		throw new IllegalStateException();
	}

	public List<Document> search(String keyword) {
		List<DocumentInfoHolder> returnList = this.trie.getAllSorted(keyword.toLowerCase(), (DocumentInfoHolder doc1, DocumentInfoHolder doc2) -> {
			if (doc1.wordCount(keyword.toLowerCase()) > doc2.wordCount(keyword.toLowerCase())) {
				return -1;
			} else if (doc2.wordCount(keyword.toLowerCase()) > doc1.wordCount(keyword.toLowerCase())) {
				return 1;
			}
			return 0;
		});
		//System.out.println(returnList.size());
		/*long currentTime = System.nanoTime();
		for (DocumentInfoHolder doc : returnList) {
			if (doc != null) {
				doc.setLastUseTime(currentTime);
				this.heap.reHeapify(doc);
			}
		}*/
		List<Document> returnList2 = new ArrayList<Document>();
		for (DocumentInfoHolder doc : returnList) {
			//returnList2.add(doc.getDoc());
			returnList2.add(this.getDocument(doc.getKey()));
		}
		return returnList2;
	}

	public List<Document> searchByPrefix(String keywordPrefix) {
		List<DocumentInfoHolder> returnList = this.trie.getAllWithPrefixSorted(keywordPrefix.toLowerCase(), (DocumentInfoHolder doc1, DocumentInfoHolder doc2) -> {
			int prefixAppearancesDoc1 = 0;
			int prefixAppearancesDoc2 = 0;

			for (String word : doc1.getWords()) {
				if (word.toString().length() >= keywordPrefix.length() && word.substring(0, keywordPrefix.length()).equals(keywordPrefix)) {
					prefixAppearancesDoc1 += doc1.wordCount(word);
				}
			}

			for (String word : doc2.getWords()) {
				if (word.toString().length() >= keywordPrefix.length() && word.substring(0, keywordPrefix.length()).equals(keywordPrefix)) {
					prefixAppearancesDoc2 += doc2.wordCount(word);
				}
			}

			if (prefixAppearancesDoc1 > prefixAppearancesDoc2) {
				return -1;
			} else if (prefixAppearancesDoc2 < prefixAppearancesDoc1) {
				return 1;
			}
			return 0;
		});
		long currentTime = System.nanoTime();
		for (DocumentInfoHolder doc : returnList) {
			if (doc != null) {
				doc.setLastUseTime(currentTime);
				this.heap.reHeapify(doc);
			}
		}
		List<Document> returnList2 = new ArrayList<Document>();
		for (DocumentInfoHolder doc : returnList) {
			//returnList2.add(doc.getDoc());
			returnList2.add(this.getDocument(doc.getKey()));
		}
		return returnList2;
	}

	public Set<URI> deleteAll(String keyword) {
		CommandSet commandSet = new CommandSet();
		List<GenericCommand<URI>> commands = new ArrayList<GenericCommand<URI>>();
		long currentTime = System.nanoTime();
		for (DocumentInfoHolder doc : this.trie.getAllSorted(keyword, ((DocumentInfoHolder doc1, DocumentInfoHolder doc2) -> {
			if (doc1.wordCount(keyword.toLowerCase()) > doc2.wordCount(keyword.toLowerCase())) {
				return -1;
			} else if (doc2.wordCount(keyword.toLowerCase()) > doc1.wordCount(keyword.toLowerCase())) {
				return 1;
			}
			return 0;
		}))) {
			File location = new File(this.dir + File.separator + doc.getKey().getHost() + doc.getKey().getPath().substring(0, doc.getKey().getPath().lastIndexOf('/') + 1));
        	File diskData = new File(location, doc.getKey().getPath().substring(doc.getKey().getPath().lastIndexOf(File.separator) + 1) + ".json");
        	if (diskData.exists()) {
        		Document currentDoc = null;
        		try {
	        		currentDoc = (new DocumentPersistenceManager(this.dir)).deserialize(doc.getKey());
	        	} catch (IOException e){}
				Document finalDoc = currentDoc;
        		DocumentInfoHolder infoHolder = new DocumentInfoHolder(finalDoc);
				commands.add(new GenericCommand<URI>(doc.getKey(), (URI uriVal) -> {
					try {
						if (finalDoc.getDocumentTxt() != null) {
							this.putDocument(new ByteArrayInputStream(finalDoc.getDocumentTxt().getBytes()), uriVal, DocumentStore.DocumentFormat.TXT);
						} else {
							this.putDocument(new ByteArrayInputStream(finalDoc.getDocumentBinaryData()), uriVal, DocumentStore.DocumentFormat.BINARY);
						}
					} catch (IOException e){}

					if (data.get(uriVal) != null) {
						finalDoc.setLastUseTime(currentTime);
						this.heap.insert(infoHolder);
						for (String word : finalDoc.getWords()) {
							this.trie.put(word, infoHolder);
						}
					}
					return true;}));
        	} else {
				commands.add(new GenericCommand<URI>(doc.getKey(), (URI uriVal) -> {
					try {
						if (doc.getDoc().getDocumentTxt() != null) {
							this.putDocument(new ByteArrayInputStream(doc.getDoc().getDocumentTxt().getBytes()), uriVal, DocumentStore.DocumentFormat.TXT);
						} else {
							this.putDocument(new ByteArrayInputStream(doc.getDoc().getDocumentBinaryData()), uriVal, DocumentStore.DocumentFormat.BINARY);
						}
					} catch (IOException e){}

					if (data.get(uriVal) != null) {
						doc.setLastUseTime(currentTime);
						this.heap.insert(doc);
						for (String word : doc.getWords()) {
							this.trie.put(word, doc);
						}
					}
					return true;}));

				this.numDocs--;
				this.numBytesUsed -= this.byteCount(doc.getDoc());
				this.removeDocFromHeap(doc.getKey());
				this.data.put(doc.getKey(), null);
				this.documentHolderData.remove(doc);
			}
		}
		for (GenericCommand command : commands) {
			commandSet.addCommand(command);
		}
		this.commandStack.push(commandSet);

		Set<DocumentInfoHolder> docSet = this.trie.deleteAll(keyword);
		Set<URI> uriSet = new HashSet<URI>();
		for (DocumentInfoHolder doc : docSet) {
			uriSet.add(doc.getKey());
			this.data.put(doc.getKey(), null);
		}
		return uriSet;
	}

	public Set<URI> deleteAllWithPrefix(String keywordPrefix) {
		CommandSet commandSet = new CommandSet();
		List<GenericCommand> commands = new ArrayList<GenericCommand>();
		long currentTime = System.nanoTime();
		for (DocumentInfoHolder doc : this.trie.getAllWithPrefixSorted(keywordPrefix, (DocumentInfoHolder doc1, DocumentInfoHolder doc2) -> {
			int prefixAppearancesDoc1 = 0;
			int prefixAppearancesDoc2 = 0;

			for (String word : doc1.getWords()) {
				if (word.toString().length() >= keywordPrefix.length() && word.substring(0, keywordPrefix.length()).equals(keywordPrefix)) {
					prefixAppearancesDoc1 += doc1.wordCount(word);
				}
			}

			for (String word : doc2.getWords()) {
				if (word.toString().length() >= keywordPrefix.length() && word.substring(0, keywordPrefix.length()).equals(keywordPrefix)) {
					prefixAppearancesDoc2 += doc2.wordCount(word);
				}
			}

			if (prefixAppearancesDoc1 > prefixAppearancesDoc2) {
				return -1;
			} else if (prefixAppearancesDoc2 < prefixAppearancesDoc1) {
				return 1;
			}
			return 0;
		})) {
				File location = new File(this.dir + File.separator + doc.getKey().getHost() + doc.getKey().getPath().substring(0, doc.getKey().getPath().lastIndexOf('/') + 1));
	        	File diskData = new File(location, doc.getKey().getPath().substring(doc.getKey().getPath().lastIndexOf(File.separator) + 1) + ".json");
	        	if (diskData.exists()) {
	        		Document currentDoc = null;
	        		try {
		        		currentDoc = (new DocumentPersistenceManager(this.dir)).deserialize(doc.getKey());
		        	} catch (IOException e){}
					Document finalDoc = currentDoc;
	        		DocumentInfoHolder infoHolder = new DocumentInfoHolder(finalDoc);
					commands.add(new GenericCommand<URI>(doc.getKey(), (URI uriVal) -> {
						try {
							if (finalDoc.getDocumentTxt() != null) {
								this.putDocument(new ByteArrayInputStream(finalDoc.getDocumentTxt().getBytes()), uriVal, DocumentStore.DocumentFormat.TXT);
							} else {
								this.putDocument(new ByteArrayInputStream(finalDoc.getDocumentBinaryData()), uriVal, DocumentStore.DocumentFormat.BINARY);
							}
						} catch (IOException e){}

						if (data.get(uriVal) != null) {
							finalDoc.setLastUseTime(currentTime);
							this.heap.insert(infoHolder);
							for (String word : finalDoc.getWords()) {
								this.trie.put(word, infoHolder);
							}
						}
						return true;}));
	        	} else {
					commands.add(new GenericCommand<URI>(doc.getKey(), (URI uriVal) -> {
						try {
							if (doc.getDoc().getDocumentTxt() != null) {
								this.putDocument(new ByteArrayInputStream(doc.getDoc().getDocumentTxt().getBytes()), uriVal, DocumentStore.DocumentFormat.TXT);
							} else {
								this.putDocument(new ByteArrayInputStream(doc.getDoc().getDocumentBinaryData()), uriVal, DocumentStore.DocumentFormat.BINARY);
							}
						} catch (IOException e){}
						
						if (data.get(uriVal) != null) {
							doc.setLastUseTime(currentTime);
							this.heap.insert(doc);
							for (String word : doc.getWords()) {
								this.trie.put(word, doc);
							}
						}
						return true;}));
					
				this.numDocs--;
				this.numBytesUsed -= this.byteCount(doc.getDoc());
				this.removeDocFromHeap(doc.getKey());
				this.data.put(doc.getKey(), null);
				this.documentHolderData.remove(doc);
			}		
		}
		for (GenericCommand command : commands) {
			commandSet.addCommand(command);
		}
		this.commandStack.push(commandSet);

		Set<DocumentInfoHolder> docSet = this.trie.deleteAllWithPrefix(keywordPrefix);
		Set<URI> uriSet = new HashSet<URI>();
		for (DocumentInfoHolder currentDoc : docSet) {
			uriSet.add(currentDoc.getKey());
			this.data.put(currentDoc.getKey(), null);
		}
		return uriSet;
	}

	public void setMaxDocumentCount(int limit) {
		if (limit < 0) {
			throw new IllegalArgumentException();
		}

		this.maxDocumentCount = limit;

		while (this.numDocs > this.maxDocumentCount) {
			try {
				this.completelyRemoveDoc();
			} catch (IOException e){}
		}
	}

	public void setMaxDocumentBytes(int limit) {
		if (limit < 0) {
			throw new IllegalArgumentException();
		}

		this.maxDocumentBytes = limit;

		while (this.numBytesUsed > this.maxDocumentBytes) {
			try {
				this.completelyRemoveDoc();
			} catch (IOException e){}
		}
	}
}
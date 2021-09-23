import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;
import edu.yu.cs.com1320.project.impl.*;
import edu.yu.cs.com1320.project.*;
import edu.yu.cs.com1320.project.stage5.impl.*;
import edu.yu.cs.com1320.project.stage5.*;
import edu.yu.cs.com1320.project.stage5.DocumentStore.DocumentFormat;
import java.net.*;
import java.io.*;
import java.util.*;

public class Tests {
	//variables to hold possible values for doc1
	private URI uri1;
	private String txt1;
    
	//variables to hold possible values for doc2
	private URI uri2;
	String txt2;

	private URI uri3;
	String txt3;

	private URI uri4;
	private URI uri5;
	private URI uri6;
    
	@BeforeEach
	public void init() throws Exception {
		//init possible values for doc1
		this.uri1 = new URI("http://edu.yu.cs/com1320/project/doc1");
		this.txt1 = "Apple Apple Pizza Fish Pie Pizza Apple";
    
		//init possible values for doc2
		this.uri2 = new URI("http://edu.yu.cs/com1320/project/doc2");
		this.txt2 = "Pizza Pizza Pizza Pizza Pizza";

		//init possible values for doc3
		this.uri3 = new URI("http://edu.yu.cs/com1320/project/doc3");
		this.txt3 = "Penguin Park Piccalo Pants Pain Possum";

		this.uri4 = new URI("http://edu.yu.cs/com1320/project/doc4");
		this.uri5 = new URI("http://edu.yu.cs/com1320/project/doc5");
		this.uri6 = new URI("http://edu.yu.cs/com1320/project/doc6");


		for (String word : string1.split(" ")) {
			trie.put(word, string1.indexOf(word));
		}
		for (String word : string2.split(" ")) {
			trie.put(word, string2.indexOf(word));
		}
		for (String word : string3.split(" ")) {
			trie.put(word, string3.indexOf(word));
		}
		for (String word : string4.split(" ")) {
			trie.put(word, string4.indexOf(word));
		}
	}

	@Test
	public void tryingToIsolateHeapProblem() throws IOException {
		DocumentStoreImpl store = new DocumentStoreImpl();
		store.putDocument(new ByteArrayInputStream(this.txt1.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt2.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
		store.undo();//back to txt1
		store.putDocument(new ByteArrayInputStream(this.txt3.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
		store.undo(this.uri1);//back to txt1
		store.getDocument(this.uri1);//<--NoSuchElementException. Fixed!
	}

	/*@Test
	public void seeWhereFilesAreGoing() throws IOException {
		DocumentStoreImpl store = new DocumentStoreImpl();
		store.putDocument(new ByteArrayInputStream(this.txt1.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
		assertNotNull(store.data.get(this.uri1));
		store.putDocument(new ByteArrayInputStream(this.txt2.getBytes()),this.uri2, DocumentStore.DocumentFormat.TXT);
		assertNotNull(store.data.get(this.uri1));
		store.setMaxDocumentCount(1);
		assertNull(store.data.get(this.uri1));
		store.getDocument(this.uri1);
		assertNotNull(store.data.get(this.uri1));//<-- shouldn't actually be null - BTree.get brings it back
		assertNull(store.data.get(this.uri2));
		//System.out.println(store.data.get(this.uri1).getDocumentTxt());
	}*/

	/*@Test
	public void moreComplexTest() throws IOException {
		DocumentStoreImpl store = new DocumentStoreImpl();
        store.putDocument(new ByteArrayInputStream(this.txt1.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
        store.putDocument(new ByteArrayInputStream(this.txt2.getBytes()),this.uri2, DocumentStore.DocumentFormat.TXT);
        store.putDocument(new ByteArrayInputStream(this.txt3.getBytes()),this.uri3, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(new byte[] {0, 0, 0}), this.uri4, DocumentStore.DocumentFormat.BINARY);
		store.putDocument(new ByteArrayInputStream(new byte[] {1, 0, 1}), this.uri5, DocumentStore.DocumentFormat.BINARY);
		store.putDocument(new ByteArrayInputStream(new byte[] {1, 1, 1}), this.uri6, DocumentStore.DocumentFormat.BINARY);
		//114 bytes used

		store.deleteDocument(this.uri4);
		store.getDocument(this.uri2);//12356 -> 13562
		store.search("possum");//13562 -> 15623
		store.undo(this.uri4);//15623 -> 156234

		store.setMaxDocumentBytes(73);//should kick out 1 and 5
		assertNull(store.getDocument(this.uri1));
		assertNull(store.getDocument(this.uri5));
		assertNotNull(store.getDocument(this.uri6));
		assertNotNull(store.getDocument(this.uri2));
		assertNotNull(store.getDocument(this.uri3));
		assertNotNull(store.getDocument(this.uri4));//maintain order with getDocuments

		store.setMaxDocumentCount(2);
		assertNull(store.getDocument(this.uri6));
		assertNull(store.getDocument(this.uri2));
		assertNotNull(store.getDocument(this.uri3));
		assertNotNull(store.getDocument(this.uri4));
	}*/

	@Test
	public void diskOverrideTest() throws IOException {
		DocumentStoreImpl store = new DocumentStoreImpl();
		store.setMaxDocumentCount(1);
		store.putDocument(new ByteArrayInputStream(this.txt1.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt2.getBytes()),this.uri2, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt2.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
		store.deleteDocument(this.uri2);
	}

	@Test
	public void memorySearchTest() throws IOException {
		DocumentStoreImpl store = new DocumentStoreImpl();
		store.setMaxDocumentCount(2);
		store.putDocument(new ByteArrayInputStream(this.txt1.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt2.getBytes()),this.uri2, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt3.getBytes()),this.uri3, DocumentStore.DocumentFormat.TXT);
		//1 is on disk
		store.search("pizza");
		//3 should be on disk - good.
		store.deleteDocument(this.uri3);
	}

	@Test
	public void undoMemoryTest() throws IOException {
		DocumentStoreImpl store = new DocumentStoreImpl();
		store.setMaxDocumentCount(1);
		store.putDocument(new ByteArrayInputStream(this.txt1.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt2.getBytes()),this.uri2, DocumentStore.DocumentFormat.TXT);
		store.undo();//<-- doc1 should come back into memory. done.
		//also trie issue may somehow be related to fact that search() calls reHeapify()
	}

	@Test
	public void quickTest() throws IOException {
		DocumentStoreImpl store = new DocumentStoreImpl();
		store.setMaxDocumentCount(1);
		store.putDocument(new ByteArrayInputStream(this.txt1.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt2.getBytes()),this.uri2, DocumentStore.DocumentFormat.TXT);
		store.getDocument(this.uri1);
		store.undo();//should delete doc2
		System.out.println();
	}

	@Test
	public void storeInDiffFolder() throws IOException, URISyntaxException {
		DocumentStoreImpl store = new DocumentStoreImpl(new File("C:\\Users\\17324\\OneDrive\\Documents\\Java\\Crystal_Ephraim_800395171\\DataStructures"));
		this.uri1 = new URI("http://newFolder/doc1");
		store.putDocument(new ByteArrayInputStream(this.txt1.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt2.getBytes()),this.uri2, DocumentStore.DocumentFormat.TXT);
		store.setMaxDocumentCount(1);
		store.deleteDocument(this.uri1);
	}

	@Test
	public void testBinary() throws IOException {
		DocumentStoreImpl store = new DocumentStoreImpl();
		store.putDocument(new ByteArrayInputStream(new byte[]{0, 1, 1, 0}), this.uri1, DocumentFormat.BINARY);
		store.setMaxDocumentBytes(4);
		store.putDocument(new ByteArrayInputStream(new byte[]{0, 1, 0, 1}), this.uri2, DocumentFormat.BINARY);
		System.out.println(store.getDocument(this.uri1).getDocumentBinaryData()[2]);//should output 1, doc2 should be on disk
		store.deleteDocument(this.uri2);
	}

	@Test
	public void settingMaxes() throws IOException {
		//edited BTree.get
		DocumentStoreImpl store = new DocumentStoreImpl();
		/*store.setMaxDocumentCount(0);
		store.putDocument(new ByteArrayInputStream(this.txt1.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);*/

		/*store.putDocument(new ByteArrayInputStream(this.txt1.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
		store.setMaxDocumentCount(0);
		System.out.println(store.getDocument(this.uri1).getDocumentTxt());*/

		/*store.setMaxDocumentCount(1);
		store.putDocument(new ByteArrayInputStream(this.txt1.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt2.getBytes()),this.uri2, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt3.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);*/

		store.setMaxDocumentCount(1);
		store.putDocument(new ByteArrayInputStream(this.txt1.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt2.getBytes()),this.uri2, DocumentStore.DocumentFormat.TXT);
		store.getDocument(this.uri1);
		store.getDocument(this.uri2);
		store.deleteDocument(this.uri1);
		store.undo();
		store.deleteDocument(this.uri2);


		//store.putDocument(new ByteArrayInputStream(this.txt1.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
		//store.setMaxDocumentCount(0);
		//System.out.println(store.getDocument(this.uri1).getDocumentTxt());
		//doc1 should be on disk


		//store.putDocument(new ByteArrayInputStream(this.txt2.getBytes()),this.uri2, DocumentStore.DocumentFormat.TXT);
		//store.putDocument(new ByteArrayInputStream(this.txt3.getBytes()),this.uri3, DocumentStore.DocumentFormat.TXT);
		//System.out.println(store.getDocument(this.uri1));
		//assertNotNull(store.getDocument(this.uri1));
		/*assertNotNull(store.getDocument(this.uri2));
		assertNotNull(store.getDocument(this.uri3));

		store = new DocumentStoreImpl();
		store.setMaxDocumentBytes(1);
		store.putDocument(new ByteArrayInputStream(new byte[] {0}), this.uri1, DocumentStore.DocumentFormat.BINARY);
		store.putDocument(new ByteArrayInputStream(new byte[] {1}), this.uri2, DocumentStore.DocumentFormat.BINARY);
		assertNull(store.getDocument(this.uri1));
		assertNotNull(store.getDocument(this.uri2));

		store = new DocumentStoreImpl();
		store.putDocument(new ByteArrayInputStream(this.txt1.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt2.getBytes()),this.uri2, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt3.getBytes()),this.uri3, DocumentStore.DocumentFormat.TXT);
		store.setMaxDocumentCount(2);
		assertNull(store.getDocument(this.uri1));
		assertNotNull(store.getDocument(this.uri2));
		assertNotNull(store.getDocument(this.uri3));

		store = new DocumentStoreImpl();
		store.putDocument(new ByteArrayInputStream(new byte[] {0}), this.uri1, DocumentStore.DocumentFormat.BINARY);
		store.putDocument(new ByteArrayInputStream(new byte[] {1}), this.uri2, DocumentStore.DocumentFormat.BINARY);
		store.setMaxDocumentBytes(1);
		assertNull(store.getDocument(this.uri1));
		assertNotNull(store.getDocument(this.uri2));

		store = new DocumentStoreImpl();
		store.setMaxDocumentBytes(50);
		store.putDocument(new ByteArrayInputStream(this.txt3.getBytes()),this.uri3, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt1.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt2.getBytes()),this.uri2, DocumentStore.DocumentFormat.TXT);
		assertNull(store.getDocument(this.uri3));
		assertNull(store.getDocument(this.uri1));
		assertNotNull(store.getDocument(this.uri2));

		store = new DocumentStoreImpl();
		store.putDocument(new ByteArrayInputStream(this.txt3.getBytes()),this.uri3, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt1.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt2.getBytes()),this.uri2, DocumentStore.DocumentFormat.TXT);
		store.search("pain");//doc3
		store.setMaxDocumentBytes(50);
		assertNull(store.getDocument(this.uri1));
		assertNull(store.getDocument(this.uri2));
		assertNotNull(store.getDocument(this.uri3));

		store = new DocumentStoreImpl();
		store.putDocument(new ByteArrayInputStream(this.txt1.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt2.getBytes()),this.uri2, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt3.getBytes()),this.uri3, DocumentStore.DocumentFormat.TXT);
		store.getDocument(this.uri1);
		store.setMaxDocumentCount(1);
		assertNotNull(store.getDocument(this.uri1));
		assertNull(store.getDocument(this.uri2));
		assertNull(store.getDocument(this.uri3));*/
	}
/*@Test //Need DocumentStoreImpl's HashTable to be public
	public void settingMaxes() throws IOException {
		DocumentStoreImpl store = new DocumentStoreImpl();
		store.setMaxDocumentCount(2);
		store.putDocument(new ByteArrayInputStream(this.txt1.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt2.getBytes()),this.uri2, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt3.getBytes()),this.uri3, DocumentStore.DocumentFormat.TXT);
		assertNull(store.data.get(this.uri1));
		assertNotNull(store.data.get(this.uri2));
		assertNotNull(store.data.get(this.uri3));

		store = new DocumentStoreImpl();
		store.setMaxDocumentBytes(1);
		store.putDocument(new ByteArrayInputStream(new byte[] {0}), this.uri1, DocumentStore.DocumentFormat.BINARY);
		store.putDocument(new ByteArrayInputStream(new byte[] {1}), this.uri2, DocumentStore.DocumentFormat.BINARY);
		assertNull(store.data.get(this.uri1));
		assertNotNull(store.data.get(this.uri2));

		store = new DocumentStoreImpl();
		store.putDocument(new ByteArrayInputStream(this.txt1.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt2.getBytes()),this.uri2, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt3.getBytes()),this.uri3, DocumentStore.DocumentFormat.TXT);
		store.setMaxDocumentCount(2);
		assertNull(store.data.get(this.uri1));
		assertNotNull(store.data.get(this.uri2));
		assertNotNull(store.data.get(this.uri3));

		store = new DocumentStoreImpl();
		store.putDocument(new ByteArrayInputStream(new byte[] {0}), this.uri1, DocumentStore.DocumentFormat.BINARY);
		store.putDocument(new ByteArrayInputStream(new byte[] {1}), this.uri2, DocumentStore.DocumentFormat.BINARY);
		store.setMaxDocumentBytes(1);
		assertNull(store.data.get(this.uri1));
		assertNotNull(store.data.get(this.uri2));

		store = new DocumentStoreImpl();
		store.setMaxDocumentBytes(50);
		store.putDocument(new ByteArrayInputStream(this.txt3.getBytes()),this.uri3, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt1.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt2.getBytes()),this.uri2, DocumentStore.DocumentFormat.TXT);
		assertNull(store.data.get(this.uri3));
		assertNull(store.data.get(this.uri1));
		assertNotNull(store.data.get(this.uri2));

		store = new DocumentStoreImpl();
		store.putDocument(new ByteArrayInputStream(this.txt3.getBytes()),this.uri3, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt1.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt2.getBytes()),this.uri2, DocumentStore.DocumentFormat.TXT);
		store.search("pain");//doc3
		store.setMaxDocumentBytes(50);
		assertNull(store.data.get(this.uri1));
		assertNull(store.data.get(this.uri2));
		assertNotNull(store.data.get(this.uri3));

		store = new DocumentStoreImpl();
		store.putDocument(new ByteArrayInputStream(this.txt1.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt2.getBytes()),this.uri2, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt3.getBytes()),this.uri3, DocumentStore.DocumentFormat.TXT);
		store.getDocument(this.uri1);
		store.setMaxDocumentCount(1);
		assertNotNull(store.data.get(this.uri1));
		assertNull(store.data.get(this.uri2));
		assertNull(store.data.get(this.uri3));
	}

	@Test  //Need DocumentStoreImpl's HashTable to be public
	public void testDocumentUsageTimes() throws URISyntaxException, IOException {
		DocumentStoreImpl store = new DocumentStoreImpl();
		store.putDocument(new ByteArrayInputStream(this.txt1.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
		long t1 = store.data.get(this.uri1).getLastUseTime();
		long t2 = store.getDocument(this.uri1).getLastUseTime();
		assertTrue(t2 > t1);
		store.search("apple");
		long t3 = store.data.get(this.uri1).getLastUseTime();
		assertTrue(t3 > t2);
		store.putDocument(new ByteArrayInputStream(this.txt2.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
		long t4 = store.data.get(this.uri1).getLastUseTime();
		assertTrue(t4 > t3);
		store.undo();
		long t5 = store.data.get(this.uri1).getLastUseTime();
		assertTrue(t5 > t3);
		store.putDocument(new ByteArrayInputStream(this.txt3.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
		long t6 = store.data.get(this.uri1).getLastUseTime();
		assertTrue(t6 > t5);
		store.undo(this.uri1);
		long t7 = store.data.get(this.uri1).getLastUseTime();
		assertTrue(t7 > t6);
		store.putDocument(new ByteArrayInputStream(this.txt2.getBytes()),this.uri2, DocumentStore.DocumentFormat.TXT);
		long t8 = store.data.get(this.uri1).getLastUseTime();
		assertTrue(t8 == t7);
	}

	@Test  //Need DocumentStoreImpl's HashTable and heap to be public, and getArrayIndex
	public void testHeapOrdering() throws IOException {
		DocumentStoreImpl store = new DocumentStoreImpl();
		store.putDocument(new ByteArrayInputStream(this.txt1.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
		assertTrue(store.heap.remove().equals(store.data.get(this.uri1)));

		store = new DocumentStoreImpl();
		store.putDocument(new ByteArrayInputStream(this.txt1.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt2.getBytes()),this.uri2, DocumentStore.DocumentFormat.TXT);
		//System.out.println("Doc1LU: " + store.data.get(this.uri1).getLastUseTime());
		//System.out.println("Doc2LU: " + store.data.get(this.uri2).getLastUseTime());
		//System.out.println("Doc1 vs Doc2: " + store.data.get(this.uri1).compareTo(store.data.get(uri2)));
		//System.out.println("Doc1: " + store.data.get(uri1));
		//System.out.println("Doc2: " + store.data.get(uri2));
		//System.out.println(store.heap.getArrayIndex(store.data.get(this.uri1)));
		//System.out.println(store.heap.getArrayIndex(store.data.get(this.uri2)));
		assertTrue(store.heap.remove().equals(store.data.get(this.uri1)));
		assertTrue(store.heap.remove().equals(store.data.get(this.uri2)));

		store = new DocumentStoreImpl();
		store.putDocument(new ByteArrayInputStream(this.txt1.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt2.getBytes()),this.uri2, DocumentStore.DocumentFormat.TXT);
		store.getDocument(this.uri1);

		assertTrue(store.heap.remove().equals(store.data.get(this.uri2)));
		assertTrue(store.heap.remove().equals(store.data.get(this.uri1)));

		store = new DocumentStoreImpl();
		store.putDocument(new ByteArrayInputStream(this.txt1.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt2.getBytes()),this.uri2, DocumentStore.DocumentFormat.TXT);
		store.search("apple");

		assertTrue(store.heap.remove().equals(store.data.get(this.uri2)));
		assertTrue(store.heap.remove().equals(store.data.get(this.uri1)));
	}*/

	@Test
	public void basicMinHeap() {
		MinHeapImpl<Integer> heap = new MinHeapImpl<Integer>();
		heap.insert(3);
		heap.insert(2);
		heap.insert(6);
		heap.insert(1);
		heap.insert(4);
		heap.insert(5);
		heap.insert(7);

		//System.out.println(heap.getArrayIndex(1));

		for (int i = 1; i < 7; i++) {
			assertEquals(i, heap.remove());
		}
	}


	//Tests from stages 1-3

	@Test
	public void wordCountAndGetWordsTest() throws URISyntaxException {
		DocumentImpl txtDoc = new DocumentImpl(new URI("placeholder"), " The!se ARE? sOme   W@o%$rds with^ s**ymbols (m)ixed [in]. Hope    this test test passes!");
		assertEquals(0, txtDoc.wordCount("bundle"));
		assertEquals(1, txtDoc.wordCount("these"));
		assertEquals(1, txtDoc.wordCount("WORDS"));
		assertEquals(1, txtDoc.wordCount("S-Y-M-B-O-??-LS"));
		assertEquals(1, txtDoc.wordCount("p@A$$sse$s"));
		assertEquals(2, txtDoc.wordCount("tEst"));
		Set<String> words = txtDoc.getWords();
		assertEquals(12, words.size());
		assertTrue(words.contains("some"));

		DocumentImpl binaryDoc = new DocumentImpl(new URI("0110"), new byte[] {0,1,1,0});
		assertEquals(0, binaryDoc.wordCount("anythingYouPutHereShouldBeZero"));
		Set<String> words2 = binaryDoc.getWords();
		assertEquals(0, words2.size());
	}
	@Test
	public void simpleTrieTest() {
		Trie trie = new TrieImpl<Integer>();
		trie.put("APPLE123", 1);
		trie.put("APPLE123", 2);
		trie.put("APPLE123", 3);
		trie.put("WORD87", 8);
		trie.put("WORD87", 7);

		List<Integer> apple123List = trie.getAllSorted("apple123", (int1, int2) -> {
			if ((int) int1 < (int) int2) {
				return -1;
			} else if ((int) int2 < (int) int1) {
				return 1;
			}
			return 0;
		});//this comparator will order integers from lowest to highest
		List<Integer> word87List = trie.getAllSorted("word87", (int1, int2) -> {
			if ((int) int1 < (int) int2) {
				return -1;
			} else if ((int) int2 < (int) int1) {
				return 1;
			}
			return 0;
		});

		assertEquals(3, apple123List.size());
		assertEquals(2, word87List.size());
		assertEquals(1, apple123List.get(0));
		assertEquals(2, apple123List.get(1));
		assertEquals(3, apple123List.get(2));
		assertEquals(7, word87List.get(0));
		assertEquals(8, word87List.get(1));

		trie.put("app", 12);
		trie.put("app", 5);
		trie.put("ap", 4);

		List<Integer> apList = trie.getAllWithPrefixSorted("AP", (int1, int2) -> {
			if ((int) int1 < (int) int2) {
				return -1;
			} else if ((int) int2 < (int) int1) {
				return 1;
			}
			return 0;
		});
		List<Integer> appList = trie.getAllWithPrefixSorted("APP", (int1, int2) -> {
			if ((int) int1 < (int) int2) {
				return -1;
			} else if ((int) int2 < (int) int1) {
				return 1;
			}
			return 0;
		});

		assertEquals(6, apList.size());
		assertEquals(5, appList.size());
		assertEquals(12, apList.get(5));
		assertEquals(12, appList.get(4));

		Set<Integer> deletedAppPrefix = trie.deleteAllWithPrefix("aPp");
		assertEquals(5, deletedAppPrefix.size());
		assertTrue(deletedAppPrefix.contains(3));
		assertTrue(deletedAppPrefix.contains(5));

		apList = trie.getAllWithPrefixSorted("AP", (int1, int2) -> {
			if ((int) int1 < (int) int2) {
				return -1;
			} else if ((int) int2 < (int) int1) {
				return 1;
			}
			return 0;
		});
		appList = trie.getAllWithPrefixSorted("APP", (int1, int2) -> {
			if ((int) int1 < (int) int2) {
				return -1;
			} else if ((int) int2 < (int) int1) {
				return 1;
			}
			return 0;
		});

		assertEquals(1, apList.size());
		assertEquals(0, appList.size());

		trie.put("deleteAll", 100);
		trie.put("deleteAll", 200);
		trie.put("deleteAll", 300);

		List<Integer> deleteList = trie.getAllSorted("DELETEALL", (int1, int2) -> {
			if ((int) int1 < (int) int2) {
				return -1;
			} else if ((int) int2 < (int) int1) {
				return 1;
			}
			return 0;
		});

		assertEquals(3, deleteList.size());
		Set<Integer> thingsActuallyDeleted = trie.deleteAll("DELETEall");
		assertEquals(3, thingsActuallyDeleted.size());
		assertTrue(thingsActuallyDeleted.contains(100));

		deleteList = trie.getAllSorted("DELETEALL", (int1, int2) -> {
			if ((int) int1 < (int) int2) {
				return -1;
			} else if ((int) int2 < (int) int1) {
				return 1;
			}
			return 0;
		});

		assertEquals(0, deleteList.size());

		trie.put("deleteSome", 100);
		trie.put("deleteSome", 200);
		trie.put("deleteSome", 300);

		List<Integer> deleteList2 = trie.getAllSorted("DELETESOME", (int1, int2) -> {
			if ((int) int1 < (int) int2) {
				return -1;
			} else if ((int) int2 < (int) int1) {
				return 1;
			}
			return 0;
		});

		assertEquals(3, deleteList2.size());
		Integer twoHundred = (Integer) trie.delete("deleteSome", 200);
		Integer nullInt = (Integer) trie.delete("deleteSome", 500);

		assertEquals(200, twoHundred);
		assertNull(nullInt);

		deleteList2 = trie.getAllSorted("DELETESOME", (int1, int2) -> {
			if ((int) int1 < (int) int2) {
				return -1;
			} else if ((int) int2 < (int) int1) {
				return 1;
			}
			return 0;
		});

		assertEquals(2, deleteList2.size());
		assertFalse(deleteList2.contains(200));
	}
	@Test
	public void basicSearchAndOrganizationTest() throws IOException {
		DocumentStore store = new DocumentStoreImpl();
		store.putDocument(new ByteArrayInputStream(this.txt1.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt2.getBytes()),this.uri2, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt3.getBytes()),this.uri3, DocumentStore.DocumentFormat.TXT);
		assertEquals(1, store.search("PiE").size());
		assertEquals(3, store.searchByPrefix("p").size());
		assertEquals(0, store.searchByPrefix("x").size());
		assertEquals(3, store.searchByPrefix("pi").size());
		assertEquals(5, store.search("PiZzA").get(0).wordCount("pizza"));
		assertEquals(6, store.searchByPrefix("p").get(0).getWords().size());
	}
	@Test
	public void basicSearchDeleteTest() throws IOException {
		DocumentStore store = new DocumentStoreImpl();
		store.putDocument(new ByteArrayInputStream(this.txt1.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt2.getBytes()),this.uri2, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt3.getBytes()),this.uri3, DocumentStore.DocumentFormat.TXT);
		assertEquals(1, store.search("PiE").size());
		assertEquals(3, store.searchByPrefix("p").size());
		assertEquals(1, store.search("possum").size());
		store.deleteDocument(this.uri3);
		DocumentImpl doc1 = new DocumentImpl(this.uri1, this.txt1);
		DocumentImpl doc2 = new DocumentImpl(this.uri2, this.txt2);
		DocumentImpl doc3 = new DocumentImpl(this.uri3, this.txt3);
		for (char c = 'a'; c<='z'; c++) {
			List<Document> list = store.searchByPrefix(Character.toString(c));
			if (list.size()!=0) {
				assertNotEquals(doc3, list.get(0));
				if ((!list.get(0).equals(doc1))&&(!list.get(0).equals(doc2))) {
					fail();
				}
			}
		}
		for (char c = '0'; c<='9'; c++) {
			List<Document> list = store.searchByPrefix(Character.toString(c));
			if (list.size()!=0) {
				assertNotEquals(doc3, list.get(0));
				if ((!list.get(0).equals(doc1))&&(!list.get(0).equals(doc2))) {
					fail();
				}
			}
		}
		assertEquals(0, store.search("possum").size());
		assertEquals(2, store.search("pizza").size());
		store.deleteDocument(this.uri2);
		assertEquals(1, store.search("pizza").size());
	}
	@Test
	public void basicPutOverwriteTest() throws IOException {
		DocumentStore store = new DocumentStoreImpl();
		store.putDocument(new ByteArrayInputStream(this.txt1.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt2.getBytes()),this.uri2, DocumentStore.DocumentFormat.TXT);
		assertEquals(2, store.search("pizza").size());
		store.putDocument(new ByteArrayInputStream(this.txt3.getBytes()),this.uri2, DocumentStore.DocumentFormat.TXT);
		assertEquals(1, store.search("pizza").size());
	}
	@Test
	public void testDeleteAndDeleteAll() throws IOException {
		DocumentStore store = new DocumentStoreImpl();
		store.putDocument(new ByteArrayInputStream(this.txt1.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt2.getBytes()),this.uri2, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt3.getBytes()),this.uri3, DocumentStore.DocumentFormat.TXT);
		assertEquals(2, store.search("pizza").size());
		store.deleteAll("PiZZa");
		assertEquals(0, store.search("pizza").size());
		assertNull(store.getDocument(this.uri1));
		store.putDocument(new ByteArrayInputStream(this.txt2.getBytes()),this.uri2, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt1.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
		assertEquals(2, store.search("pizza").size());
		assertNotNull(store.getDocument(this.uri1));assertNotNull(store.getDocument(this.uri2));assertNotNull(store.getDocument(this.uri3));
		store.deleteAllWithPrefix("p");
		assertNull(store.getDocument(this.uri1));assertNull(store.getDocument(this.uri2));assertNull(store.getDocument(this.uri3));
	}
	@Test
    public void testUndoNoArgs() throws IOException {
        DocumentStore store = new DocumentStoreImpl();
        store.putDocument(new ByteArrayInputStream(this.txt1.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
        store.putDocument(new ByteArrayInputStream(this.txt2.getBytes()),this.uri2, DocumentStore.DocumentFormat.TXT);
        store.putDocument(new ByteArrayInputStream(this.txt3.getBytes()),this.uri3, DocumentStore.DocumentFormat.TXT);
        store.undo();
        assertEquals(null, store.getDocument(this.uri3));
        assertEquals(0, store.search("penguin").size());
        store.putDocument(new ByteArrayInputStream(this.txt3.getBytes()),this.uri3, DocumentStore.DocumentFormat.TXT);
        store.deleteAll("pizza");
        assertEquals(0, store.search("pizza").size());
        assertNull(store.getDocument(this.uri1));
        store.undo();
        assertEquals(2, store.search("pizza").size());

		assertNotNull(store.getDocument(this.uri1));
    }
    @Test
    public void testUndoWithArgs() throws IOException {
        DocumentStore store = new DocumentStoreImpl();
        store.putDocument(new ByteArrayInputStream(this.txt1.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
        store.putDocument(new ByteArrayInputStream(this.txt2.getBytes()),this.uri2, DocumentStore.DocumentFormat.TXT);
        store.putDocument(new ByteArrayInputStream(this.txt3.getBytes()),this.uri3, DocumentStore.DocumentFormat.TXT);
        assertEquals(1, store.search("apple").size());
        assertEquals(1, store.searchByPrefix("a").size());
        store.undo(this.uri1);
        assertEquals(0, store.search("apple").size());
        assertEquals(0, store.searchByPrefix("a").size());
    }
    @Test
    public void testUndoCommandSet() throws IOException {
        DocumentStore store = new DocumentStoreImpl();
        store.putDocument(new ByteArrayInputStream(this.txt1.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
        store.putDocument(new ByteArrayInputStream(this.txt2.getBytes()),this.uri2, DocumentStore.DocumentFormat.TXT);
        //store.deleteAll("pizza");
        assertEquals(2, store.deleteAll("pizza").size());
        store.putDocument(new ByteArrayInputStream(this.txt3.getBytes()),this.uri3, DocumentStore.DocumentFormat.TXT);
        assertNotNull(store.getDocument(this.uri3));
        assertEquals(0, store.search("pizza").size());
        store.undo(uri1);
        assertEquals(1, store.search("pizza").size());
        assertEquals(4, store.search("pizza").get(0).getWords().size());
        store.undo(uri2);
        assertEquals(2, store.search("pizza").size());
        assertEquals(1, store.search("pizza").get(0).getWords().size());
        store.undo();
        assertNull(store.getDocument(this.uri3));
        assertEquals(0, store.search("penguin").size());
    }
    @Test
    public void testUndoCommandSet2() throws IOException {
        DocumentStore store = new DocumentStoreImpl();
        store.putDocument(new ByteArrayInputStream(this.txt1.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
        store.putDocument(new ByteArrayInputStream(this.txt2.getBytes()),this.uri2, DocumentStore.DocumentFormat.TXT);
        store.deleteAll("pizza");
        assertEquals(0, store.search("pizza").size());
        store.undo(uri2);
        assertEquals(1, store.search("pizza").size());
        store.undo(uri2);
        assertEquals(0, store.search("pizza").size());
        boolean test = false;
        try {
            store.undo(uri2);
        } catch (IllegalStateException e) {
            test = true;
        }
        assertTrue(test);
        assertEquals(0, store.search("pizza").size());
        store.undo(uri1);
        assertEquals(1, store.searchByPrefix("app").size());
        assertEquals(1, store.search("pizza").size());
    }
    @Test
    public void removeCommandSet() throws IOException {
        DocumentStore store = new DocumentStoreImpl();
        store.putDocument(new ByteArrayInputStream(this.txt1.getBytes()),this.uri1, DocumentStore.DocumentFormat.TXT);
        store.putDocument(new ByteArrayInputStream(this.txt2.getBytes()),this.uri2, DocumentStore.DocumentFormat.TXT);
        store.deleteAll("pizza");
        assertEquals(0, store.search("pizza").size());
        store.undo(uri2);
        assertEquals(1, store.search("pizza").size());
        store.undo(uri1);
        assertEquals(2, store.search("pizza").size());
        store.undo();
        assertNull(store.getDocument(uri2));
        assertNotNull(store.getDocument(uri1));
        assertEquals(1, store.search("pizza").size());
    }
	Trie trie = new TrieImpl<>();
	String string1 = "It was a dark and stormy night";
	String string2 = "It was the best of times it was the worst of times";
	String string3 = "It was a bright cold day in April and the clocks were striking thirteen";
	String string4 = "I am free no matter what rules surround me";
	@Test
	public void testPut() {
		assertThrows(IllegalArgumentException.class, () -> {
			trie.put(null, 100);
		});

		trie.put("", 100);

		trie.put("the", null);

		assertEquals(trie.getAllSorted("the", Comparator.naturalOrder()).size(), 2);
	}
	@Test
	public void testGetAllSorted() {
		assertThrows(IllegalArgumentException.class, () -> {
			trie.getAllSorted("the", null);
		});

		assertThrows(IllegalArgumentException.class, () -> {
			trie.getAllSorted(null, Comparator.naturalOrder());
		});

		assertEquals(trie.getAllSorted("", Comparator.naturalOrder()).size(), 0);
	}
	@Test
	public void testGetAllPrefixSorted() {
		assertThrows(IllegalArgumentException.class, () -> {
			trie.getAllWithPrefixSorted("the", null);
		});

		assertThrows(IllegalArgumentException.class, () -> {
			trie.getAllWithPrefixSorted(null, Comparator.naturalOrder());
		});

		assertEquals(trie.getAllWithPrefixSorted("", Comparator.naturalOrder()).size(), 0);
	}
	@Test
	public void testDeleteWithPrefix() {
		assertThrows(IllegalArgumentException.class, () -> {
			trie.deleteAllWithPrefix(null);
		});

		assertTrue(trie.deleteAllWithPrefix("").size()==0);

		assertEquals(trie.getAllWithPrefixSorted("the", Comparator.naturalOrder()).size(), 2);
	}
	@Test
	public void testDeleteAll() {
		assertThrows(IllegalArgumentException.class, () -> {
			trie.deleteAll(null);
		});

		assertTrue(trie.deleteAll("").size()==0);

		assertEquals(trie.getAllSorted("the", Comparator.naturalOrder()).size(), 2);
	}
	@Test
	public void testDelete() {
		assertThrows(IllegalArgumentException.class, () -> {
			trie.delete("the", null);
		});

		assertThrows(IllegalArgumentException.class, () -> {
			trie.delete(null, 100);
		});

		assertTrue(trie.delete("", 0)==null);
//Deliberately don't use Integer factory
		@SuppressWarnings("deprecation")
		Integer largeInteger = new Integer(1000);
		@SuppressWarnings("deprecation")
		Integer largeInteger2 = new Integer(1000);
		trie.put("largeInteger", largeInteger);
		assertTrue(largeInteger != largeInteger2);
		assertTrue(trie.delete("largeInteger", largeInteger2)==largeInteger);
	}
	@Test
	public void complicatedTrieTest() {
		Trie trie = new TrieImpl<Integer>();
		trie.put("APPLE123", 1);
		trie.put("APPLE123", 2);
		trie.put("APPLE123", 3);
		trie.put("APPle87", 8);
		trie.put("aPpLe87", 7);
		List<Integer> appleList = trie.getAllSorted("apple123", (int1, int2) -> {
			if ((int) int1 < (int) int2) {
				return -1;
			} else if ((int) int2 < (int) int1) {
				return 1;
			}
			return 0;});
		appleList.addAll(trie.getAllSorted("apple87", (int1, int2) -> {
			if ((int) int1 < (int) int2) {
				return -1;
			} else if ((int) int2 < (int) int1) {
				return 1;
			}
			return 0;}));
		assertEquals(5, appleList.size());
		List<Integer> testSet = List.copyOf(appleList);
		Set<Integer> deleteSet = trie.deleteAllWithPrefix("app");
		assertEquals(5, deleteSet.size());
		assertEquals(deleteSet.size(), testSet.size());
		if (!deleteSet.containsAll(testSet)){
			fail();
		}
		//System.out.println("you passed complicatedTrieTest, congratulations!!!");
	}

	//variables to hold possible values for doc1
	private URI uri11;
	private String txt11;

	//variables to hold possible values for doc2
	private URI uri22;
	String txt22;

	private URI uri33;
	String txt33;
	@Test
	public void complicatedDocumentStoreTest() throws IOException, URISyntaxException {
		//init possible values for doc1
		this.uri11 = new URI("http://edu.yu.cs/com1320/project/doc1");
		this.txt11 = "Apple Apple AppleProducts applesAreGood Apps APCalculus Apricots";

		//init possible values for doc2
		this.uri22 = new URI("http://edu.yu.cs/com1320/project/doc2");
		this.txt22 = "Apple Apple Apple Apple Apple";

		//init possible values for doc3
		this.uri33 = new URI("http://edu.yu.cs/com1320/project/doc3");
		this.txt33 = "APenguin APark APiccalo APants APain APossum";
		DocumentStore store = new DocumentStoreImpl();
		store.putDocument(new ByteArrayInputStream(this.txt11.getBytes()), this.uri11, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt22.getBytes()), this.uri22, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt33.getBytes()), this.uri33, DocumentStore.DocumentFormat.TXT);
		List<Document> appleList = new ArrayList<>();
		appleList.addAll(store.searchByPrefix("ap"));
		assertEquals(3, appleList.size());
		List<URI> testSet = new ArrayList<>();
		for(Document doc :appleList){
			testSet.add(doc.getKey());
		}
		Set<URI> deleteSet = store.deleteAllWithPrefix("ap");
		assertEquals(3, deleteSet.size());
		assertEquals(deleteSet.size(), testSet.size());
		if (!deleteSet.containsAll(testSet)){
			fail();
		}
		//System.out.println("you passed complicatedDocumentStoreTest, congratulations!!!");
	}
	@Test
	public void reallyComplicatedDocumentStoreUndoTest() throws IOException, URISyntaxException {
		//init possible values for doc1
		this.uri1 = new URI("http://edu.yu.cs/com1320/project/doc1");
		this.txt1 = "Apple Apple AppleProducts applesAreGood Apps APCalculus Apricots";

		//init possible values for doc2
		this.uri2 = new URI("http://edu.yu.cs/com1320/project/doc2");
		this.txt2 = "Apple Apple Apple Apple Apple";

		//init possible values for doc3
		this.uri3 = new URI("http://edu.yu.cs/com1320/project/doc3");
		this.txt3 = "APenguin APark APiccalo APants APain APossum";
		DocumentStore store = new DocumentStoreImpl();
		store.putDocument(new ByteArrayInputStream(this.txt1.getBytes()), this.uri1, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt2.getBytes()), this.uri2, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt3.getBytes()), this.uri3, DocumentStore.DocumentFormat.TXT);
		List<Document> appleList = new ArrayList<>();
		appleList.addAll(store.searchByPrefix("ap"));
		assertEquals(3, appleList.size());
		store.undo(this.uri2);
		appleList = store.searchByPrefix("ap");
		assertEquals(2, appleList.size());
		List<URI> testSet = new ArrayList<>();
		for(Document doc :appleList){
			testSet.add(doc.getKey());
		}
		store.putDocument(new ByteArrayInputStream(this.txt2.getBytes()), this.uri2, DocumentStore.DocumentFormat.TXT);
		appleList = store.searchByPrefix("ap");
		assertEquals(3, appleList.size());
		Set<URI> deleteSet = store.deleteAllWithPrefix("ap");
		assertEquals(3, deleteSet.size());
		store.undo(this.uri1);
		store.undo(this.uri3);
		assertEquals(2, store.searchByPrefix("ap").size());
		deleteSet = store.deleteAllWithPrefix("ap");
		assertEquals(2, deleteSet.size());
		assertEquals(deleteSet.size(), testSet.size());
		if (!deleteSet.containsAll(testSet)){
			fail();
		}
		//System.out.println("you passed reallyComplicatedDocumentStoreUndoTest, congratulations!!!");
	}
	@Test
	public void testOrder() throws IOException, URISyntaxException{
		this.uri1 = new URI("http://edu.yu.cs/com1320/project/doc1");
		this.txt1 = "Apple Apple AppleProducts applesAreGood Apps APCalculus Apricots";

//init possible values for doc2
		this.uri2 = new URI("http://edu.yu.cs/com1320/project/doc2");
		this.txt2 = "Apple Apple Apple Apple Apple";

//init possible values for doc3
		this.uri3 = new URI("http://edu.yu.cs/com1320/project/doc3");
		this.txt3 = "APenguin APark APiccalo APants APain APossum";

		URI uri4 = new URI("http://edu.yu.cs/com1320/project/doc4");
		String txt4 = "ap APPLE apartment";
		DocumentStoreImpl store = new DocumentStoreImpl();
		store.putDocument(new ByteArrayInputStream(this.txt1.getBytes()), this.uri1, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt2.getBytes()), this.uri2, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(this.txt3.getBytes()), this.uri3, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream(txt4.getBytes()), uri4, DocumentStore.DocumentFormat.TXT);
		List<Document> wordList = store.search("apple");
		List<Document> prefixList = store.searchByPrefix("ap");
		assertEquals(wordList.size(), 3);
		assertEquals(wordList.get(0).getKey(), uri2);
		assertEquals(wordList.get(1).getKey(), uri1);
		assertEquals(wordList.get(2).getKey(), uri4);

		assertEquals(prefixList.size(), 4);
		assertEquals(prefixList.get(0).getKey(), uri1);
		assertEquals(prefixList.get(1).getKey(), uri3);
		assertEquals(prefixList.get(2).getKey(), uri2);

	}
	@Test
	public void simplePushAndPop() {
	    StackImpl<String> s = new StackImpl<>();
	    s.push("one");
	    s.push("two");
	    s.push("three");
	    assertEquals(3, s.size());
	    assertEquals("three", s.peek());
	    assertEquals("three", s.pop());
	    assertEquals("two", s.peek());
	    assertEquals("two", s.peek());
	    assertEquals(2, s.size());
	    assertEquals("two", s.pop());
	    assertEquals("one", s.pop());
	    assertEquals(0, s.size());
	}
	@Test
	public void aLotOfData() {
	    StackImpl<Integer> s = new StackImpl<>();
	    for (int i = 0; i < 1000; i++) {
	        s.push(i);
	        assertEquals((Integer)i, s.peek());
	    }
	    assertEquals(1000, s.size());
	    assertEquals((Integer)999, s.peek());
	    for (int i = 999; i >= 0; i--) {
	        assertEquals((Integer)i, s.peek());
	        assertEquals((Integer)i, s.pop());
	    }
	    assertEquals(0, s.size());
	}
	@Test
	public void undoTest() throws IOException {
	    DocumentStore documentStore = new DocumentStoreImpl();
	    Boolean test = false;
	    try {
	        documentStore.undo();
	    } catch (IllegalStateException e) {
	        test = true;
	    }
	    assertEquals(true, test);
	    String string1 = "It was a dark and stormy night";
	    String string2 = "It was the best of times, it was the worst of times";
	    String string3 = "It was a bright cold day in April, and the clocks were striking thirteen";
	    InputStream inputStream1 = new ByteArrayInputStream(string1.getBytes());
	    InputStream inputStream2 = new ByteArrayInputStream(string2.getBytes());
	    InputStream inputStream3 = new ByteArrayInputStream(string3.getBytes());
	    URI uri1 = URI.create("www.wrinkleintime.com");

	    documentStore.putDocument(inputStream1, uri1, DocumentStore.DocumentFormat.TXT);
	    assertEquals(string1, documentStore.getDocument(uri1).getDocumentTxt());
	    documentStore.putDocument(inputStream2, uri1, DocumentStore.DocumentFormat.TXT);
	    assertEquals(string2, documentStore.getDocument(uri1).getDocumentTxt());
	    documentStore.undo();
	    //System.out.println(documentStore.getDocument(uri1).getDocumentTxt());
	    assertEquals(string1, documentStore.getDocument(uri1).getDocumentTxt());
	    documentStore.undo();
		//System.out.println(documentStore.getDocument(uri1).getDocumentTxt());
	    assertEquals(null, documentStore.getDocument(uri1));

	    documentStore.putDocument(inputStream3, uri1, DocumentStore.DocumentFormat.TXT);
	    assertEquals(string3, documentStore.getDocument(uri1).getDocumentTxt());
	    documentStore.deleteDocument(uri1);
	    assertEquals(null, documentStore.getDocument(uri1));
	    documentStore.undo();
	    assertEquals(string3, documentStore.getDocument(uri1).getDocumentTxt());
	}
	@Test
	public void testUndoSpecificUri() throws IOException {
	    DocumentStore documentStore = new DocumentStoreImpl();

	    String string1 = "It was a dark and stormy night";
	    String string2 = "It was the best of times, it was the worst of times";
	    String string3 = "It was a bright cold day in April, and the clocks were striking thirteen";
	    String string4 = "I am free, no matter what rules surround me.";
	    InputStream inputStream1 = new ByteArrayInputStream(string1.getBytes());
	    InputStream inputStream2 = new ByteArrayInputStream(string2.getBytes());
	    InputStream inputStream3 = new ByteArrayInputStream(string3.getBytes());
	    InputStream inputStream4 = new ByteArrayInputStream(string4.getBytes());
	    URI uri1 = URI.create("www.wrinkleintime.com");
	    URI uri2 = URI.create("www.taleoftwocities.com");
	    URI uri3 = URI.create("www.1984.com");

	    documentStore.putDocument(inputStream1, uri1, DocumentStore.DocumentFormat.TXT);
	    assertEquals(string1, documentStore.getDocument(uri1).getDocumentTxt());
	    documentStore.putDocument(inputStream2, uri2, DocumentStore.DocumentFormat.TXT);
	    assertEquals(string2, documentStore.getDocument(uri2).getDocumentTxt());
	    documentStore.undo(uri1);
	    assertEquals(null, documentStore.getDocument(uri1));
	    assertEquals(string2, documentStore.getDocument(uri2).getDocumentTxt());
	    documentStore.putDocument(inputStream3, uri1, DocumentStore.DocumentFormat.TXT);
	    assertEquals(string3, documentStore.getDocument(uri1).getDocumentTxt());
	    documentStore.putDocument(inputStream4, uri1, DocumentStore.DocumentFormat.TXT);
	    assertEquals(string4, documentStore.getDocument(uri1).getDocumentTxt());
	    documentStore.deleteDocument(uri1);
	    assertEquals(null, documentStore.getDocument(uri1));
	    documentStore.undo(uri2);
	    assertEquals(null, documentStore.getDocument(uri2));
	    documentStore.undo();
	    assertEquals(string4, documentStore.getDocument(uri1).getDocumentTxt());
	    documentStore.undo(uri1);
	    assertEquals(string3, documentStore.getDocument(uri1).getDocumentTxt());

	    Boolean test = false;
	    try {
	        documentStore.undo(uri3);
	    } catch (IllegalStateException e) {
	        test = true;
	    }
	    assertEquals(true, test);
	}
	@Test
	void testStackUndo() throws IOException, URISyntaxException {
        DocumentStoreImpl store = new DocumentStoreImpl();
        String str1 = "1"; byte[] array1 = str1.getBytes();
        ByteArrayInputStream stream1 = new ByteArrayInputStream(array1);
        ByteArrayInputStream stream11 = new ByteArrayInputStream(array1);
        URI uri1 = new URI("1");
        assertEquals(0, store.putDocument(stream1, uri1, DocumentFormat.BINARY));
        Document doc = new DocumentImpl(uri1, stream11.readAllBytes());
        assertEquals(doc, store.getDocument(uri1));
        store.undo(); assertEquals(null, store.getDocument(uri1));
        boolean test = false;
        try {
            store.undo();
        } catch (IllegalStateException e) {
            test = true;
        }
        assertTrue(test);
    }
    @Test
    void testStackUndoUri() throws IOException, URISyntaxException {
        DocumentStoreImpl store = new DocumentStoreImpl();
        String str1 = "1"; byte[] array1 = str1.getBytes();
        ByteArrayInputStream stream1 = new ByteArrayInputStream(array1); ByteArrayInputStream stream11 = new ByteArrayInputStream(array1);
        URI uri1 = new URI("1");
        assertEquals(0, store.putDocument(stream1, uri1, DocumentFormat.BINARY));
        Document doc = new DocumentImpl(uri1, stream11.readAllBytes());
        assertEquals(doc, store.getDocument(uri1));
        String str2 = "2";
        byte[] array2 = str2.getBytes();
        ByteArrayInputStream stream2 = new ByteArrayInputStream(array2);
        ByteArrayInputStream stream22 = new ByteArrayInputStream(array2);
        URI uri2 = new URI("2");
        assertEquals(0, store.putDocument(stream2, uri2, DocumentFormat.BINARY));
        Document doc2 = new DocumentImpl(uri2, stream22.readAllBytes());
        assertEquals(doc2, store.getDocument(uri2));
        store.undo(uri1);
        assertEquals(null, store.getDocument(uri1));
        assertEquals(doc2, store.getDocument(uri2));
    }
    @Test
    void testStackUriPutOverwrite() throws IOException, URISyntaxException {
        DocumentStoreImpl store = new DocumentStoreImpl();
        String str1 = "1"; byte[] array1 = str1.getBytes();
        ByteArrayInputStream stream1 = new ByteArrayInputStream(array1); ByteArrayInputStream stream11 = new ByteArrayInputStream(array1);
        URI uri = new URI("1");
        assertEquals(0, store.putDocument(stream1, uri, DocumentFormat.BINARY));
        Document doc = new DocumentImpl(uri, stream11.readAllBytes());
        assertEquals(doc, store.getDocument(uri));
        String str2 = "2"; byte[] array2 = str2.getBytes();
        ByteArrayInputStream stream2 = new ByteArrayInputStream(array2); ByteArrayInputStream stream22 = new ByteArrayInputStream(array2);
        assertEquals(doc.hashCode(), store.putDocument(stream2, uri, DocumentFormat.BINARY));
        Document doc2 = new DocumentImpl(uri, stream22.readAllBytes());
        assertEquals(doc2, store.getDocument(uri));
        store.undo();
        assertNotEquals(doc2, store.getDocument(uri));
        assertEquals(doc, store.getDocument(uri));
        store.undo(); assertEquals(null, store.getDocument(uri));
    }
    @Test
    void testStackUriDeleteOverwrite() throws IOException, URISyntaxException {
        DocumentStoreImpl store = new DocumentStoreImpl();
        String str1 = "1"; byte[] array1 = str1.getBytes();
        ByteArrayInputStream stream1 = new ByteArrayInputStream(array1); ByteArrayInputStream stream11 = new ByteArrayInputStream(array1);
        URI uri = new URI("1");
        assertEquals(0, store.putDocument(stream1, uri, DocumentFormat.BINARY));
        Document doc = new DocumentImpl(uri, stream11.readAllBytes());
        assertEquals(doc, store.getDocument(uri));
        String str2 = "2"; byte[] array2 = str2.getBytes();
        ByteArrayInputStream stream2 = new ByteArrayInputStream(array2); ByteArrayInputStream stream22 = new ByteArrayInputStream(array2);
        assertEquals(doc.hashCode(), store.putDocument(stream2, uri, DocumentFormat.BINARY));
        Document doc2 = new DocumentImpl(uri, stream22.readAllBytes());
        assertEquals(doc2, store.getDocument(uri));
        assertTrue(store.deleteDocument(uri)); assertEquals(null, store.getDocument(uri));
        String str3 = "3"; byte[] array3 = str3.getBytes();
        ByteArrayInputStream stream3 = new ByteArrayInputStream(array3); ByteArrayInputStream stream33 = new ByteArrayInputStream(array3);
        assertEquals(0, store.putDocument(stream3, uri, DocumentFormat.BINARY));
        Document doc3 = new DocumentImpl(uri, stream33.readAllBytes());
        assertEquals(doc3, store.getDocument(uri));
        store.undo(uri); assertEquals(null, store.getDocument(uri));
        store.undo(uri); assertEquals(doc2, store.getDocument(uri));
        store.undo(uri); assertEquals(doc, store.getDocument(uri));
        store.undo(uri); assertEquals(null, store.getDocument(uri));
    }
    @Test
    void testStackUriDeleteOverwriteNoParams() throws IOException, URISyntaxException {
        DocumentStoreImpl store = new DocumentStoreImpl();
        String str1 = "1"; byte[] array1 = str1.getBytes();
        ByteArrayInputStream stream1 = new ByteArrayInputStream(array1); ByteArrayInputStream stream11 = new ByteArrayInputStream(array1);
        URI uri = new URI("1");
        assertEquals(0, store.putDocument(stream1, uri, DocumentFormat.BINARY));
        Document doc = new DocumentImpl(uri, stream11.readAllBytes());
        assertEquals(doc, store.getDocument(uri));
        String str2 = "2"; byte[] array2 = str2.getBytes();
        ByteArrayInputStream stream2 = new ByteArrayInputStream(array2); ByteArrayInputStream stream22 = new ByteArrayInputStream(array2);
        assertEquals(doc.hashCode(), store.putDocument(stream2, uri, DocumentFormat.BINARY));
        Document doc2 = new DocumentImpl(uri, stream22.readAllBytes());
        assertEquals(doc2, store.getDocument(uri));
        assertTrue(store.deleteDocument(uri)); assertEquals(null, store.getDocument(uri));
        String str3 = "3"; byte[] array3 = str3.getBytes();
        ByteArrayInputStream stream3 = new ByteArrayInputStream(array3); ByteArrayInputStream stream33 = new ByteArrayInputStream(array3);
        assertEquals(0, store.putDocument(stream3, uri, DocumentFormat.BINARY));
        Document doc3 = new DocumentImpl(uri, stream33.readAllBytes());
        assertEquals(doc3, store.getDocument(uri));
        store.undo(); assertEquals(null, store.getDocument(uri));
        store.undo(); assertEquals(doc2, store.getDocument(uri));
        store.undo(); assertEquals(doc, store.getDocument(uri));
        store.undo(); assertEquals(null, store.getDocument(uri));
    }
    @Test
    void testStackUriDeleteOverwriteMultipleDocs() throws IOException, URISyntaxException {
        DocumentStoreImpl store = new DocumentStoreImpl();
        String str1 = "1"; byte[] array1 = str1.getBytes();
        ByteArrayInputStream stream1 = new ByteArrayInputStream(array1); ByteArrayInputStream stream11 = new ByteArrayInputStream(array1);
        URI uri = new URI("1");
        assertEquals(0, store.putDocument(stream1, uri, DocumentFormat.BINARY));
        Document doc = new DocumentImpl(uri, stream11.readAllBytes());
        assertEquals(doc, store.getDocument(uri));
        String str2 = "2"; byte[] array2 = str2.getBytes();
        ByteArrayInputStream stream2 = new ByteArrayInputStream(array2); ByteArrayInputStream stream22 = new ByteArrayInputStream(array2);
        assertEquals(doc.hashCode(), store.putDocument(stream2, uri, DocumentFormat.BINARY));
        Document doc2 = new DocumentImpl(uri, stream22.readAllBytes());
        assertEquals(doc2, store.getDocument(uri));
        assertTrue(store.deleteDocument(uri)); assertEquals(null, store.getDocument(uri));
        String str3 = "3"; byte[] array3 = str3.getBytes();
        URI uri2 = new URI("Hello");
        ByteArrayInputStream stream3 = new ByteArrayInputStream(array3); ByteArrayInputStream stream33 = new ByteArrayInputStream(array3);
        assertEquals(0, store.putDocument(stream3, uri2, DocumentFormat.BINARY));
        Document doc3 = new DocumentImpl(uri2, stream33.readAllBytes());
        assertEquals(doc3, store.getDocument(uri2));
        store.undo(uri); assertEquals(doc3, store.getDocument(uri2)); assertEquals(doc2, store.getDocument(uri));
        store.undo(); assertEquals(null, store.getDocument(uri2));
        store.undo(); assertEquals(doc, store.getDocument(uri));
        store.undo(); assertEquals(null, store.getDocument(uri));
    }
    @Test
    void testThrowsException() throws URISyntaxException, IOException {
        DocumentStoreImpl store = new DocumentStoreImpl();
        boolean test = false;
        try {
            store.undo();
        } catch (IllegalStateException e) {
            test = true;
        }
        assertTrue(test);
        test = false;
        String str1 = "1"; byte[] array1 = str1.getBytes();
        ByteArrayInputStream stream1 = new ByteArrayInputStream(array1); ByteArrayInputStream stream11 = new ByteArrayInputStream(array1);
        URI uri = new URI("1");
        assertEquals(0, store.putDocument(stream1, uri, DocumentFormat.BINARY));
        Document doc = new DocumentImpl(uri, stream11.readAllBytes());
        assertEquals(doc, store.getDocument(uri));
        URI uriFake = new URI("ThisIsAFake");
        try {
            store.undo(uriFake);
        } catch (IllegalStateException e) {
            test = true;
        }
        assertTrue(test);
    }
    @Test
    void testPointlessDeleteEmptyUndo() throws URISyntaxException {
        DocumentStoreImpl store = new DocumentStoreImpl();
        URI uri = new URI("Pizza");
        assertFalse(store.deleteDocument(uri));
        store.undo();
    }
    @Test
    void testPointlessDeleteFullUndo() throws URISyntaxException {
        DocumentStoreImpl store = new DocumentStoreImpl();
        URI uri = new URI("Pizza");
        assertFalse(store.deleteDocument(uri));
        store.undo(uri);
    }
    @Test
    void testPointlessPutEmptyUndo() throws URISyntaxException, IOException {
        DocumentStoreImpl store = new DocumentStoreImpl();
        String str1 = "1"; byte[] array1 = str1.getBytes();
        URI uri = new URI("1");
        assertEquals(0, store.putDocument(null, uri, DocumentFormat.TXT));
        store.undo();
    }
    @Test
    void testPointlessPutFullUndo() throws URISyntaxException, IOException {
        DocumentStoreImpl store = new DocumentStoreImpl();
        String str1 = "1";
        URI uri = new URI("1");
        assertEquals(0, store.putDocument(null, uri, DocumentFormat.TXT));
        assertNull(store.getDocument(uri));
        boolean test = false;
        try {
            store.undo(new URI("Pizza"));
        } catch (IllegalStateException e) {
            test = true;
        }
        assertTrue(test);
        store.undo(uri);
    }
	URI[] uriArray = new URI[21];
	Document[] docArray = new Document[21];
	String[] stringArray = {"The blue parrot drove by the hitchhiking mongoose.",
			"She thought there'd be sufficient time if she hid her watch.",
			"Choosing to do nothing is still a choice, after all.",
			"He found the chocolate covered roaches quite tasty.",
			"The efficiency we have at removing trash has made creating trash more acceptable.",
			"Peanuts don't grow on trees, but cashews do.",
			"A song can make or ruin a person's day if they let it get to them.",
			"You bite up because of your lower jaw.",
			"He realized there had been several deaths on this road, but his concern rose when he saw the exact number.",
			"So long and thanks for the fish.",
			"Three years later, the coffin was still full of Jello.",
			"Weather is not trivial - it's especially important when you're standing in it.",
			"He walked into the basement with the horror movie from the night before playing in his head.",
			"He wondered if it could be called a beach if there was no sand.",
			"Jeanne wished she has chosen the red button.",
			"It's much more difficult to play tennis with a bowling ball than it is to bowl with a tennis ball.",
			"Pat ordered a ghost pepper pie.",
			"Everyone says they love nature until they realize how dangerous she can be.",
			"The memory we used to share is no longer coherent.",
			"My harvest will come Tiny valorous straw Among the millions Facing to the sun",
			"A dreamy-eyed child staring into night On a journey to storyteller's mind Whispers a wish speaks with the stars the words are silent in him"};
	@Test
	void testUndo() {
		for (int i = 0; i < 7; i++) {
			uriArray[i] = URI.create("www.google"+i+".com");
		}

		for (int i = 0; i < 7; i++) {
			docArray[i] = new DocumentImpl(uriArray[i], stringArray[i]);
		}
		for (int i = 0; i < 7; i++) {
			docArray[i+7] = new DocumentImpl(uriArray[i], stringArray[i+7].getBytes());
		}
		for (int i = 0; i < 7; i++) {
			docArray[i+14] = new DocumentImpl(uriArray[i], stringArray[i+14]);
		}
		DocumentStore documentStore = new DocumentStoreImpl();
		try {
			int testa1 = documentStore.putDocument(new ByteArrayInputStream(stringArray[0].getBytes()), uriArray[0], DocumentStore.DocumentFormat.TXT);
			int testa2 = documentStore.putDocument(new ByteArrayInputStream(stringArray[1].getBytes()), uriArray[1], DocumentStore.DocumentFormat.TXT);
			int testa3 = documentStore.putDocument(new ByteArrayInputStream(stringArray[2].getBytes()), uriArray[2], DocumentStore.DocumentFormat.TXT);
			int testa4 = documentStore.putDocument(new ByteArrayInputStream(stringArray[3].getBytes()), uriArray[3], DocumentStore.DocumentFormat.TXT);
			int testa5 = documentStore.putDocument(new ByteArrayInputStream(stringArray[4].getBytes()), uriArray[4], DocumentStore.DocumentFormat.TXT);
			int testa6 = documentStore.putDocument(new ByteArrayInputStream(stringArray[5].getBytes()), uriArray[5], DocumentStore.DocumentFormat.TXT);
			int testa7 = documentStore.putDocument(new ByteArrayInputStream(stringArray[6].getBytes()), uriArray[6], DocumentStore.DocumentFormat.TXT);
			assertEquals(testa1, 0);
			assertEquals(testa2, 0);
			assertEquals(testa3, 0);
			assertEquals(testa4, 0);
			assertEquals(testa5, 0);
			assertEquals(testa6, 0);
			assertEquals(testa7, 0);
		} catch (java.io.IOException e) {
			fail();
		}

		documentStore.undo();

		assertEquals(docArray[0], documentStore.getDocument(uriArray[0]));
		assertEquals(docArray[1], documentStore.getDocument(uriArray[1]));
		assertEquals(docArray[2], documentStore.getDocument(uriArray[2]));
		assertEquals(docArray[3], documentStore.getDocument(uriArray[3]));
		assertEquals(docArray[4], documentStore.getDocument(uriArray[4]));
		assertEquals(docArray[5], documentStore.getDocument(uriArray[5]));
		assertEquals(null, documentStore.getDocument(uriArray[6]));

		documentStore.undo(uriArray[1]);

		try {
			int testb1 = documentStore.putDocument(new ByteArrayInputStream(stringArray[7].getBytes()), uriArray[0], DocumentStore.DocumentFormat.BINARY);
			int testb2 = documentStore.putDocument(new ByteArrayInputStream(stringArray[8].getBytes()), uriArray[1], DocumentStore.DocumentFormat.BINARY);
			int testb3 = documentStore.putDocument(new ByteArrayInputStream(stringArray[9].getBytes()), uriArray[2], DocumentStore.DocumentFormat.BINARY);
			int testb4 = documentStore.putDocument(new ByteArrayInputStream(stringArray[10].getBytes()), uriArray[3], DocumentStore.DocumentFormat.BINARY);
			int testb5 = documentStore.putDocument(new ByteArrayInputStream(stringArray[11].getBytes()), uriArray[4], DocumentStore.DocumentFormat.BINARY);
			int testb6 = documentStore.putDocument(new ByteArrayInputStream(stringArray[12].getBytes()), uriArray[5], DocumentStore.DocumentFormat.BINARY);
			int testb7 = documentStore.putDocument(new ByteArrayInputStream(stringArray[13].getBytes()), uriArray[6], DocumentStore.DocumentFormat.BINARY);
			assertEquals(testb1, docArray[0].hashCode());
			assertEquals(testb2, 0);
			assertEquals(testb3, docArray[2].hashCode());
			assertEquals(testb4, docArray[3].hashCode());
			assertEquals(testb5, docArray[4].hashCode());
			assertEquals(testb6, docArray[5].hashCode());
			assertEquals(testb7, 0);
		} catch (java.io.IOException e) {
			fail();
		}

		documentStore.undo(uriArray[1]);
		documentStore.undo(uriArray[4]);
		documentStore.undo(uriArray[5]);

		assertEquals(docArray[7], documentStore.getDocument(uriArray[0]));
		assertEquals(null, documentStore.getDocument(uriArray[1]));
		assertEquals(docArray[9], documentStore.getDocument(uriArray[2]));
		assertEquals(docArray[10], documentStore.getDocument(uriArray[3]));
		assertEquals(docArray[4], documentStore.getDocument(uriArray[4]));
		assertEquals(docArray[5], documentStore.getDocument(uriArray[5]));
		assertEquals(docArray[13], documentStore.getDocument(uriArray[6]));

		try {
			int testc1 = documentStore.putDocument(new ByteArrayInputStream(stringArray[14].getBytes()), uriArray[0], DocumentStore.DocumentFormat.TXT);
			int testc2 = documentStore.putDocument(new ByteArrayInputStream(stringArray[15].getBytes()), uriArray[1], DocumentStore.DocumentFormat.TXT);
			int testc3 = documentStore.putDocument(new ByteArrayInputStream(stringArray[16].getBytes()), uriArray[2], DocumentStore.DocumentFormat.TXT);
			int testc4 = documentStore.putDocument(new ByteArrayInputStream(stringArray[17].getBytes()), uriArray[3], DocumentStore.DocumentFormat.TXT);
			int testc5 = documentStore.putDocument(new ByteArrayInputStream(stringArray[18].getBytes()), uriArray[4], DocumentStore.DocumentFormat.TXT);
			int testc6 = documentStore.putDocument(new ByteArrayInputStream(stringArray[19].getBytes()), uriArray[5], DocumentStore.DocumentFormat.TXT);
			int testc7 = documentStore.putDocument(new ByteArrayInputStream(stringArray[20].getBytes()), uriArray[6], DocumentStore.DocumentFormat.TXT);

			documentStore.undo(uriArray[1]);
			documentStore.undo(uriArray[6]);
			documentStore.undo();

			assertEquals(testc1, docArray[7].hashCode());
			assertEquals(testc2, 0);
			assertEquals(testc3, docArray[9].hashCode());
			assertEquals(testc4, docArray[10].hashCode());
			assertEquals(testc5, docArray[4].hashCode());
			assertEquals(testc6, docArray[5].hashCode());
			assertEquals(testc7, docArray[13].hashCode());
		} catch (java.io.IOException e) {
			fail();
		}

		assertEquals(docArray[14], documentStore.getDocument(uriArray[0]));
		assertEquals(null, documentStore.getDocument(uriArray[1]));
		assertEquals(docArray[16], documentStore.getDocument(uriArray[2]));
		assertEquals(docArray[17], documentStore.getDocument(uriArray[3]));
		assertEquals(docArray[18], documentStore.getDocument(uriArray[4]));
		assertEquals(docArray[5], documentStore.getDocument(uriArray[5]));
		assertEquals(docArray[13], documentStore.getDocument(uriArray[6]));

		for (int i = 0; i < 7; i++) {
			documentStore.undo();
		}

		assertEquals(docArray[7], documentStore.getDocument(uriArray[0]));
		assertEquals(null, documentStore.getDocument(uriArray[1]));
		assertEquals(docArray[2], documentStore.getDocument(uriArray[2]));
		assertEquals(docArray[3], documentStore.getDocument(uriArray[3]));
		assertEquals(docArray[4], documentStore.getDocument(uriArray[4]));
		assertEquals(docArray[5], documentStore.getDocument(uriArray[5]));
		assertEquals(null, documentStore.getDocument(uriArray[6]));
	}
    @Test
    void hasParameterlessPublicConstructorTestStack() {
        try {
            new StackImpl<>();
        } catch (RuntimeException e) {
            fail("no parameterless constructor");
        }
    }
    @Test
    void testParameterLessUndoOnDoc() throws URISyntaxException, IOException {
        URI uri = new URI("YouAreEye");
        String first = "first";
        String second = "second";
        Document one = new DocumentImpl(uri, first);
        Document two = new DocumentImpl(uri, second);
        DocumentStore store = new DocumentStoreImpl();
        store.putDocument(new ByteArrayInputStream(first.getBytes()), uri, DocumentFormat.TXT);
        assertEquals(store.getDocument(uri), one);
        assertEquals(store.putDocument(new ByteArrayInputStream(second.getBytes()), uri,
                DocumentFormat.TXT), one.hashCode());
        assertEquals(store.getDocument(uri), two);
        store.undo();
        assertEquals(store.getDocument(uri), one);
    }
    @Test
    void testParameterUndoOnDoc() throws URISyntaxException, IOException {
        URI uri = new URI("YouAreEye");
        String first = "first";
        String second = "second";
        Document one = new DocumentImpl(uri, first);
        Document two = new DocumentImpl(uri, second);
        DocumentStore store = new DocumentStoreImpl();
        store.putDocument(new ByteArrayInputStream(first.getBytes()), uri, DocumentFormat.TXT);
        assertEquals(store.getDocument(uri), one);
        assertEquals(store.putDocument(new ByteArrayInputStream(second.getBytes()), uri, DocumentFormat.TXT),
                one.hashCode());
        assertEquals(store.getDocument(uri), two);
        store.undo(uri);
        assertEquals(store.getDocument(uri), one);
    }
    @Test
    void testUndoWithEmptyStack() throws URISyntaxException {
        DocumentStore store = new DocumentStoreImpl();
        assertThrows(IllegalStateException.class, ()->{
            store.undo();
        });
        assertThrows(IllegalStateException.class, () -> {
            store.undo(new URI("uri"));
        });
    }
    @Test
    public void oneStackImpl(){
        StackImpl<Integer> stack = new StackImpl<>();
        for(int i = 0; i < 100; i++){
            stack.push(i);
            
        }
        for(int i = 99; i >= 0; i--){
            assertEquals(stack.peek(), i);
            assertEquals(stack.pop(), i);
        }
    }
    @Test
    void DocumentImplTest() throws URISyntaxException {
        URI uri1 = new URI("www.tuvwxyz.com");
        String txt = "YAGILU";
        URI uri2 = new URI("www.xyz.com");
        URI uri = new URI("cmpsci4days.com");
        String txt2 = "Up too late at night";
        byte[] pic = "Lots of Pics and Lots of Bytes".getBytes();
        byte[] pic2 = "Even more pix".getBytes();
        DocumentImpl doc1 = new DocumentImpl(uri1, txt);
        DocumentImpl pic1 = new DocumentImpl(uri2, pic2);
        DocumentImpl doc2 = new DocumentImpl(uri1, txt);
        assertEquals(uri1, doc1.getKey());
        assertEquals(uri2, pic1.getKey());
        assertEquals(txt, doc1.getDocumentTxt());
        assertEquals(pic2, pic1.getDocumentBinaryData());
        assertEquals(doc1.hashCode(), doc2.hashCode());
    }
    @Test
    void StackImplTest() {
        StackImpl stack = new StackImpl<Integer>();
        for (int i = 0; i < 30; i++) {
            stack.push(i);
        }
        assertEquals(29, stack.pop());
        assertEquals(28, stack.peek());
        assertEquals(29, stack.size());
        stack.push(123);
        assertEquals(123, stack.peek());
        assertEquals(123, stack.pop());
    }
    @Test
	void DocStoreTestFollowUp() throws URISyntaxException, IOException {
		DocumentStoreImpl store = new DocumentStoreImpl();
		URI uri = new URI("hopeThisWorks");
		DocumentImpl doc = new DocumentImpl(uri, "TEXT");
		store.putDocument(new ByteArrayInputStream("45".getBytes()),uri, DocumentStore.DocumentFormat.TXT);
		store.putDocument(new ByteArrayInputStream("ABC".getBytes()),uri, DocumentStore.DocumentFormat.TXT);
		store.deleteDocument(uri);
		assertNull(store.getDocument(uri));
		store.putDocument(new ByteArrayInputStream("123".getBytes()), uri, DocumentStore.DocumentFormat.TXT);
		doc = new DocumentImpl(uri, "123");
		assertEquals(doc, store.getDocument(uri));
		//Change the top of the command stack
		DocumentImpl doc764 = new DocumentImpl(new URI("764"), "789");
		store.putDocument(new ByteArrayInputStream("789".getBytes()), new URI ("764"), DocumentStore.DocumentFormat.TXT);
		store.undo(uri);
		//Undid the put so should be deleted
		//System.out.println(store.getDocument(uri));
		//assertNull(store.getDocument(uri));
		//should undo the delete
		store.undo(uri);
		assertEquals(new DocumentImpl(uri, "ABC"), store.getDocument(uri));
		store.undo(uri);
		assertEquals(new DocumentImpl(uri, "45"), store.getDocument(uri));
		//Undo the top of the command stack, whcih should get rid of doc764
		store.undo();
		assertNull(store.getDocument(new URI("764")));
	}
/*@Test - never figured this one out
	void DocStoreTest() throws URISyntaxException, IOException {
		DocumentStoreImpl store = new DocumentStoreImpl();
		URI uri1 = new URI("www.tuvwxyz.com");
		String txt = "YAGILU";
		URI uri2 = new URI("www.xyz.com");
		URI uri = new URI("cmpsci4days.com");
		String txt2 = "Up too late at night";
		byte[] pic = "Lots of Pics and Lots of Bytes".getBytes();
		byte [] pic2 = "Even more pix".getBytes();
		DocumentImpl doc1 = new DocumentImpl(uri1, txt);
		DocumentImpl pic1 = new DocumentImpl(uri2, pic2);
		DocumentImpl doc2 = new DocumentImpl(uri1, txt);
		for (int i = 0; i < 30; i++) {
			String str = "";
			str += i + i * 2;
			String str2 = "";
			str2+= i * i;
			URI uri34 = new URI(str2);
			assertEquals(0,store.putDocument(new ByteArrayInputStream(str.getBytes()), uri34, DocumentStore.DocumentFormat.TXT));
		}
		DocumentImpl doc23 = new DocumentImpl(new URI("36"), "18");
		assertEquals(doc23,store.getDocument(new URI("36")));
		store.undo(new URI ("36"));
		assertNull(store.getDocument(new URI("36")));
		URI x = new URI("225");
		DocumentImpl doc225 = new DocumentImpl(x, "45");
		//Change the Value
		assertEquals(doc225.hashCode(), store.putDocument(new ByteArrayInputStream("1".getBytes()),x, DocumentStore.DocumentFormat.TXT));
		//Delete
		store.deleteDocument(x);
		//Make Sure it is gone
		assertNull(store.getDocument(x));
		//Put it back
		store.putDocument(new ByteArrayInputStream("123".getBytes()), x, DocumentStore.DocumentFormat.TXT);
		doc225 = new DocumentImpl(x, "123");
		assertEquals(doc225, store.getDocument(x));
		//Change the top of the command stack
		DocumentImpl doc764 = new DocumentImpl(new URI("764"), "789");
		store.putDocument(new ByteArrayInputStream("789".getBytes()), new URI ("764"), DocumentStore.DocumentFormat.TXT);
		store.undo(x);
		//Undid the put so should be deleted
		assertNull(store.getDocument(x));
		//should undo the delete
		store.undo(x);
		assertEquals(new DocumentImpl(x, "1"), store.getDocument(x));
		store.undo(x);
		assertEquals(new DocumentImpl(x, "45"), store.getDocument(x));
		//Undo the top of the command stack, whcih should get rid of doc764
		store.undo();
		assertNull(store.getDocument(new URI("764")));
	}*/
}
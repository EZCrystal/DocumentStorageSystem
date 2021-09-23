package edu.yu.cs.com1320.project.stage5.impl;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import edu.yu.cs.com1320.project.stage5.Document;
import edu.yu.cs.com1320.project.stage5.PersistenceManager;

import java.io.*;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * created by the document store and given to the BTree via a call to BTree.setPersistenceManager
 */
public class DocumentPersistenceManager implements PersistenceManager<URI, Document> {
    private File dir;
    private Gson gson;

    public DocumentPersistenceManager(File baseDir) {
        if (baseDir == null) {
            this.dir = new File(System.getProperty("user.dir"));
        } else {
            this.dir = baseDir;
        }

        this.gson = new Gson();
    }

    @Override
    public void serialize(URI uri, Document val) throws IOException {
        String jsonData = gson.toJson(val);
        File location = new File(this.dir + File.separator + uri.getHost() + uri.getPath().substring(0, uri.getPath().lastIndexOf(File.separator) + 1));
        Files.createDirectories(Paths.get(String.valueOf(location)));
        File diskData = new File(location, uri.getPath().substring(uri.getPath().lastIndexOf(File.separator) + 1) + ".json");
        FileWriter writer = new FileWriter(diskData);
        writer.write(jsonData);
        writer.close();
    }

    @Override
    public Document deserialize(URI uri) throws IOException {
        File location = new File(this.dir + File.separator + uri.getHost() + uri.getPath().substring(0, uri.getPath().lastIndexOf(File.separator) + 1));
        File diskData = new File(location, uri.getPath().substring(uri.getPath().lastIndexOf(File.separator) + 1) + ".json");
        Scanner reader = new Scanner(diskData);
        String data = reader.nextLine();
        reader.close();
        DocumentImpl doc = gson.fromJson(data, DocumentImpl.class);
        this.delete(uri);
        return doc;
    }

    @Override
    public boolean delete(URI uri) throws IOException {
        File location = new File(this.dir + File.separator + uri.getHost() + uri.getPath().substring(0, uri.getPath().lastIndexOf(File.separator) + 1));
        File diskData = new File(location, uri.getPath().substring(uri.getPath().lastIndexOf(File.separator) + 1) + ".json");
        return diskData.delete();
    }

    /*public static void main (String[] args) throws URISyntaxException, IOException {
        DocumentPersistenceManager test = new DocumentPersistenceManager(null);
        URI uri = new URI("http://www.yu.edu/documents/doc1");
        URI uri2 = new URI("http://www.yu.edu/documents/doc2");
        DocumentImpl doc = new DocumentImpl(uri, new byte[]{0, 1, 1, 0});
        DocumentImpl doc2 = new DocumentImpl(uri, "Please work");
        test.serialize(uri, doc);
        test.serialize(uri2, doc2);
        Document hopefullySameDoc = test.deserialize(uri);
        System.out.println(hopefullySameDoc.getDocumentBinaryData()[0]);
        System.out.println(hopefullySameDoc.getDocumentBinaryData()[1]);
        System.out.println(hopefullySameDoc.getDocumentBinaryData()[2]);
        System.out.println(hopefullySameDoc.getDocumentBinaryData()[3]);
        Document hopefullySameDoc2 = test.deserialize(uri2);
        System.out.println(hopefullySameDoc2.getDocumentTxt());
    }*/
}
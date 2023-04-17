package PCSG.util;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.FSDirectory;

import java.nio.file.Paths;

public class IndexUtil {
    
    public static int countDocuments(String indexPath) {
        int docAmount = 0;
        try {
            IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
            docAmount = reader.maxDoc();
            reader.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
        return docAmount;
    }

    public static String getFieldValue(String indexPath, String idName, int docId, String fieldName) {
        String result = "";
        try {
            IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(Paths.get(indexPath))));
            Query query = IntPoint.newExactQuery(idName, docId);
            ScoreDoc[] docs = searcher.search(query, 1).scoreDocs;
            Document document = searcher.doc(docs[0].doc);
            result = document.get(fieldName);
        }catch (Exception e){
            e.printStackTrace();
        }
        return result;
    }
}

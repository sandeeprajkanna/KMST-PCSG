package PCSG.SnippetGeneration;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import PCSG.PATHS;
import PCSG.util.IndexUtil;
import PCSG.util.ReadFile;
import PCSG.util.StemAnalyzer;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;

public class storeKeys1 {

    // record all isolated nodes in run cases
    public static void recordSingleNodesInCases() {
        List<List<String>> pair = ReadFile.readString(PATHS.ProjectData + "file/CasesWithKeywords.txt", "\t");
        try (PrintWriter writer = new PrintWriter(new FileWriter(PATHS.ProjectData + "file/IsolatedNodesInCases.txt"))) {
            for (List<String> iter : pair) {
                String id = iter.get(0);
                List<String> nodes = ReadFile.readString(PATHS.ProjectData + "KeyKGPWithKeyword/" + id + "/subName.txt");
                if (nodes.size() == 1) {
                    writer.println(id);
                }
            }
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    // pair: which row in QueryPair.txt, start from 0
    private static List<String> getKeysAll(int pair, int dataset, List<String> keywords) throws IOException, ParseException {
        List<String> result = new ArrayList<>();
        File file = new File(PATHS.ProjectData + "SCPResultWithKeyword/" + pair + "-" + dataset + ".txt");
        if (!file.exists()) {
            return result;
        }
        Map<Integer, String> id2lp = new HashMap<>();
        IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(PATHS.ProjectData + "LPIndex/" + dataset + "/")));
        for (int i = 0; i < reader.maxDoc(); i++) {
            Document doc = reader.document(i);
            id2lp.put(Integer.parseInt(doc.get("id")), doc.get("LP"));
        }
        reader.close();
        String componentFolder = PATHS.ProjectData + "ComponentIndex/" + dataset + "/";
        Map<String, Set<Integer>> keyword2Component = new HashMap<>();
        IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(Paths.get(componentFolder))));
        for (String iter: keywords) {
            QueryParser parser = new QueryParser("text", new StemAnalyzer());
            Query query = parser.parse(iter);
            TopDocs docs = searcher.search(query, 100000000);
            ScoreDoc[] scores = docs.scoreDocs;
            Set<Integer> compValue = new HashSet<>();
            for (ScoreDoc score: scores) {
                int id = Integer.parseInt(searcher.doc(score.doc).get("id"));
                compValue.add(id);
            }
            keyword2Component.put(iter, compValue);
        }
        List<List<Integer>> comps  = ReadFile.readInteger(PATHS.ProjectData + "SCPResultWithKeyword/" + pair + "-" + dataset + ".txt", " ");
        for (int comp: comps.get(0)) {
            StringBuilder currentStr = new StringBuilder(dataset + "-" + comp + "\t");
            String edpStr = IndexUtil.getFieldValue(PATHS.ProjectData + "ComponentIndex/" + dataset + "/", "id", comp, "edp");
            edpStr = edpStr.replace(" ", "\t");
            currentStr.append(edpStr.trim()).append("\t");
            String lpIdStr = IndexUtil.getFieldValue(PATHS.ProjectData + "ComponentIndex/" + dataset + "/", "id", comp, "lp");
            if (lpIdStr.length() > 0) {
                for (String lp: lpIdStr.split(" ")) {
                    currentStr.append(id2lp.get(Integer.parseInt(lp))).append("\t");
                }
            }
            for (Map.Entry<String, Set<Integer>> iter: keyword2Component.entrySet()) {
                if (iter.getValue().contains(comp)) {
                    currentStr.append("\"").append(iter.getKey()).append("\"").append("\t");
                }
            }
            result.add(currentStr.toString().trim());
        }
        return result;
    }

    // generate KeysWithKeywords.txt and/or KeysOnlyKeywords.txt
    public static void processAll(int start, int end) {
        List<List<String>> pair = ReadFile.readString(PATHS.FileBase + "file/QueryPair.txt", "\t");
        try (PrintWriter writer = new PrintWriter(PATHS.FileBase + "file/KeysWithKeywords.txt")) { // PATHS.FileBase + "file/KeysOnlyKeywords.txt"
            for (int i = 0; i < pair.size(); i++) {
                List<String> iter = pair.get(i);
                int dataset = Integer.parseInt(iter.get(0));
                if (dataset < start || dataset > end) {
                    continue;
                }
                List<String> keywords = new ArrayList<>(Arrays.asList(iter.get(4).split(" ")));
                List<String> result = getKeysAll(i, dataset, keywords);
//                List<String> result = getKeywords(i, dataset, keywords);
                if (!result.isEmpty()) {
                    for (String s: result) {
                        writer.println(i + "-" + s);
                    }
                    System.out.println("Finish pair " + i + "-" + dataset + ". ");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

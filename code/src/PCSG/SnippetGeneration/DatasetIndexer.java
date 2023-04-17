package PCSG.SnippetGeneration;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.jgrapht.alg.connectivity.ConnectivityInspector;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.Multigraph;
import PCSG.PATHS;
import PCSG.util.*;

import java.io.*;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;

public class DatasetIndexer {

    private static boolean generateComponentIndex(int dataset) {
        Connection connection = new DBUtil().conn;
        try {
            HashMap<Integer, Integer> entity2edp = new HashMap<>();
            String edpPath = PATHS.ProjectData + "Entity2EDP/" + dataset + "/";
            IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(edpPath)));
            for (int i = 0; i < reader.maxDoc(); i++ ){
                Document doc = reader.document(i);
                int entity = Integer.parseInt(doc.get("entity"));
                int edp = Integer.parseInt(doc.get("edpId"));
                entity2edp.put(entity, edp);
            }
            reader.close();
            HashMap<String, Integer> lp2id = new HashMap<>();
            String lpPath = PATHS.ProjectData + "LPIndex/" + dataset + "/";
//            String lpPath = PATHS.wesleyBase + "LPIndex/" + dataset + "/";
            reader = DirectoryReader.open(FSDirectory.open(Paths.get(lpPath)));
            for (int i = 0; i < reader.maxDoc(); i++ ){
                Document doc = reader.document(i);
                int lpId = Integer.parseInt(doc.get("id"));
                lp2id.put(doc.get("LP"), lpId);
            }
            reader.close();
            HashMap<String, Integer> triple2LocalId = new HashMap<>();
            HashMap<Integer, String> localId2lp = new HashMap<>();
            String triplePath = PATHS.ProjectData + "Triple2LP/" + dataset + "/";
//            String triplePath = PATHS.wesleyBase + "Triple2LP/" + dataset + "/";
            reader = DirectoryReader.open(FSDirectory.open(Paths.get(triplePath)));
            for (int i = 0; i < reader.maxDoc(); i++ ){
                Document doc = reader.document(i);
                triple2LocalId.put(doc.get("triple"), -(i+1));
                localId2lp.put(-(i+1), doc.get("lp"));
            }
            reader.close();
            //////////////////////////////////////////////////////////////////////////////////////////////
            String getType = "select type_id from dataset_info_202007 where dataset_local_id = " + dataset;
            String getLabel = "select id, label, is_literal from uri_label_id? where dataset_local_id = ?";
            String select = "select subject, predicate, object from triple? where dataset_local_id = ?";
            PreparedStatement getTypeStatement = connection.prepareStatement(getType);
            PreparedStatement getLabelStatement = connection.prepareStatement(getLabel);
            PreparedStatement getTriple = connection.prepareStatement(select);
            ResultSet resultSet = getTypeStatement.executeQuery();
            resultSet.next();
            int typeID = resultSet.getInt("type_id");
            if (dataset <= 311) {
                getLabelStatement.setInt(1, 2);
                getLabelStatement.setInt(2, dataset);
                getTriple.setInt(1, 2);
                getTriple.setInt(2,  dataset);
            }
            else {
                getLabelStatement.setInt(1, 3);
                getLabelStatement.setInt(2, (dataset - 311));
                getTriple.setInt(1, 3);
                getTriple.setInt(2,  (dataset - 311));
            }
            HashMap<Integer, String> id2Label = new HashMap<>();
            HashSet<Integer> literalSet = new HashSet<>();// for literals
            resultSet = getLabelStatement.executeQuery();
            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                String label = resultSet.getString("label");
                if (label == null) {
                    id2Label.put(id, "");
                }
                else {
                    label = StringUtil.processLabel(resultSet.getString("label")); // the processed label!!!!
                    id2Label.put(id, label);
                }
                if (resultSet.getInt("is_literal") == 1) {
                    literalSet.add(id);
                }
            }
            HashMap<Integer, HashSet<ArrayList<Integer>>> entity2Triple = new HashMap<>(); // including subject and object
            Multigraph<Integer, DefaultEdge> ERgraph = new Multigraph<>(DefaultEdge.class);
            resultSet = getTriple.executeQuery();
            while (resultSet.next()) {
                int sid = resultSet.getInt("subject");
                int pid = resultSet.getInt("predicate");
                int oid = resultSet.getInt("object");
                if (pid == typeID || literalSet.contains(oid)) { // do not need to be added into the graph
                    ERgraph.addVertex(sid);
                    HashSet<ArrayList<Integer>> tempTripleSet = entity2Triple.getOrDefault(sid, new HashSet<>());
                    tempTripleSet.add(new ArrayList<>(Arrays.asList(sid, pid, oid)));
                    entity2Triple.put(sid, tempTripleSet);
                }
                else {
                    int link = triple2LocalId.get(sid + " " + pid + " " + oid);
                    ERgraph.addVertex(sid);
                    ERgraph.addVertex(link);
                    ERgraph.addVertex(oid);
                    ERgraph.addEdge(sid, link);
                    ERgraph.addEdge(link, oid);
                    HashSet<ArrayList<Integer>> tempTripleSet = entity2Triple.getOrDefault(sid, new HashSet<>());
                    tempTripleSet.add(new ArrayList<>(Arrays.asList(sid, pid, oid)));
                    entity2Triple.put(sid, tempTripleSet);
                    tempTripleSet = entity2Triple.getOrDefault(oid, new HashSet<>());
                    tempTripleSet.add(new ArrayList<>(Arrays.asList(sid, pid, oid)));
                    entity2Triple.put(oid, tempTripleSet);
                }
            }
            ConnectivityInspector<Integer, DefaultEdge> inspector = new ConnectivityInspector<>(ERgraph);
            List<Set<Integer>> components = inspector.connectedSets();
            HashMap<Set<Integer>, TreeSet<Integer>> component2edp = new HashMap<>();
            HashMap<Set<Integer>, HashSet<ArrayList<Integer>>> component2triple = new HashMap<>();
            HashMap<Set<Integer>, TreeSet<Integer>> component2lp = new HashMap<>();
            HashMap<Set<Integer>, Integer> component2Size = new HashMap<>();
            for (Set<Integer> comp: components) {
                TreeSet<Integer> edpSet = new TreeSet<>(); //instantiated edps in the component
                HashSet<ArrayList<Integer>> tripleSet = new HashSet<>(); // involved triples in the component
                TreeSet<Integer> lpSet = new TreeSet<>(); // involved lps in the component
                for (int node: comp) {
                    if (entity2edp.containsKey(node)) {
                        edpSet.add(entity2edp.get(node));
                        tripleSet.addAll(entity2Triple.get(node));
                    }
                    if (localId2lp.containsKey(node)) {
                        lpSet.add(lp2id.get(localId2lp.get(node)));
                    }
                }
                component2edp.put(comp, edpSet);
                component2triple.put(comp, tripleSet);
                component2lp.put(comp, lpSet);
                component2Size.put(comp, edpSet.size() + lpSet.size());
            }
            List<Map.Entry<Set<Integer>, Integer>> toBeSortList = new ArrayList<>(component2Size.entrySet());
            Collections.sort(toBeSortList, new Comparator<Map.Entry<Set<Integer>, Integer>>() {
                @Override
                public int compare(Map.Entry<Set<Integer>, Integer> o1, Map.Entry<Set<Integer>, Integer> o2) {
                    return o2.getValue() - o1.getValue();
                }
            });
            String componentFolder = PATHS.ProjectData + "ComponentIndex/" + dataset + "/";
//            String componentFolder = PATHS.wesleyBase + "ComponentIndex/" + dataset + "/";
            File file = new File(componentFolder);
            if (!file.exists()) {
                file.mkdirs();
            }
            Directory directory = FSDirectory.open(Paths.get(componentFolder));
            HashMap<String, Analyzer> perFieldAnalyzer = new HashMap<>();
            perFieldAnalyzer.put("text", new StemAnalyzer()); //Add analyzers for each field
            PerFieldAnalyzerWrapper analyzer = new PerFieldAnalyzerWrapper(new StandardAnalyzer(), perFieldAnalyzer);
            IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
            indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig);
            int count = 0; // the i-th component
            for (Map.Entry<Set<Integer>, Integer> iter: toBeSortList) {
                Set<Integer> comp = iter.getKey();
                HashSet<ArrayList<Integer>> tripleSet = component2triple.get(comp);
                StringBuilder tripleStr = new StringBuilder();
                StringBuilder textStr = new StringBuilder();
                for (ArrayList<Integer> triple: tripleSet) {
                    tripleStr.append(triple.get(0)).append(" ").append(triple.get(1)).append(" ").append(triple.get(2)).append(",");
                    textStr.append(id2Label.get(triple.get(0))).append(" ").append(id2Label.get(triple.get(1))).append(" ").append(id2Label.get(triple.get(2))).append(" ");
                }
                TreeSet<Integer> edpList = component2edp.get(comp);
                StringBuilder edpStr = new StringBuilder();
                for (int edp: edpList) {
                    edpStr.append(edp).append(" ");
                }
                TreeSet<Integer> lpList = component2lp.get(comp);
                StringBuilder lpStr = new StringBuilder();
                for (int lp: lpList) {
                    lpStr.append(lp).append(" ");
                }
                Document document = new Document();
                count++;
                document.add(new IntPoint("id", count));
                document.add(new StoredField("id", count));
                document.add(new TextField("triple", tripleStr.substring(0, tripleStr.length() - 1), Field.Store.YES));
                document.add(new TextField("text", textStr.toString().trim(), Field.Store.NO)); // Field.Store.YES except for 310
                document.add(new TextField("edp", edpStr.toString().trim(), Field.Store.YES));
                document.add(new TextField("lp", lpStr.toString().trim(), Field.Store.YES));
                indexWriter.addDocument(document);
            }
            indexWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            try {
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return true;
    }

    public static void preprocess(int start, int end) {
        List<List<Integer>> datasets = ReadFile.readInteger("src/xxwang/file/dataset.txt", "\t");
//        ArrayList<ArrayList<Integer>> datasets = ReadFile.readInteger(PATHS.FileBase + "file/dataset.txt", "\t");
        for (int dataset: datasets.get(0)) {
            if (dataset < start || dataset > end) {
                continue;
            }
            boolean succ = generateComponentIndex(dataset);
            System.out.println(dataset + "-th dataset: " + succ);
        }
    }

    public static boolean getSetCoverComponents(int dataset) {
        try {
            String edpPath = "D:/Work/ISWC2021Index/EDPIndex/" + dataset + "/";
            IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(edpPath)));
            int edpSize = reader.maxDoc(); // amount of EDPs, id = {1, ..., edpSize}
            reader.close();
            String lpPath = "D:/Work/ISWC2021Index/LPIndex/" + dataset + "/";
            reader = DirectoryReader.open(FSDirectory.open(Paths.get(lpPath)));
            int lpSize = reader.maxDoc(); // amount of LPs, id = {1, ..., lpSize}
            reader.close();
            HashMap<Integer, HashSet<Integer>> component2edp = new HashMap<>();
            HashMap<Integer, HashSet<Integer>> component2lp = new HashMap<>();
            String componentFolder = "D:/Work/ISWC2021Index/ComponentIndex/" + dataset + "/";
            reader = DirectoryReader.open(FSDirectory.open(Paths.get(componentFolder)));
            for (int i = 0; i < reader.maxDoc(); i++){
                Document doc = reader.document(i);
                int id = Integer.parseInt(doc.get("id"));
                HashSet<Integer> edpSet = splitAsInteger(doc.get("edp"), ' ');
                HashSet<Integer> lpSet = splitAsInteger(doc.get("lp"), ' ');
                component2edp.put(id, edpSet);
                component2lp.put(id, lpSet);
            }
            reader.close();
            /**finish preparing sets. */
//            System.out.println(edpSize);////////////////////////////////////////////////////////////////////////////
//            System.out.println(lpSize);
//            System.out.println(component2edp);
//            System.out.println(component2lp);
            ArrayList<Integer> SCPResult = new ArrayList<>();
            SCPResult.add(1);
            HashSet<Integer> coveredEDP = component2edp.get(1);
            HashSet<Integer> coveredLP = component2lp.get(1);
            HashSet<Integer> currentEDP = component2edp.get(1);
            HashSet<Integer> currentLP = component2lp.get(1);
            component2edp.remove(1);
            component2lp.remove(1);
//            System.out.println(coveredEDP);////////////////////////////////////////////////////////////////////////////
//            System.out.println(coveredLP);
//            System.out.println(currentEDP);
//            System.out.println(currentLP);
            while (coveredEDP.size() < edpSize || coveredLP.size() < lpSize) {
//                System.out.print(coveredEDP.size() + " ==== " + edpSize + "  ");
//                System.out.println(coveredLP.size() + " ==== " + lpSize);
                int maxSize = 0;
                int maxComp = 0; // to record the component with the most |edp|+|lp|
                for (Map.Entry<Integer, HashSet<Integer>> iter: component2edp.entrySet()) {
                    int comp = iter.getKey();
                    iter.getValue().removeAll(currentEDP);
                    component2lp.get(comp).removeAll(currentLP);
                    int size = iter.getValue().size() + component2lp.get(comp).size();
                    if (maxSize < size) {
                        maxComp = comp;
                        maxSize = size;
                    }
                }
                SCPResult.add(maxComp);
                currentEDP = component2edp.get(maxComp);
                coveredEDP.addAll(currentEDP);
                currentLP = component2lp.get(maxComp);
                coveredLP.addAll(currentLP);
                component2edp.remove(maxComp);
                component2lp.remove(maxComp);
//                System.out.println(coveredEDP);////////////////////////////////////////////////////////////////////////////
//                System.out.println(coveredLP);
//                System.out.println(currentEDP);
//                System.out.println(currentLP);
            }
            StringBuilder compStr = new StringBuilder();
            for (int iter: SCPResult) {
                compStr.append(iter).append(" ");
            }
            String resultFolder = "D:/Work/ISWC2021Index/SCPResult/" + dataset + ".txt";
            File file = new File(resultFolder);
            if (!file.exists()) {
                file.createNewFile();
            }
            BufferedWriter writer = new BufferedWriter(new FileWriter(file));
            writer.write(compStr.toString().trim());
            writer.write("\n");
            writer.close();
        }catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**Get SCP result for all datasets.
     * @DATE: 2020/11
     */
    public static void process(int start, int end) {
        List<List<Integer>> datasets = ReadFile.readInteger(PATHS.ProjectData + "file/datasetSC.txt", "\t");
//        ArrayList<ArrayList<Integer>> datasets = ReadFile.readInteger(PATHS.FileBase + "file/dataset.txt", "\t");
        for (int dataset: datasets.get(0)) {
            if (dataset < start || dataset > end) {
                continue;
            }
            boolean succ = getSetCoverComponents(dataset);
            System.out.println(dataset + "-th dataset: " + succ);
        }
    }

    /** Used in SCP.
     * @DATE: 2020/11
     * */
    private static HashSet<Integer> splitAsInteger(String str, char regex) {
        HashSet<Integer> result = new HashSet<>();
        String splitStr = "";
        int length = str.length();
        int i = 0, begin = 0;
        for (i = 0; i < length; i++) {
            if (str.charAt(i) == regex) {
                splitStr = str.substring(begin, i);
                result.add(Integer.parseInt(splitStr));
                str = str.substring(i+1, length);
                length = str.length();
                i = 0;
            }
        }
        if (!str.isEmpty()) {
            result.add(Integer.parseInt(str));
        }
        return result;
    }

    /** Elements to be covered: EDP, LP, Keywords
     * @DATE: 2020/11
     * @param dataset : dataset id
     */
    private static boolean getSetCoverComponents(int pair, int dataset, List<String> keywords) {
        try {
            String edpPath = PATHS.ProjectData + "EDPIndex/" + dataset + "/";
            IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(edpPath)));
            int edpSize = reader.maxDoc(); // amount of EDPs, id = {1, ..., edpSize}
            reader.close();
            String lpPath = PATHS.ProjectData + "LPIndex/" + dataset + "/";
            reader = DirectoryReader.open(FSDirectory.open(Paths.get(lpPath)));
            int lpSize = reader.maxDoc(); // amount of LPs, id = {1, ..., lpSize}
            reader.close();
            HashMap<Integer, HashSet<Integer>> component2edp = new HashMap<>();
            HashMap<Integer, HashSet<Integer>> component2lp = new HashMap<>();
            String componentFolder = PATHS.ProjectData + "ComponentIndex/" + dataset + "/";
            reader = DirectoryReader.open(FSDirectory.open(Paths.get(componentFolder)));
            for (int i = 0; i < reader.maxDoc(); i++){
                Document doc = reader.document(i);
                int id = Integer.parseInt(doc.get("id"));
                HashSet<Integer> edpSet = splitAsInteger(doc.get("edp"), ' ');
                HashSet<Integer> lpSet = splitAsInteger(doc.get("lp"), ' ');
                component2edp.put(id, edpSet);
                component2lp.put(id, lpSet);
            }
            reader.close();
            HashMap<Integer, HashSet<Integer>> component2Key = new HashMap<>();
            int keywordSize = 0; //search for keywords
            IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(Paths.get(componentFolder))));
            for (int i = 0; i < keywords.size(); i++) {
                String keyword = keywords.get(i);
                QueryParser parser = new QueryParser("text", new StemAnalyzer());
                Query query = parser.parse(keyword);
                TopDocs docs = searcher.search(query, 100000000);
                ScoreDoc[] scores = docs.scoreDocs;
                if (scores.length == 0) {
                    System.out.println("KEYWORD: " + keyword + " NO HIT!!");
                    continue;
                }
                keywordSize++;
                for (ScoreDoc score : scores) {
                    int id = Integer.parseInt(searcher.doc(score.doc).get("id"));
                    HashSet<Integer> tempValue = component2Key.getOrDefault(id, new HashSet<>());
                    tempValue.add(i);
                    component2Key.put(id, tempValue);
                }
            }
            /**finish preparing sets. */
            ArrayList<Integer> SCPResult = new ArrayList<>();
            HashSet<Integer> coveredEDP = new HashSet<>();
            HashSet<Integer> coveredLP = new HashSet<>();
            HashSet<Integer> coveredKey = new HashSet<>();
            HashSet<Integer> currentEDP = new HashSet<>();
            HashSet<Integer> currentLP = new HashSet<>();
            HashSet<Integer> currentKey = new HashSet<>();
            while (coveredEDP.size() < edpSize || coveredLP.size() < lpSize || coveredKey.size() < keywordSize) {
//                System.out.print(coveredEDP.size() + " ==== " + edpSize + "  ");
//                System.out.println(coveredLP.size() + " ==== " + lpSize);
                int maxSize = 0;
                int maxComp = 0; // to record the component with the most |edp|+|lp|+|Key|
                for (Map.Entry<Integer, HashSet<Integer>> iter: component2edp.entrySet()) {
                    int comp = iter.getKey();
                    iter.getValue().removeAll(currentEDP);
                    component2lp.get(comp).removeAll(currentLP);
                    int size = iter.getValue().size() + component2lp.get(comp).size();
                    if (component2Key.containsKey(comp)) {
                        component2Key.get(comp).removeAll(currentKey);
                        size += component2Key.get(comp).size();
                    }
                    if (maxSize < size) {
                        maxComp = comp;
                        maxSize = size;
                    }
                }
                SCPResult.add(maxComp);
                currentEDP = component2edp.get(maxComp);
                coveredEDP.addAll(currentEDP);
                currentLP = component2lp.get(maxComp);
                coveredLP.addAll(currentLP);
                currentKey = component2Key.getOrDefault(maxComp, new HashSet<>());
                coveredKey.addAll(currentKey);
                component2edp.remove(maxComp);
                component2lp.remove(maxComp);
                component2Key.remove(maxComp);
            }
            StringBuilder compStr = new StringBuilder();
            for (int iter: SCPResult) {
                compStr.append(iter).append(" ");
            }
            String resultFolder = PATHS.ProjectData + "SCPResultWithKeyword/" + pair + "-" + dataset + ".txt";
            File file = new File(resultFolder);
            if (file.exists()) {
                System.out.println("File EXIST!!");
                return false;
            }
            file.createNewFile();
            BufferedWriter writer = new BufferedWriter(new FileWriter(file));
            writer.write(compStr.toString().trim());
            writer.write("\n");
            writer.close();
        }catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**Get SCP result [INCLUDING KEYWORDS!!] for all datasets.
     * @DATE: 2020/11
     */
    public static void processWithKeyword(int start, int end) {
        ArrayList<String> queryPair = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(new File(PATHS.ProjectData + "file/QueryPair.txt")))){
            String line = null;
            while ((line = reader.readLine()) != null) {
                queryPair.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < queryPair.size(); i++) {
            String iter = queryPair.get(i);
            int dataset = Integer.parseInt(iter.split("\t")[0]);
            if (dataset < start || dataset > end || dataset == 311) {
                continue;
            }
            ArrayList<String> keyword = new ArrayList<>(Arrays.asList(iter.split("\t")[4].split(" ")));
            boolean succ = getSetCoverComponents(i, dataset, keyword);
            System.out.println(dataset + "-th dataset: " + succ);
        }
    }

//    public static void main(String[] args) {
//        generateComponentIndex(21);
//        getSetCoverComponents(8);
//        preprocess(0, 309); // 310, 311: on wesley 2020-12-11
//        process(0, 311); // 2020-12-12
//        processWithKeyword(0, 9630);
//    }

}

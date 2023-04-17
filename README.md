# PCSG


## Source Codes and Dependencies

All source codes of implementation are provided in [code/src](https://github.com/nju-websoft/PCSG/tree/main/code/src).

### Dependencies

- JDK 8+
- MySQL 5.6+
- Apache Lucene 7.5.0
- JGraphT 1.3.0

useful packages (jar files) are provided in [code/lib](https://github.com/nju-websoft/PCSG/tree/main/code/lib).

### Run an Example

We provide an example dataset to run our algorithm PCSG, here are the steps:

1. Move [code/example](https://github.com/nju-websoft/PCSG/tree/main/code/example) to your local folder, in which _dataset.txt_ contains the triple of element IDs. Each ID is corresponding to the number of row in _label.txt_ , where the first column shows if this element is a literal, and the second column shows the textual form of the element.
2. Open [src](https://github.com/nju-websoft/PCSG/tree/main/code/src) as a JAVA project. Edit the variable "_folder_" in _src/PCSG/example/DatasetIndexer.java_ to your local folder path.
3. Run all the steps in _DatasetIndex.main()_, to generate index for EDPs, LPs, get set cover components for the dataset, and get the hub labels for the Group Steiner Tree.
4. Edit the folder path in _getResultTree.java_ and run, the edges of the result Group Steiner Trees will be output to terminal.

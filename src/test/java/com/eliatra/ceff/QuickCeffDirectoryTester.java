/* 
 * Copyright (C) 2021 Excelerate Technology Ltd. T/A Eliatra - All Rights Reserved
 * Unauthorized copying, usage or modification of this file in its source or binary form, 
 * via any medium is prohibited.
 * 
 * https://eliatra.com
 */
package com.eliatra.ceff;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.Random;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SuppressForbidden;
import org.junit.Assert;

@SuppressForbidden(reason = "Command line test tool")
public class QuickCeffDirectoryTester {

  private static long errors;
  private static long total;

  public static void main(String[] args) throws Exception {

    if (!QuickCeffDirectoryTester.class.desiredAssertionStatus()) {
      throw new Exception("Please enable assertions (-ea)");
    }

    for (; ; ) {

      total++;

      try {
        test();
      } catch (final Throwable e) {
        System.out.println();
        e.printStackTrace();
        errors++;
        System.out.println();
        System.out.println(
            "   ############################## ERROR #####################################");
        System.out.println();
      }

      System.out.println("Errors so far: " + errors + "/" + total);
    }
  }

  public static void test() throws Exception {

    final byte[] key =
        (new byte[] {
          9, 3, 2, 4, 2, 1, 2, 3, 4, 9, 4, 5, 6, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
          1, 1
        });

    final long seed = System.currentTimeMillis() + System.nanoTime();
    final Random rand = new Random(seed);

    try {
      final StandardAnalyzer analyzer = new StandardAnalyzer();
      final Path path = Files.createTempDirectory("lucenecefftmp");
      path.toFile().deleteOnExit();

      final boolean fast = false;

      final int segments = rand.nextInt(fast ? 9 : 101) + 1; // 101
      final int docsPerSegment = rand.nextInt(fast ? 101 : 10001) + 1; // 10001

      final boolean compoundFile = rand.nextBoolean();
      final int chunkLength = rand.ints(1, 16 * 1024, 500_000).findFirst().getAsInt();
      final int mode = rand.ints(1, 0, 3).findFirst().getAsInt();

      System.out.println(
          new Date()
              + " -------------------------------------------------------------------------------------");
      System.out.println("seed           : " + seed);
      System.out.println("fast mode      : " + fast);
      System.out.println("segments       : " + segments);
      System.out.println("docsPerSegment : " + docsPerSegment);
      System.out.println("compoundFile   : " + compoundFile);
      System.out.println("chunkLength      : " + chunkLength);
      System.out.println("mode           : " + mode);
      final long start = System.currentTimeMillis();

      final Directory index =
          new CeffDirectory(
              new MMapDirectory(path), key, chunkLength, CeffMode.getByModeByte((byte) mode));

      for (int s = 0; s < segments; s++) {

        final IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setUseCompoundFile(compoundFile);

        try (IndexWriter w = new IndexWriter(index, config)) {

          for (int d = 0; d < docsPerSegment; d++) {
            final Document doc = new Document();
            doc.add(
                new TextField(
                    "title", "my fantastic book title " + d + " in seg " + s, Field.Store.YES));
            doc.add(
                new StringField(
                    "comments", "my hilarious comments " + d + " in seg " + s, Field.Store.YES));
            doc.add(
                new TextField("author", "Max Kai Musterman " + d + " in seg " + s, Field.Store.NO));
            doc.add(
                new StringField(
                    "publisher", "the glorious publisher " + d + " in seg " + s, Field.Store.YES));

            for (int k = 0; k < 3; k++) {
              doc.add(new StringField("testfield " + k, "value of " + k, Field.Store.YES));
              doc.add(new IntPoint("testfield int " + k, k));
              doc.add(new StoredField("testfield sf " + k, k));
              doc.add(new NumericDocValuesField("testfield ndv " + k, k));
              doc.add(new NumericDocValuesField("testfield ndv " + k + "-" + d, k));
              doc.add(new StringField("testsdv " + k, "12345", Field.Store.YES));
              doc.add(new SortedDocValuesField("testsdv " + k, new BytesRef("12345")));
            }

            final String id = s + "_" + d;
            doc.add(new StringField("id", id, Field.Store.NO));
            w.addDocument(doc);
          }
        }
      }

      System.out.println("    Indexed " + (segments * docsPerSegment) + " docs");

      final int maxHits = docsPerSegment * segments;
      try (IndexReader reader =
          DirectoryReader.open(index)) { // now we read the data by opening an IndexReader

        Assert.assertEquals(segments * docsPerSegment, reader.numDocs());
        Assert.assertEquals(0, reader.numDeletedDocs());
        Assert.assertEquals(segments * docsPerSegment, reader.maxDoc());

        int numDocsFromLeaves = 0;

        for (final LeafReaderContext lrc : reader.leaves()) {
          numDocsFromLeaves += lrc.reader().numDocs();
          Assert.assertNull(lrc.reader().getLiveDocs());
        }

        Assert.assertEquals(reader.numDocs(), numDocsFromLeaves);

        final Query query = new MatchAllDocsQuery();
        final IndexSearcher searcher = new IndexSearcher(reader);
        final TopScoreDocCollector collector = TopScoreDocCollector.create(maxHits, maxHits);
        searcher.search(query, collector);
        final ScoreDoc[] hits = collector.topDocs().scoreDocs;

        Assert.assertEquals(docsPerSegment * segments, hits.length);

        System.out.println("Found " + hits.length + " hits.");
        for (int i = 0; i < hits.length; ++i) {
          final int docId = hits[i].doc;
          final Document d = searcher.doc(docId);
          Assert.assertTrue(d.get("title").contains("book"));
          Assert.assertTrue(d.get("publisher").contains("glorious"));
        }

        final IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setUseCompoundFile(compoundFile);
        try (IndexWriter w = new IndexWriter(index, config)) {
          w.forceMerge(1, true);
        }

        final long end = System.currentTimeMillis();
        System.out.println(
            "Successful. Took "
                + ((end - start) / 1000d)
                + " sec -> "
                + (hits.length / (end - start) + " docs/ms"));
        System.out.println();
      }
    } catch (final Exception e) {
      throw new Exception("Seed " + seed + ": " + e, e);
    }
  }
}

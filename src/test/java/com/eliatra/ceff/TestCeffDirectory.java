/* 
 * Copyright (C) 2021 Excelerate Technology Ltd. T/A Eliatra - All Rights Reserved
 * Unauthorized copying, usage or modification of this file in its source or binary form, 
 * via any medium is prohibited.
 * 
 * https://eliatra.com
 */
package com.eliatra.ceff;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import org.apache.lucene.analysis.MockAnalyzer;
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
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.BaseDirectoryTestCase;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.English;
import org.apache.lucene.util.TestUtil;
import org.junit.Assert;
import org.junit.Test;

public class TestCeffDirectory extends BaseDirectoryTestCase {

  protected static final byte[] DEFAULT_KEY =
      new byte[] {
        2, 3, 2, 4, 2, 1, 2, 3, 4, 4, 4, 5, 6, 1, 1, 1, 1, 1, 7, 1, 1, 1, 6, 1, 1, 1, 1, 1, 1, 1, 1,
        1
      };

  protected static final byte[] OTHER_KEY =
      new byte[] {
        90, 3, 2, 40, 2, 9, -2, 30, 4, 4, 7, 5, 6, 1, 2, 2, -1, 9, 7, 1, -1, 9, 6, 1, 1, 1, 6, 51,
        1, 1, 1, 1
      };

  protected final byte[] key;
  protected int chunkLength;
  protected final CeffMode mode;

  public TestCeffDirectory(byte[] key, int chunkLength, CeffMode mode, String name) {
    this.key = key;
    this.chunkLength = chunkLength;
    this.mode = mode;
  }

  @Override
  protected Directory getDirectory(Path path) throws IOException {
    return new CeffDirectory(
        new NIOFSDirectory(path),
        this.key,
        this.chunkLength <= 0
            ? this.chunkLength = random().ints(1, 16 * 1024, 400000).findFirst().getAsInt()
            : this.chunkLength,
        this.mode);
  }

  protected Directory getDirectoryOtherKey(Path path) throws IOException {
    return new CeffDirectory(
        new NIOFSDirectory(path),
        OTHER_KEY,
        this.chunkLength <= 0
            ? this.chunkLength = random().ints(1, 16 * 1024, 400000).findFirst().getAsInt()
            : this.chunkLength,
        this.mode);
  }

  @Test
  public void testBuildIndex() throws IOException {
    try (Directory dir = this.getDirectory(createTempDir("testBuildIndex"));
        IndexWriter writer =
            new IndexWriter(
                dir,
                new IndexWriterConfig(new MockAnalyzer(random())).setOpenMode(OpenMode.CREATE))) {
      final int docs = RandomizedTest.randomIntBetween(0, 10);
      for (int i = docs; i > 0; i--) {
        final Document doc = new Document();
        doc.add(newStringField("content", English.intToEnglish(i).trim(), Field.Store.YES));
        writer.addDocument(doc);
      }
      writer.commit();
      assertEquals(docs, writer.getDocStats().numDocs);
    }
  }

  @ParametersFactory(argumentFormatting = "impl=%4$s")
  public static Iterable<Object[]> parametersWithCustomName() {
    return Arrays.asList(
        new Object[][] {
          {DEFAULT_KEY, 16 * 1024, CeffMode.NULL_MODE, "16k - NULL"},
          {DEFAULT_KEY, 128 * 1024, CeffMode.NULL_MODE, "128k - NULL"},
          {DEFAULT_KEY, 16 * 1024, CeffMode.AES_GCM_MODE, "16k - AES"},
          {DEFAULT_KEY, 128 * 1024, CeffMode.AES_GCM_MODE, "128k - AES"},
          {DEFAULT_KEY, -1, CeffMode.AES_GCM_MODE, "random - AES"},
          {DEFAULT_KEY, 16 * 1024, CeffMode.CHACHA20_POLY1305_MODE, "16k - Chacha20"},
          {DEFAULT_KEY, 128 * 1024, CeffMode.CHACHA20_POLY1305_MODE, "128k - Chacha20"},
          {DEFAULT_KEY, -1, CeffMode.CHACHA20_POLY1305_MODE, "random - Chacha20"}
        });
  }

  // file is not directly comparable because we add some extra bytes like ceff header etc
  @Override
  public void testCopyBytes() throws Exception {
    try (Directory dir = this.getDirectory(createTempDir("testCopyBytes"))) {
      IndexOutput out = dir.createOutput("test", newIOContext(random()));
      final byte[] bytes = new byte[TestUtil.nextInt(random(), 1, 77777)];
      final int size = TestUtil.nextInt(random(), 1, 1777777);
      int upto = 0;
      int byteUpto = 0;
      while (upto < size) {
        bytes[byteUpto++] = value(upto);
        upto++;
        if (byteUpto == bytes.length) {
          out.writeBytes(bytes, 0, bytes.length);
          byteUpto = 0;
        }
      }

      out.writeBytes(bytes, 0, byteUpto);
      assertEquals(size, out.getFilePointer());
      out.close();
      final long encryptedFileLength = dir.fileLength("test");
      assertEquals(
          size
              + CeffUtils.calculateEncryptionOverhead(
                  encryptedFileLength, this.chunkLength, this.mode),
          encryptedFileLength);

      // copy from test -> test2
      final IndexInput in = dir.openInput("test", newIOContext(random()));

      out = dir.createOutput("test2", newIOContext(random()));

      upto = 0;
      while (upto < size) {
        if (random().nextBoolean()) {
          out.writeByte(in.readByte());
          upto++;
        } else {
          final int chunk = Math.min(TestUtil.nextInt(random(), 1, bytes.length), size - upto);
          out.copyBytes(in, chunk);
          upto += chunk;
        }
      }
      assertEquals(size, upto);
      out.close();
      in.close();

      // verify
      final IndexInput in2 = dir.openInput("test2", newIOContext(random()));
      upto = 0;
      while (upto < size) {
        if (random().nextBoolean()) {
          final byte v = in2.readByte();
          assertEquals(value(upto), v);
          upto++;
        } else {
          final int limit = Math.min(TestUtil.nextInt(random(), 1, bytes.length), size - upto);
          in2.readBytes(bytes, 0, limit);
          for (int byteIdx = 0; byteIdx < limit; byteIdx++) {
            assertEquals(value(upto), bytes[byteIdx]);
            upto++;
          }
        }
      }
      in2.close();

      dir.deleteFile("test");
      dir.deleteFile("test2");
    }
  }

  public void testWriteRead() throws Exception {
    final StandardAnalyzer analyzer = new StandardAnalyzer();

    final int segments = random().nextInt(4) + 1; // 101
    final int docsPerSegment = random().nextInt(44) + 1; // 10001
    final boolean compoundFile = random().nextBoolean();

    try (Directory dir = this.getDirectory(createTempDir("testWriteRead"))) {

      for (int s = 0; s < segments; s++) {

        final IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setUseCompoundFile(compoundFile);

        try (IndexWriter w = new IndexWriter(dir, config)) {

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

      final int maxHits = docsPerSegment * segments;
      try (IndexReader reader = DirectoryReader.open(dir)) {

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

        for (int i = 0; i < hits.length; ++i) {
          final int docId = hits[i].doc;
          final Document d = searcher.doc(docId);
          Assert.assertTrue(d.get("title").contains("book"));
          Assert.assertTrue(d.get("publisher").contains("glorious"));
        }

        final IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setUseCompoundFile(compoundFile);
        try (IndexWriter w = new IndexWriter(dir, config)) {
          w.forceMerge(1, true);
        }
      }
    }
  }

  public void testTamperedWith() throws Exception {
    final StandardAnalyzer analyzer = new StandardAnalyzer();

    final Path tmpDirPath = createTempDir("testWriteRead");

    try (CeffDirectory dir = (CeffDirectory) this.getDirectory(tmpDirPath)) {

      final IndexWriterConfig config = new IndexWriterConfig(analyzer);

      try (IndexWriter w = new IndexWriter(dir, config)) {

        final Document doc = new Document();
        doc.add(new TextField("title", "my fantastic book title ", Field.Store.YES));
        doc.add(new StringField("comments", "my hilarious comments ", Field.Store.YES));
        doc.add(new TextField("author", "Max Kai Musterman ", Field.Store.NO));
        doc.add(new StringField("publisher", "the glorious publisher ", Field.Store.YES));

        for (int k = 0; k < 5; k++) {
          doc.add(new StringField("testfield " + k, "value of " + k, Field.Store.YES));
          doc.add(new IntPoint("testfield int " + k, k));
          doc.add(new StoredField("testfield sf " + k, k));
          doc.add(new NumericDocValuesField("testfield ndv " + k, k));
          doc.add(new StringField("testsdv " + k, "12345", Field.Store.YES));
          doc.add(new SortedDocValuesField("testsdv " + k, new BytesRef("12345")));
        }

        w.addDocument(doc);
      }
    }

    expectThrows(
        IndexFormatTooOldException.class,
        () -> {
          try (CeffDirectory dir = (CeffDirectory) this.getDirectory(tmpDirPath)) {
            DirectoryReader.open(dir.getDelegate());
          }
        });

    try (CeffDirectory dir = (CeffDirectory) this.getDirectory(tmpDirPath)) {
      final Path file = tmpDirPath.resolve(dir.listAll()[0]);
      this.prepareFile(file);

      try (FileChannel fc =
          FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.SYNC)) {
        assert CeffUtils.calculateNumberOfChunks(fc.size(), this.chunkLength, this.mode) == 1;
        fc.position(fc.size() - (this.mode.getTagLength() + CeffUtils.SIGNATURE_LENGTH) + 5);
        final byte[] rand = new byte[3];
        random().nextBytes(rand);
        fc.write(ByteBuffer.wrap(rand));
        // destroy signature
      }

      expectThrows(
          IOException.class,
          CeffCryptoException.class,
          () -> {
            DirectoryReader.open(dir);
          });

      this.restoreFile(file);
    }

    try (CeffDirectory dir = (CeffDirectory) this.getDirectory(tmpDirPath)) {
      final Path file = tmpDirPath.resolve(dir.listAll()[0]);
      this.prepareFile(file);

      try (FileChannel fc =
          FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.SYNC)) {
        assert CeffUtils.calculateNumberOfChunks(fc.size(), this.chunkLength, this.mode) == 1;
        fc.position(CeffUtils.HEADER_LENGTH);
        final byte[] rand = new byte[3];
        random().nextBytes(rand);
        fc.write(ByteBuffer.wrap(rand));
        // destroy first nonce (or when using null mode first chunk)
      }

      expectThrows(
          IOException.class,
          CeffCryptoException.class,
          () -> {
            DirectoryReader.open(dir);
          });

      this.restoreFile(file);
    }

    try (CeffDirectory dir = (CeffDirectory) this.getDirectory(tmpDirPath)) {
      final Path file = tmpDirPath.resolve(dir.listAll()[0]);
      this.prepareFile(file);

      try (FileChannel fc =
          FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.SYNC)) {
        assert CeffUtils.calculateNumberOfChunks(fc.size(), this.chunkLength, this.mode) == 1;
        fc.position(CeffUtils.HEADER_LENGTH + this.mode.getNonceLength());
        final byte[] rand = new byte[3];
        random().nextBytes(rand);
        fc.write(ByteBuffer.wrap(rand));
        // destroy first chunk
      }

      expectThrows(
          IOException.class,
          CeffCryptoException.class,
          () -> {
            DirectoryReader.open(dir);
          });

      this.restoreFile(file);
    }

    if (this.mode != CeffMode.NULL_MODE) {
      // wrong key
      try (CeffDirectory dir = (CeffDirectory) this.getDirectoryOtherKey(tmpDirPath)) {
        expectThrows(
            IOException.class,
            CeffCryptoException.class,
            () -> {
              DirectoryReader.open(dir);
            });
      }
    }
  }

  private void prepareFile(Path file) throws IOException {
    Files.copy(
        file,
        file.getParent().resolve(file.getFileName() + ".enctmp"),
        StandardCopyOption.REPLACE_EXISTING);
  }

  private void restoreFile(Path file) throws IOException {
    Files.move(
        file.getParent().resolve(file.getFileName() + ".enctmp"),
        file.getParent().resolve(file.getFileName().toString().replace(".enctmp", "")),
        StandardCopyOption.REPLACE_EXISTING);
  }

  private static byte value(int idx) {
    return (byte) ((idx % 256) * (1 + (idx / 256)));
  }
}

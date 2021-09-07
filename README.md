## Lucene CEFF (Chunked encrypted file format)

This library adds modern state-of-the art encryption to Lucene. It wraps a Lucene FSDirectory:

```
Path path = ...;
byte[] key = ...; // 256 bit key
int chunkSize = 64 * 1024;// chunkSize in byte
CeffMode encryptionMode = CeffMode.AES_GCM_MODE;// ChaCha20-Poly1305 is also available

Directory encryptedIndex =
          new CeffDirectory(
              new MMapDirectory(path), key, chunkLength, encryptionMode);

IndexWriter w = new IndexWriter(encryptedIndex, config);
DirectoryReader r = DirectoryReader.open(encryptedIndex);
```

All en-/decryption will be handled transparently. The reading part of the CeffDirectory detects which encryption mode was used, so that also intermixing modes is possible (albeit it makes only sense when migration from one mode to another).

Java 8 required but Java 11+ strongly recommended.

The recommended chunklength is 64kb. 

### Build

Prerequisites:
 * Java 11+
 * Maven 3.6.0+ 

```
./build.sh
```

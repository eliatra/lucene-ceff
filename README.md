## Lucene Chunked Encrypted File Format (CEFF)

Lucene CEFF is an experimental library that adds modern state-of-the art encryption to Lucene data at REST. All files written to and read from disk by Lucene are encrypted and authenticated.

Support for AES-GCM and ChaCha20-Poly1305 is built-in, but encryption schemes are pluggable and extensible.

CEFF works with Lucene 8.0.0+ and Java 8+ (Java 11+ recommended for performance reasons). It has no third-parts dependencies and is easy to use.

**This code is alpha and not yet ready to be used in production.**

## Build

Prerequisites:

* Java 8+
* Maven 3.6.0+ 

CEFF is based on Maven so to build the library either execute

```
./build.sh
```

or 

```
mvn clean package
```

## Usage example

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

Encryption and decryption will be handled transparently. The reading part of the CeffDirectory detects which encryption mode was used, so that also intermixing modes is possible (albeit it makes only sense when migrating from one mode to another).

The recommended chunklength is 64kb. 


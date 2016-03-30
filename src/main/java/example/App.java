package example;

import net.quasardb.qdb.*;
import java.nio.ByteBuffer;

/**
 * An example of a Java program using Quasardb
 */
public class App {
  final static String clusterUri = "qdb://127.0.0.1:2836";
  static QdbCluster db;

  public static void main(String[] args) {
    connectToCluster();
    playWithInteger();
    playWithBlob();
    playWithDeque();
    playWithTag();
    removeEntries();
  }

  static void connectToCluster() {
    try {
      System.out.println("Connecting to " + clusterUri);
      db = new QdbCluster(clusterUri);
    } catch (QdbConnectionRefusedException ex) {
      System.err.println("Failed to connect to " + clusterUri +
                         ", make sure server is running!");
      System.exit(1);
    }
  }

  static void playWithInteger() {
    QdbInteger entrty = db.integer("example.integer");

    System.out.println("Create an integer entry");
    entrty.put(12);

    System.out.println("Replace its value");
    entrty.update(34);

    System.out.println("Increment");
    entrty.add(8);

    System.out.print("Read value: ");
    System.out.println(entrty.get());
  }

  static void playWithBlob() {
    QdbBlob entry = db.blob("example.blob");

    ByteBuffer sample = createByteBuffer("Hello World!");

    System.out.println("Create a blob");
    entry.put(sample);

    System.out.print("Read blob: ");
    QdbBuffer content = entry.get();
    System.out.println(content.toByteBuffer().asCharBuffer());
    content.close();
  }

  static void playWithDeque() {
    QdbDeque entry = db.deque("example.deque");

    ByteBuffer sample1 = createByteBuffer("sample1");
    ByteBuffer sample2 = createByteBuffer("sample2");

    System.out.println("Push to the back of the deque");
    entry.pushBack(sample1);
    entry.pushBack(sample2);

    System.out.println("Read deque in from the front: ");
    QdbBuffer content = entry.popBack();
    while (content != null) {
      System.out.println(content.toByteBuffer().asCharBuffer());
      content.close();
      content = entry.popBack();
    }
  }

  static void playWithTag() {
    QdbTag tag = db.tag("example.tag");

    System.out.println("Tag the integer from the tag handle");
    db.blob("example.integer").addTag(tag);
    db.blob("example.blob").addTag(tag);
    db.deque("example.deque").addTag(tag);

    System.out.println("Enumerate tagged entries:");
    for (QdbEntry entry : tag.getEntries()) {
      System.out.println(entry.alias());
      if (entry instanceof QdbInteger)
        System.out.println("Value is: " + ((QdbInteger)entry).get());
    }
  }

  static void removeEntries() {
    System.out.println("Delete the integer");
    db.integer("example.integer").remove();

    System.out.println("Delete the blob");
    db.blob("example.blob").remove();

    System.out.println("Delete the deque");
    db.deque("example.deque").remove();

    System.out.println("Delete the tag");
    db.tag("example.tag").remove();
  }

  static ByteBuffer createByteBuffer(String content) {
    ByteBuffer bb = ByteBuffer.allocateDirect(content.length() * 2);
    bb.asCharBuffer().put(content);
    return bb;
  }
}

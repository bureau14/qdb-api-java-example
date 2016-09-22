import net.quasardb.qdb.*;
import java.nio.ByteBuffer;

/**
 * An example of a Java program using Quasardb
 */
public class QdbExample {
  final static String clusterUri = "qdb://127.0.0.1:2836";

  public static void main(String[] args) {
    QdbCluster db = connectToCluster();
    playWithInteger(db);
    playWithBlob(db);
    playWithDeque(db);
    playWithTag(db);
    removeEntries(db);
  }

  static QdbCluster connectToCluster() {
    try {
      System.out.println("Connecting to " + clusterUri);
      return new QdbCluster(clusterUri);
    } catch (QdbConnectionRefusedException ex) {
      System.err.println("Failed to connect to " + clusterUri +
                         ", make sure server is running!");
      System.exit(1);
      return null;
    }
  }

  static void playWithInteger(QdbCluster db) {
    QdbInteger entry = db.integer("example.integer");

    System.out.println("Create an integer entry");
    entry.put(12);

    System.out.println("Replace its value");
    entry.update(34);

    System.out.println("Increment");
    entry.add(8);

    System.out.print("Read value: ");
    System.out.println(entry.get());
  }

  static void playWithBlob(QdbCluster db) {
    QdbBlob entry = db.blob("example.blob");

    ByteBuffer sample = createByteBuffer("Hello World!");

    System.out.println("Create a blob");
    entry.put(sample);

    System.out.print("Read blob: ");
    QdbBuffer content = entry.get();
    System.out.println(content.toByteBuffer().asCharBuffer());
    content.close();
  }

  static void playWithDeque(QdbCluster db) {
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

  static void playWithTag(QdbCluster db) {
    QdbTag tag = db.tag("example.tag");

    System.out.println("Tag the integer from the tag handle");
    db.blob("example.integer").attachTag(tag);
    db.blob("example.blob").attachTag(tag);
    db.deque("example.deque").attachTag(tag);

    System.out.println("Enumerate tagged entries:");
    for (QdbEntry entry : tag.entries()) {
      System.out.println(entry.alias());
      if (entry instanceof QdbInteger)
        System.out.println("Value is: " + ((QdbInteger)entry).get());
    }
  }

  static void removeEntries(QdbCluster db) {
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

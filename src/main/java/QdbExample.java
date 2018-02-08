import net.quasardb.qdb.Session;
import net.quasardb.qdb.ts.Table;
import net.quasardb.qdb.ts.Writer;
import net.quasardb.qdb.ts.Reader;
import net.quasardb.qdb.ts.Column;
import net.quasardb.qdb.ts.Value;
import net.quasardb.qdb.ts.Row;
import net.quasardb.qdb.ts.TimeRange;
import net.quasardb.qdb.ts.Timespec;

import net.quasardb.qdb.exception.ConnectionRefusedException;

import java.util.UUID;
import java.util.Random;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * An example of a Java program using Quasardb
 */
public class QdbExample {

    // Use a time range from -1h until +1h
    static final TimeRange[] DEFAULT_RANGE = {
        new TimeRange(Timespec.now(),
                      Timespec.now().plusSeconds(3600))
    };

    public static void main(String[] args) throws IOException {
        Session session = connectToCluster("qdb://127.0.0.1:2836");

        compareWriters(session);
        compareReaders(session);
    }

    static Session connectToCluster(String uri) {
        try {
            return Session.connect(uri);
        } catch (ConnectionRefusedException ex) {
            System.err.println("Failed to connect to " + uri +
                               ", make sure server is running!");
            System.exit(1);
            return null;
        }
    }

    /**
     * This function illustrates that QuasarDB has various buffering strategies
     * for writing data into QuasarDB, and the different semantics that come with
     * each of them.
     *
     * Which one you should use depends upon your use case. When in doubt, however,
     * we recommend using an autoFlushWriter.
     */
    static void compareWriters(Session session) throws IOException {
        System.out.println("== Creating tables ==");
        Table t1 = createTable(session);
        Table t2 = createTable(session);
        Table t3 = createTable(session);
        System.out.println("== Done! ==");
        System.out.println();

        /**
         * Creating a regular Writer using `Table.writer` will initialise a writer
         * that does not perform any automatic flushing at all.
         */
        Writer w1 = Table.writer(session, t1);

        System.out.println("== Writing with regular writer ==");
        writeTable(session, w1, 75000);

        /**
         * As you can see directly after writing, none of the data has been written.
         * At this point all data is still in cache.
         */
        System.out.println("rows in table: " +
                           Table.reader(session,
                                        t1,
                                        DEFAULT_RANGE)
                           .stream()
                           .count());

        /**
         * Only after we explicitly flush the writer, the data becomes visible.
         */
        w1.flush();
        System.out.println("rows in table: " +
                           Table.reader(session,
                                        t1,
                                        DEFAULT_RANGE)
                           .stream()
                           .count());

        System.out.println("== Done! ==");
        System.out.println();

        /**
         * Creating an AutoFlushWriter using `Table.autoFlushWriter` will initialise a
         * writer that periodically flushes the internal buffer with a default buffer size
         * of 50000 rows.
         */
        Writer w2 = Table.autoFlushWriter(session, t2);

        System.out.println("== Writing with auto flush writer ==");
        writeTable(session, w2, 75000);

        /**
         * As you can see directly after 75000 rows, only the first chunk of 50000 rows
         * have been written.
         */
        System.out.println("rows in table: " +
                           Table.reader(session,
                                        t2,
                                        DEFAULT_RANGE).stream().count());

        /**
         * After we either explicitly flush the writer, or write another 25000 rows, the
         * data becomes visible.
         */
        w2.flush();
        System.out.println("rows in table: " +
                           Table.reader(session,
                                        t2,
                                        DEFAULT_RANGE)
                           .stream()
                           .count());

        System.out.println("== Done! ==");
        System.out.println();

        /**
         * Creating an AutoFlushWriter using `Table.autoFlushWriter` with a buffer size
         * of 1 row will initialise a writer that flushes the internal buffer after every
         * write operation.
         *
         * A buffer size of 1 row should typically not be used since performance will suffer,
         * but it does illustrate the configurability of the AutoFlushWriter.
         */
        Writer w3 = Table.autoFlushWriter(session, t3, 1);

        System.out.println("== Writing with auto flush writer with buffer length 1 ==");
        writeTable(session, w3, 1000);
        System.out.println("rows in table: " +
                           Table.reader(session,
                                        t3,
                                        DEFAULT_RANGE).stream().count());


        /**
         * And in this specific case, flushing is unnecessary so we are done here.
         */
        System.out.println("== Done! ==");
        System.out.println();
    }

    static void compareReaders(Session session) throws IOException {
        System.out.println("== Seeding table before read ==");
        Table t = createTable(session);
        Writer w = Table.writer(session, t);
        writeTable(session, w, 100);
        w.flush();
        System.out.println("== Done! ==");
        System.out.println();


        /**
         * The fastest way to traverse over an entire dataset is to use the Reader.
         * It provides an Iterator interface on top of a timeseries table.
         */

        System.out.println("== Reading table as iterator ==");
        Reader r1 = Table.reader(session, t, DEFAULT_RANGE);
        while (r1.hasNext() == true) {
            Row row = r1.next();
            System.out.println(row.toString());
        }
        System.out.println("== Done! ==");
        System.out.println();

        /**
         * It also exposes a Stream interface to allow for a more functional
         * style, introduced in Java 8.
         */
        System.out.println("== Reading table as stream ==");
        Table
            .reader(session, t, DEFAULT_RANGE)
            .stream()
            .forEach(System.out::println);
        System.out.println("== Done! ==");
        System.out.println();



    }

    static void writeTable(Session s, Writer w, long count) throws IOException {
        System.out.println("Writing " + count + " rows to table...!");
        for (long i = 0; i < count; ++i) {
            w.append(randomRow());
        }
    }

    static void readTable(Session s, Reader r) {
        System.out.println("Reading table..., count: " + r.stream().count());

    }

    static Table createTable(Session session) {
        // We generate a random name here, but you're free to use
        // anything you want.
        String name = UUID.randomUUID().toString();

        Column[] columns = {
            new Column.Double("double_val"),
            new Column.Int64("int_val"),
            new Column.Timestamp("ts_val")
        };

        System.out.println("Creating table '" + name + "'");

        return Table.create(session, name, columns);
    }

    /**
     * Utility function that generates a random row that matches the table layout
     * created in createTable.
     */
    static Row randomRow() {
        Random r = new Random();

        Value[] values = {
            Value.createDouble(r.nextDouble()), // double_val
            Value.createInt64(r.nextLong()), // int_val
            Value.createTimestamp(Timespec.now()) // ts_val

        };

        return new Row(Timespec.now(), values);
    }
}

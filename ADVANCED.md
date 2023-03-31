# Writing Your Own Data Sinks

## Rationale

A production program could simply use the convenience SinkFactory.arrays() method in the Deephaven CSV Library,
outputting Java arrays in the manner described in [README.md](README.md). However, if your system defines its own column
data structures, it will be more efficient for the library to write directly to them, rather than first writing to
arrays and then copying the data to its final destination.

Interfacing your own column data structures to the library involves four steps:
1. Wrap each of your data structures with an adaptor class that implements our interface `Sink<TARRAY>`.
2. Write a factory class, implementing our interface `SinkFactory`, that provides these wrapped data structures on 
   demand.
3. If your data structures have a representation for a distinct `NULL` value, write code to support the translation to 
   that value.
4. If you wish to support our fast path for numeric type inference, write additional code to support that optimization.
   This step is optional.

These steps are outlined in more detail below.

## Wrapping Your Data Structures with Adaptor Classes

These are the adaptor classes you need to write (for convenience we will name them `MyXXXSink`, though of course you can
name them whatever you like):

* `class MyByteSink implements Sink<byte[]>`
* `class MyShortSink implements Sink<short[]>`
* `class MyIntSink implements Sink<int[]>`
* `class MyLongSink implements Sink<long[]>`
* `class MyFloatSink implements Sink<float[]>`
* `class MyDoubleSink implements Sink<double[]>`
* `class MyBooleanAsByteSink implements Sink<byte[]>`
* `class MyCharSink implements Sink<char[]>`
* `class MyStringSink implements Sink<String[]>`
* `class MyDateTimeAsLongSink implements Sink<long[]>`
* `class MyTimestampAsLongSink implements Sink<long[]>`

The job of each wrapper class is to:
* Hold a reference to your actual underlying column data structure,
* Implement the `write()` method from `Sink<TARRAY>` in order to copy data to that data structure, and
* Implement the `getUnderlying()` method to give the underlying data structure back to the caller when done.

## Implementing an Adaptor Class

The definition of the `Sink<TARRAY>` interface is:

```java
public interface Sink<TARRAY> {
    void write(TARRAY src, boolean[] isNull, long destBegin,
        long destEnd, boolean appending);
    Object getUnderlying();
}
```

As it is populating a column, the Deephaven CSV Library will repeatedly call `write()` with chunks of data. It is the
job of `write()` to:
* Ensure that the target column data structure has enough capacity.
* Copy the data to the target column data structure.
* If your data structure is capable of representing NULL values, process them appropriately.

There are five arguments to write():
* `src` - the source data. This is a temporary array from which the data should be copied. The data should be copied
  from array index 0; the number of elements to be copied is given by `(destEnd - destBegin)`.
* `isNull` - a parallel array of booleans. If `isNull[i]` is true for some index `i`, this means that the value at
  src[i] should be ignored; instead the corresponding element should be considered to have a null value. We will discuss
  null handling in a later section.
* `destBegin` - the inclusive start index of the destination data range
* `destEnd` - the exclusive end index of the destination data range.
* `appending` - this flag is set to true if the data being written will "grow" the column; i.e., if it is appending data
  beyond the last point where data was written before. If false, the data overwrites a previous range in the column.
  This flag is provided as a convenience: code can derive the same information by comparing `destEnd` to the current
  size of their underlying data structure. Note that some sparse data structures like
  hashtables don't care about the appending/overwriting distinction, but other data structures like `ArrayList` do care.
  Also note that every call will either be fully-appending or fully-overwriting: there is no "straddling" case where
  the library will try to overwrite some data and then append some more within a single call to `write()`.
 
Code should be prepared to accept nonsequential calls to `write()`. Due to the type inference algorithm the system may
write the "suffix" of a column prior to writing its "prefix". For example the system might first append rows 50-99 to a
`Sink<double[]>` and then come back and fill in rows 0-49.

### Example

Here is a sample implementation of `MyIntSink`, using a Java `int[]` array as the underlying data structure.

```java
private static final class MyIntSink implements Sink<int[]> {
    private static final int INITIAL_SIZE = 1024;

    private int[] array;

    public MyIntSink() {
        array = new int[INITIAL_SIZE];
    }

    public void write(int[] src, boolean[] isNull, long destBegin,
                      long destEnd, boolean appending) {
        if (destBegin == destEnd) {
            return;
        }
        final int destBeginAsInt = Math.toIntExact(destBegin);
        final int destEndAsInt = Math.toIntExact(destEnd);
        final int destSize = destEndAsInt - destBeginAsInt;
        
        if (array.length < destEndAsInt) {
            final int highBit = Integer.highestOneBit(destEndAsInt);
            final int newCapacity =
                destEndAsInt == highBit ? highBit : highBit * 2;
            final int[] newArray = new int[newCapacity];
            System.arraycopy(array, 0, newArray, 0, array.length);
            array = newArray;
        }

        // Write chunk to storage.
        System.arraycopy(src, 0, array, destBeginAsInt, destSize);
    }

    public Object getUnderlying() { return array; }
}
```

## Creating a SinkFactory

Once you've written adaptors for all the sink types, you need to create a "sink factory" which provides the adaptors
to the library on demand. To do this, you can either implement our `SinkFactory` interface or call one of its convenient
factory methods. In our example, we would call `SinkFactory.ofSimple` as described below. Also, if you know that a sink
type is unused by your input, you can pass in `null` for that type. For example, if your application doesn't ever read
DateTimes or Timestamps, you might leave those arguments null rather than bothering to implement sinks for them.

```java
private static SinkFactory makeMySinkFactory() {
    return SinkFactory.ofSimple(
        MyByteSink::new,
        MyShortSink::new,
        MyIntSink::new,
        MyLongSink::new,
        MyFloatSink::new,
        MyDoubleSink::new,
        MyBooleanAsByteSink::new,
        MyCharSink::new,
        MyStringSink::new,
        MyDateTimeAsLongSink::new,
        MyTimestampAsLongSink::new);
}
```

## Putting It All Together

We now have everything we need to use our own data structures with the library. We use the same code from
[README.md](README.md) with one small change: we call `makeMySinkFactory()` instead of `SinkFactory.arrays()`:

```java
final InputStream inputStream = ...;
final CsvSpecs specs = CsvSpecs.csv();
final CsvReader.Result result = CsvReader.read(specs, inputStream, makeMySinkFactory); // ** CHANGED **
final long numRows = result.numRows();
for (CsvReader.ResultColumn col : result) {
    switch (col.dataType()) {
        case BOOLEAN_AS_BYTE: {
            byte[] data = (byte[]) col.data();
            // Process this boolean-as-byte column.
            // Be sure to use numRows rather than data.length, because
            // the underlying array might have excess capacity.
            process(data, numRows);
            break;
        }
        case SHORT: {
            short[] data = (short[]) col.data();
            // Process this short column.
            process(data, numRows);
            break;
        }
        // etc...
    }
}
```

## Handling nulls

Although the CSV specification itself doesn't contemplate the concept of "null value", many systems such as Deephaven do
have such a concept, so it makes sense for the library to support it. Typically the caller will configure one or more
null literals (perhaps the empty string or the set {"null", "(null)"}. Then, when the library encounters one of those
null literals in the input text, it will encode it as the appropriate null representation.

What constitutes the "appropriate null representation" is left up to your column implementation. Some
implementations reserve a special sentinel value from the data type. For example, they may use `Integer.MIN_VALUE` to
represent the null value for the integer types. Other implementations keep a boolean flag off to the side which
indicates whether that element is null. Deephaven supports both approaches.

To handle nulls, you will need to modify the `write()` method that we described above. Let's assume that your system
uses `Integer.MIN_VALUE` as a null sentinel for the `int` type. These are the modifications needed for `MyIntSink`;
you would need to do something similar for all your `MyXXXSinks`.

```java
private static final class MyIntSink implements Sink<int[]> {
    private static final int INITIAL_SIZE = 1024;

    private int[] array;

    public MyIntSink() {
        array = new int[INITIAL_SIZE];
    }

    public void write(int[] src, boolean[] isNull, long destBegin,
                      long destEnd, boolean appending) {
        if (destBegin == destEnd) {
            return;
        }
        final int destBeginAsInt = Math.toIntExact(destBegin);
        final int destEndAsInt = Math.toIntExact(destEnd);
        final int destSize = destEndAsInt - destBeginAsInt;

        // *** This is the new null-handling code, which conveniently
        // modifies the source data in place before processing it ***
        for (int i = 0; i < size; ++i) {
            if (isNull[i]) {
                src[i] = Integer.MIN_VALUE;
            }
        }
        // *** End new code ***
        
        if (array.length < destEndAsInt) {
            final int highBit = Integer.highestOneBit(destEndAsInt);
            final int newCapacity =
                destEndAsInt == highBit ? highBit : highBit * 2;
            final int[] newArray = new int[newCapacity];
            System.arraycopy(array, 0, newArray, 0, array.length);
            array = newArray;
        }

        for (int i = 0; i < destSize; ++i) {
            if (isNull[i]) {
                src[i] = Integer.MIN_VALUE;
            }
        }
        
        // Write chunk to storage.
        System.arraycopy(src, 0, array, destBeginAsInt, destSize);
    }

    public Object getUnderlying() { return array; }
}
```

For most purposes the issue of null sentinels (whether or not you use them, and what their specific values are) is a
private detail of your `Sink` implementation. However, there is one important exception: type inference. If you use null
sentinels, the type inference algorithm needs to know what those values are so that it can do the right thing if a
sentinel value appears in the input. For example, say your sentinel null value for `int` is -2147483648, and the input
text "-2147483648" appears in the input. That value should not be considered to be an int, even though it looks like an
int and would normally parse as an int. Instead, it needs to be rejected as an int and instead needs to be interpreted
as the next widest type, namely `long`. Sentinel values are conveyed to the library via the `SinkFactory` interface. For
example, If you are using the factory methods, you can invoke this overload of SinkFactory.ofSimple:

```java
private static SinkFactory makeMySinkFactory() {
    return SinkFactory.ofSimple(
        MyByteSink::new,
        Byte.MIN_VALUE, // your null sentinel value for Byte
        MyShortSink::new,
        Short.MIN_VALUE, // your null sentinel value for Short
        MyIntSink::new,
        Integer.MIN_VALUE, // etc...
        MyLongSink::new,
        Long.MIN_VALUE,
        MyFloatSink::new,
        -Float.MAX_VALUE,
        MyDoubleSink::new,
        -Double.MAX_VALUE,
        MyBooleanAsByteSink::new,
        // No sentinel needed for boolean as byte
        MyCharSink::new,
        Character.MIN_VALUE,
        MyStringSink::new,
        null, // It's uncommon to have a sentinel for String
        MyDateTimeAsLongSink::new,
        Long.MIN_VALUE,
        MyTimestampAsLongSink::new,
        Long.MIN_VALUE);
}
```

## Supporting the Fast Path for Numeric Type Inference

There is an optional optimization available for the four integral sinks (namely `byte`, `short`, `int`, and `long`),
which allows them to support faster type inference at the cost of some additional implementation effort. This
optimization allows the library to read data back from your collection rather than reparsing the input when it needs to
widen the type. To implement it, the corresponding four adaptor classes (`MyByteSink`, `MyShortSink`, `MyIntSink`,
`MyLongSink`) should implement the `Source<TARRAY>` interface as well. Because this is an optional optimization, you
should only implement it if your data structure can easily support it. Also note that this optimization only applies to
those four types. There is no need to implement `Source<TARRAY>` for any of the other sink types (`double`, `String`,
etc).

The definition of `Source<TARRAY>` is:

```java
public interface Source<TARRAY> {
    void read(final TARRAY dest, final boolean[] isNull,
        final long srcBegin, final long srcEnd);
}
```


When it is reading back data from a column, the library will repeatedly call `read()` to get chunks of data. It is the
job of `read()` to:

* Copy the data from the column data structure.
* If your data structure is capable of representing null values, process them appropriately.

These are the four arguments to `read`:
* `dest` - the destination data. This is a temporary array to which the data should be copied. The data should be copied
  starting at array index 0, and the number of elements to be copied is given by `(destEnd - destBegin)`.
* `isNull` - a parallel array of booleans. If nulls are supported, the implementor should set `isNull[i]` to `true` for
  each element that represents a null value, otherwise it should set it to false. If `isNull[i]` is true, then the
  corresponding value in `dest[i]` will be ignored.
* `srcBegin`  - the inclusive start index of the source data range
* `srcEnd` - the exclusive end index of the source data range. The library promises to only read from elements that it
  has previously written to.

What follows is a complete implementation of `MyIntSink`, including a `Source<int[]>` implementation and null value
handling:

```java
private static final class MyIntSink implements Sink<int[]>, Source<int[]> {
    private static final int INITIAL_SIZE = 1024;

    private int[] array;

    public MyIntSink() {
        array = new int[INITIAL_SIZE];
    }

    public void write(int[] src, boolean[] isNull, long destBegin,
                      long destEnd, boolean appending) {
        if (destBegin == destEnd) {
            return;
        }
        final int destBeginAsInt = Math.toIntExact(destBegin);
        final int destEndAsInt = Math.toIntExact(destEnd);
        final int destSize = destEndAsInt - destBeginAsInt;
        
        if (array.length < destEndAsInt) {
            final int highBit = Integer.highestOneBit(destEndAsInt);
            final int newCapacity =
                destEndAsInt == highBit ? highBit : highBit * 2;
            final int[] newArray = new int[newCapacity];
            System.arraycopy(array, 0, newArray, 0, array.length);
            array = newArray;
        }

        for (int i = 0; i < destSize; ++i) {
            if (isNull[i]) {
                src[i] = Integer.MIN_VALUE;
            }
        }
        
        // Write chunk to storage.
        System.arraycopy(src, 0, array, destBeginAsInt, destSize);
    }


    // *** New code here: implement the read() method from Source<int[]> ***
    @Override
    public void read(int[] dest, boolean[] isNull, long srcBegin,
            long srcEnd) {
        if (srcBegin == srcEnd) {
            return;
        }
        final int srcBeginAsInt = Math.toIntExact(srcBegin);
        final int srcSize = Math.toIntExact(srcEnd - srcBegin);
        System.arraycopy(array, srcBeginAsInt, dest, 0, srcSize);
        for (int ii = 0; ii < srcSize; ++ii) {
            isNull[ii] = dest[ii] == Integer.MIN_VALUE;
        }
    }
    // *** End new code ***

    public Object getUnderlying() { return array; }
}
```

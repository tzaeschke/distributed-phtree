package ch.ethz.globis.distindex.operation;

public class OpCode {

    public static final byte CREATE_INDEX = 1;
    public static final byte PUT = 10;
    public static final byte DELETE = 11;
    public static final byte GET = 20;
    public static final byte GET_RANGE = 21;
    public static final byte GET_KNN = 22;
    public static final byte GET_BATCH = 23;
    public static final byte GET_SIZE = 24;
    public static final byte GET_DIM = 25;
    public static final byte GET_DEPTH = 26;
    public static final byte CLOSE_ITERATOR = 27;
    public static final byte CONTAINS = 28;

    public static final byte BALANCE_INIT = 30;
    public static final byte BALANCE_PUT = 31;
    public static final byte BALANCE_COMMIT = 32;
}
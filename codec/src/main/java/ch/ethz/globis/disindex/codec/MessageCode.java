package ch.ethz.globis.disindex.codec;

public class MessageCode {

    public static final byte PUT = 1;
    public static final byte GET = 2;
    public static final byte GET_RANGE = 3;
    public static final byte GET_KNN = 4;

    public static final byte CREATE_INDEX = 5;

    public static final byte SUCCESS = 6;
}
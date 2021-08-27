package com.wyb.spark.lineage.linkis.utils;


import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;


public interface Fs extends Closeable {

    public abstract void init(Map<String, String> properties) throws IOException;

    public abstract String fsName();

    String rootUserName();

    FsPath get(String dest) throws IOException;

    public abstract InputStream read(FsPath dest) throws IOException;

    public abstract OutputStream write(FsPath dest, boolean overwrite) throws IOException;

    boolean create(String dest) throws IOException;

    List<FsPath> list(final FsPath path) throws IOException;

    public abstract boolean canRead(FsPath dest) throws IOException;

    public abstract boolean canWrite(FsPath dest) throws IOException;

    public abstract boolean exists(FsPath dest) throws IOException;

    public abstract boolean delete(FsPath dest) throws IOException;

    public abstract boolean renameTo(FsPath oldDest, FsPath newDest) throws IOException;

}
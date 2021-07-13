/*
 * Copyright 2019 Jörg Eichhorn <joerg@kiwigrid.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package hdfs.jsr203;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileLock;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.FileSystem;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * created on 07.05.19.
 *
 * @author Jörg Eichhorn {@literal <joerg.eichhorn@kiwigrid.com>}
 */
public class HadoopAsynchronousFileChannelImpl extends AsynchronousFileChannel {

    private static final String FILE_LOCKING_NOT_SUPPORTED = "File locking not supported by hadoop filesystem.";

    private final HadoopPath file;
    private final ExecutorService executor;
    private final HadoopFileSystem hdfs;
    private final SeekableByteChannel byteChannel;

    HadoopAsynchronousFileChannelImpl(Path file, Set<? extends OpenOption> options, ExecutorService executor, FileAttribute<?>... attrs) throws IOException {
        if (!(file instanceof HadoopPath)) {
            throw new IllegalStateException("Path has to be hadoop path");
        }
        this.file = (HadoopPath) file;
        this.executor = executor;
        FileSystem fileSystem = file.getFileSystem();
        if (!(fileSystem instanceof HadoopFileSystem)) {
            throw new IllegalStateException("FileSystem has to be hadoop filesystem");
        }
        hdfs = (HadoopFileSystem) fileSystem;
        checkOpenFileSystem();
        byteChannel = this.file.newByteChannel(options, attrs);
    }

    private void checkOpenFileSystem() {
        if (!hdfs.isOpen()) {
            throw new IllegalStateException("Hadoop FileSystem not open");
        }
    }

    @Override
    public long size() throws IOException {
        return file.getAttributes().size();
    }

    @Override
    public AsynchronousFileChannel truncate(long size) {
        throw new UnsupportedOperationException("Truncation not supported by hadoop filesystem.");
    }

    @Override
    public void force(boolean metaData) {
        // no-op
    }

    @Override
    public <A> void lock(long l, long l1, boolean b, A a, CompletionHandler<FileLock, ? super A> completionHandler) {
        throw new UnsupportedOperationException(FILE_LOCKING_NOT_SUPPORTED);
    }

    @Override
    public Future<FileLock> lock(long l, long l1, boolean b) {
        throw new UnsupportedOperationException(FILE_LOCKING_NOT_SUPPORTED);
    }

    @Override
    public FileLock tryLock(long l, long l1, boolean b) {
        throw new UnsupportedOperationException(FILE_LOCKING_NOT_SUPPORTED);
    }

    @Override
    public <A> void read(ByteBuffer dst, long position, A attachment, CompletionHandler<Integer,? super A> handler) {
        implRead(dst, position)
                .handle((integer, throwable) -> {
                    if (throwable != null) {
                        handler.failed(throwable, attachment);
                    } else {
                        handler.completed(integer, attachment);
                    }
                    return null;
                });
    }

    @Override
    public Future<Integer> read(ByteBuffer byteBuffer, long position) {
        return implRead(byteBuffer, position);
    }

    private CompletableFuture<Integer> implRead(final ByteBuffer dst, final long position) {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        if (position < 0L) {
            throw new IllegalArgumentException("Negative position");
        } else if (dst.isReadOnly()) {
            throw new IllegalArgumentException("Read-only buffer");
        } else {
            try {
                if (position != byteChannel.position()) {
                    throw new IllegalArgumentException("Cannot seek into hadoop file");
                } else {
                    Runnable task = () -> {
                        try {
                            int read = byteChannel.read(dst);
                            future.complete(read);
                        } catch (Exception e) {
                            future.completeExceptionally(e);
                        }
                    };
                    this.executor.execute(task);
                }
            } catch (IOException e) {
                future.completeExceptionally(e);
            }
            return future;
        }
    }

    @Override
    public <A> void write(ByteBuffer byteBuffer, long position, A attachement, CompletionHandler<Integer, ? super A> handler) {
        implWrite(byteBuffer, position).handle((integer, throwable) -> {
            if (throwable != null) {
                handler.failed(throwable, attachement);
            } else {
                handler.completed(integer, attachement);
            }
            return null;
        });
    }

    @Override
    public Future<Integer> write(ByteBuffer byteBuffer, long position) {
        return implWrite(byteBuffer, position);
    }

    private CompletableFuture<Integer> implWrite(final ByteBuffer src, final long position) {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        if (position < 0L) {
            throw new IllegalArgumentException("Negative position");
        } else if (this.isOpen() && src.remaining() != 0){
            try {
                if (position != byteChannel.position()) {
                    throw new IllegalArgumentException("Cannot seek into hadoop file");
                } else {
                    Runnable task = () -> {
                        try {
                            int written = byteChannel.write(src);
                            future.complete(written);
                        } catch (Exception e) {
                            future.completeExceptionally(e);
                        }
                    };
                    this.executor.execute(task);
                }
            } catch (IOException e) {
                future.completeExceptionally(e);
            }
        }
        return future;
    }


    @Override
    public boolean isOpen() {
        return byteChannel.isOpen();
    }

    @Override
    public void close() throws IOException {
        byteChannel.close();
    }
}

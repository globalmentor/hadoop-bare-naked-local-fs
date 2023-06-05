package org.apache.hadoop.fs.local;

import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem;
import org.apache.hadoop.fs.DelegateToFileSystem;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Required to handle streaming and checkpointing
 *
 * fs.AbstractFileSystem.file.impl
 * spark.hadoop.fs.AbstractFileSystem.file.impl
 *
 */
public class BareStreamingLocalFileSystem extends DelegateToFileSystem {
    public BareStreamingLocalFileSystem(java.net.URI uri, org.apache.hadoop.conf.Configuration conf) throws IOException, URISyntaxException {
        super(uri, new BareLocalFileSystem(), conf, "file", false);
    }
}
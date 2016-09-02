package com.ryanberg.alfresco.s3;


import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import org.alfresco.repo.content.AbstractContentReader;
import org.alfresco.service.cmr.repository.ContentIOException;
import org.alfresco.service.cmr.repository.ContentReader;
import org.alfresco.util.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

public class S3ContentReader extends AbstractContentReader {

    private static final Log logger = LogFactory.getLog(S3ContentReader.class);

    private String key;
    private AmazonS3 client;
    private String bucketName;
    private S3Object fileObject;
    private ObjectMetadata fileObjectMetadata;

    /**
     * @param client
     * @param contentUrl the content URL - this should be relative to the root of the store
     * @param bucketName
     */
    protected S3ContentReader(String key, String contentUrl, AmazonS3 client, String bucketName) {
        super(contentUrl);
        this.key = key;
        this.client = client;
        this.bucketName = bucketName;
        this.fileObject = getObject();
        this.fileObjectMetadata = getObjectMetadata(this.fileObject);
    }

    @Override
    protected void finalize() throws Throwable {
        this.fileObject.close();
        super.finalize();
    }

    @Override
    protected ContentReader createReader() throws ContentIOException {

        logger.debug("Called createReader for contentUrl -> " + getContentUrl() + ", Key: " + key);
        return new S3ContentReader(key, getContentUrl(), client, bucketName);
    }

    @Override
    protected ReadableByteChannel getDirectReadableChannel() throws ContentIOException {

        if(!exists()) {
            throw new ContentIOException("Content object does not exist on S3");
        }

        try {
            final ReadableByteChannel ch0 = Channels.newChannel(fileObject.getObjectContent());
            return new ReadableByteChannel() {
                @Override
                public int read(ByteBuffer dst) throws IOException {
                    return ch0.read(dst);
                }

                @Override
                public boolean isOpen() {
                    return ch0.isOpen();
                }

                @Override
                public void close() throws IOException {
                    ch0.close();
                    fileObject.close();
                }
            };
        } catch ( Exception e ) {
            throw new ContentIOException("Unable to retrieve content object from S3", e);
        }

    }

    @Override
    public boolean exists() {
        return fileObjectMetadata != null;
    }

    @Override
    public long getLastModified() {

        if(!exists()) {
            return 0L;
        }

        return fileObjectMetadata.getLastModified().getTime();

    }

    @Override
    public long getSize() {

        if(!exists()) {
            return 0L;
        }

        return fileObjectMetadata.getContentLength();
    }

    private S3Object getObject() {

        S3Object object = null;

        try {
            logger.debug("GETTING OBJECT - BUCKET: " + bucketName + " KEY: " + key);
            object = client.getObject(bucketName, key);
        } catch (Exception e) {
            logger.error("Unable to fetch S3 Object", e);
        }

        return object;
    }

    private ObjectMetadata getObjectMetadata(S3Object object) {

        ObjectMetadata metadata = null;

        if(object != null) {
            metadata = object.getObjectMetadata();
        }

        return metadata;

    }
}

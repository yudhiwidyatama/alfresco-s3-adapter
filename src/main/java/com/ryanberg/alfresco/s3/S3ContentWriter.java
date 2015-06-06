package com.ryanberg.alfresco.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.transfer.TransferManager;
import org.alfresco.repo.content.AbstractContentWriter;
import org.alfresco.service.cmr.repository.ContentIOException;
import org.alfresco.service.cmr.repository.ContentReader;
import org.alfresco.util.GUID;
import org.alfresco.util.TempFileProvider;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;


public class S3ContentWriter extends AbstractContentWriter {

    private static final Log logger = LogFactory.getLog(S3ContentWriter.class);

    private TransferManager transferManager;
    private AmazonS3 client;
    private String contentUrl;
    private String bucketName;
    private File tempFile;
    private long size;

    public S3ContentWriter(String contentUrl, ContentReader existingContentReader, AmazonS3 client, TransferManager transferManager, String bucketName) {
        super(contentUrl, existingContentReader);
        this.client = client;
        this.transferManager = transferManager;
        this.contentUrl = contentUrl;
        this.bucketName = bucketName;
        addListener(new S3StreamListener(this));
    }

    @Override
    protected ContentReader createReader() throws ContentIOException {
        return new S3ContentReader(getContentUrl(), client, bucketName);
    }

    @Override
    protected WritableByteChannel getDirectWritableChannel() throws ContentIOException {

        try
        {

            String uuid = GUID.generate();
            logger.debug("S3ContentWriter Creating Temp File: uuid="+uuid);
            tempFile = TempFileProvider.createTempFile(uuid, ".bin");
            OutputStream os = new FileOutputStream(tempFile);
            logger.debug("S3ContentWriter Returning Channel to Temp File: uuid="+uuid);
            return Channels.newChannel(os);
        }
        catch (Throwable e)
        {
            throw new ContentIOException("S3ContentWriter.getDirectWritableChannel(): Failed to open channel. " + this, e);
        }

    }

    @Override
    public long getSize() {
        return this.size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    @Override
    public String getContentUrl() {
        return contentUrl;
    }

    public TransferManager getTransferManager() {
        return transferManager;
    }

    public AmazonS3 getClient() {
        return client;
    }

    public String getBucketName() {
        return bucketName;
    }

    public File getTempFile() {
        return tempFile;
    }
}

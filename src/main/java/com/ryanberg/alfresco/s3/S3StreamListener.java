package com.ryanberg.alfresco.s3;

import com.amazonaws.services.s3.transfer.TransferManager;
import org.alfresco.service.cmr.repository.ContentIOException;
import org.alfresco.service.cmr.repository.ContentStreamListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;

public class S3StreamListener implements ContentStreamListener {

    private static final Log logger = LogFactory.getLog(S3StreamListener.class);

    private S3ContentWriter writer;

    public S3StreamListener(S3ContentWriter writer) {

        this.writer = writer;

    }

    @Override
    public void contentStreamClosed() throws ContentIOException {

        File file = writer.getTempFile();
        long size = file.length();
        writer.setSize(size);

        try {

            TransferManager transferManager = writer.getTransferManager();
            transferManager.upload(writer.getBucketName(), writer.getContentUrl(), writer.getTempFile());

        } catch (Exception e) {
            logger.error("S3StreamListener Failed to Upload File", e);
        }

    }
}

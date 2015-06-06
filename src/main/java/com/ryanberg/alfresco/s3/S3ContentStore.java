package com.ryanberg.alfresco.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.transfer.TransferManager;
import org.alfresco.repo.content.AbstractContentStore;
import org.alfresco.repo.content.ContentStore;
import org.alfresco.repo.content.ContentStoreCreatedEvent;
import org.alfresco.repo.content.filestore.FileContentStore;
import org.alfresco.service.cmr.repository.ContentReader;
import org.alfresco.service.cmr.repository.ContentWriter;
import org.alfresco.util.GUID;
import org.apache.commons.lang3.StringUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.Map;


public class S3ContentStore extends AbstractContentStore
        implements ApplicationContextAware, ApplicationListener<ApplicationEvent> {

    private static final Log logger = LogFactory.getLog(S3ContentStore.class);
    private ApplicationContext applicationContext;

    private AmazonS3 s3Client;
    private TransferManager transferManager;

    private String accessKey;
    private String secretKey;
    private String bucketName;
    private String regionName;

    @Override
    public boolean isWriteSupported() {
        return true;
    }

    @Override
    public ContentReader getReader(String contentUrl) {

        return new S3ContentReader(contentUrl, s3Client, bucketName);

    }

    public void init() {

        AWSCredentials credentials = null;

        if(StringUtils.isNotBlank(this.accessKey) && StringUtils.isNotBlank(this.secretKey)) {

            logger.debug("Found credentials in properties file");
            credentials = new BasicAWSCredentials(this.accessKey, this.secretKey);

        } else {
            try {
                logger.debug("AWS Credentials not specified in properties, will fallback to credentials provider");
                credentials = new ProfileCredentialsProvider().getCredentials();
            } catch (Exception e) {
                logger.error("Can not find AWS Credentials");
            }
        }

        s3Client = new AmazonS3Client(credentials);
        Region region = Region.getRegion(Regions.fromName(this.regionName));
        s3Client.setRegion(region);
        transferManager = new TransferManager(s3Client);
    }


    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public void setRegionName(String regionName) {
        this.regionName = regionName;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }


    @Override
    protected ContentWriter getWriterInternal(ContentReader existingContentReader, String newContentUrl) {

        String contentUrl = newContentUrl;

        if(StringUtils.isBlank(contentUrl)) {
            contentUrl = createNewUrl();
        }

        return new S3ContentWriter(contentUrl, existingContentReader, s3Client, transferManager, bucketName);

    }

    public static String createNewUrl() {

        Calendar calendar = new GregorianCalendar();
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH) + 1;  // 0-based
        int day = calendar.get(Calendar.DAY_OF_MONTH);
        int hour = calendar.get(Calendar.HOUR_OF_DAY);
        int minute = calendar.get(Calendar.MINUTE);
        // create the URL
        StringBuilder sb = new StringBuilder(20);
        sb.append(FileContentStore.STORE_PROTOCOL)
                .append(ContentStore.PROTOCOL_DELIMITER)
                .append(year).append('/')
                .append(month).append('/')
                .append(day).append('/')
                .append(hour).append('/')
                .append(minute).append('/')
                .append(GUID.generate()).append(".bin");
        String newContentUrl = sb.toString();
        // done
        return newContentUrl;

    }

    @Override
    public boolean delete(String contentUrl) {

        try {
            logger.debug("Deleting object from S3 with url: " + contentUrl);
            s3Client.deleteObject(bucketName, contentUrl);
            return true;
        } catch (Exception e) {
            logger.error("Error deleting S3 Object", e);
        }

        return false;

    }





    /**
     * Publishes an event to the application context that will notify any interested parties of the existence of this
     * content store.
     *
     * @param context
     *            the application context
     * @param extendedEventParams
     */
    private void publishEvent(ApplicationContext context, Map<String, Serializable> extendedEventParams)
    {
        context.publishEvent(new ContentStoreCreatedEvent(this, extendedEventParams));
    }

    public void onApplicationEvent(ApplicationEvent event)
    {
        // Once the context has been refreshed, we tell other interested beans about the existence of this content store
        // (e.g. for monitoring purposes)
        if (event instanceof ContextRefreshedEvent && event.getSource() == this.applicationContext)
        {
            publishEvent(((ContextRefreshedEvent) event).getApplicationContext(), Collections.<String, Serializable> emptyMap());
        }
    }
}

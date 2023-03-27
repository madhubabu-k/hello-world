/**
 * 
 */
package com.pearson.ps.ingest.module;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.pearson.ps.ingest.creator.ProductFolderCreator;
import com.pearson.ps.ingest.creator.ProductFolderCreatorImpl;
import com.pearson.ps.ingest.helper.ManifestHelper;
import com.pearson.ps.ingest.helper.ManifestHelperImpl;
import com.pearson.ps.ingest.main.IngestJob;
import com.pearson.ps.ingest.main.IngestJobImpl;
import com.pearson.ps.ingest.provider.IngestJobConfigProvider;
import com.pearson.ps.ingest.provider.IngestJobConfigProviderImpl;
import com.pearson.ps.ingest.provider.CachedDataProvider;
import com.pearson.ps.ingest.provider.CachedDataProviderImpl;
import com.pearson.ps.ingest.provider.GenericPropertiesProvider;
import com.pearson.ps.ingest.provider.IngestLogProvider;
import com.pearson.ps.ingest.provider.IngestLogProviderImpl;
import com.pearson.ps.ingest.provider.MailNotificationProvider;
import com.pearson.ps.ingest.provider.MailNotificationProviderImpl;
import com.pearson.ps.ingest.provider.MessagesPropertiesProviderImpl;

/**
 * @author Manav Leslie
 *
 */
public class IngestModule extends AbstractModule {


	private static final Logger LOGGER = LoggerFactory.getLogger(IngestModule.class);	
	
	/**
	 * Uses Google Guice For dependency Injection 
	 * @see <a href="http://code.google.com/p/google-guice/">Google Guice</a>
	 * Configuring the dependencies which will be injected. 
	 */
	@Override
	protected void configure() {
		LOGGER.info("Configuring the ingest module ");
		//Following are created as singleton 
		bind(IngestJobConfigProvider.class).to(IngestJobConfigProviderImpl.class).in(Singleton.class);
		bind(IngestLogProvider.class).to(IngestLogProviderImpl.class).in(Singleton.class);
		bind(CachedDataProvider.class).to(CachedDataProviderImpl.class).in(Singleton.class);
		bind(GenericPropertiesProvider.class).to(MessagesPropertiesProviderImpl.class).in(Singleton.class);
		
		
		bind(ProductFolderCreator.class).to(ProductFolderCreatorImpl.class);
		bind(ManifestHelper.class).to(ManifestHelperImpl.class);
		bind(IngestJob.class).to(IngestJobImpl.class);
		bind(MailNotificationProvider.class).to(MailNotificationProviderImpl.class);
		
		
	}

}

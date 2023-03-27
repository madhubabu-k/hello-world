package com.pearson.ps.ingest.main;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.pearson.ps.ingest.module.IngestModule;

/**
 * Main class called by the ingest job
 * 
 * @author Manav Leslie
 * 
 */
public final class InitiateIngestJob {

	/**
	 *Private constructor to prevent creating instances
	 */
	private InitiateIngestJob() {
	}

	private static final Logger LOGGER = LoggerFactory
			.getLogger(InitiateIngestJob.class);
	public static String bcc = "";
	public static String seperator = "";

	public static void main(String[] args) {
		LOGGER.info("Starting the Ingest Process ");
		// Create Injector using the IngestModule
		int iArgCnt = args.length;
		switch (iArgCnt) {
		case 0:
			LOGGER
					.warn("main() - no BCC mail recipients is passed to Ingest Main method. Check IngestProduct.sh to configure");
			break;
		case 1:
			bcc = args[0];
			LOGGER.info("main() - m_BCC : {}", bcc);
			break;
		case 2:
			bcc = args[0];
			seperator = args[1];
			LOGGER.info("main() - m_BCC : {}", bcc);
			LOGGER.info("main() - m_Seperator : {}", seperator);
			break;
		default:
			LOGGER
					.warn("main() - Wrong no. of arguments are passed to Ingest Main method so no BCC mail recipients could be passed to Ingest Process. Check IngestProduct.sh to configure");
			break;
		}
		Injector injector = Guice.createInjector(new IngestModule());
		IngestJob ingestJob = injector.getInstance(IngestJob.class);
		ingestJob.startIngest();
	}

}

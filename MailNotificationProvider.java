package com.pearson.ps.ingest.provider;

import java.io.IOException;

import com.documentum.fc.common.DfException;
import com.pearson.ps.ingest.exception.IngestException;

public interface MailNotificationProvider {

	void mailLogFile(String formReqNo, StringBuffer logFileStrBuffer) throws IngestException, DfException, IOException;

}
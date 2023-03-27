/**
 * 
 */
package com.pearson.ps.ingest.provider;

/**
 * @author Manav Leslie
 *
 */
public class IngestLogProviderImpl implements IngestLogProvider {
	
	private StringBuffer mainStrBuffer;
	private StringBuffer footerStrBuffer;
	private StringBuffer failedFilesStrBuffer;
	
	private StringBuffer tempProductBuffer;
	private StringBuffer successProductStrBuffer;
	private StringBuffer failedProductStrBuffer;

	/**
	 * @param mainStrBuffer
	 */
	public IngestLogProviderImpl() {
		mainStrBuffer  = new StringBuffer();
		footerStrBuffer = new StringBuffer();
		failedFilesStrBuffer = new StringBuffer();
		
		tempProductBuffer = new StringBuffer();
		successProductStrBuffer = new StringBuffer();
		failedProductStrBuffer = new StringBuffer();
	}

	/* 
	 * @see com.pearson.ps.ingest.helper.IIngestLogProvider#appendMessage(java.lang.String)
	 */
	@Override
	public void appendMainMessage(String message) {
		this.mainStrBuffer.append(message).append("\n");

	}


	/* 
	 * @see com.pearson.ps.ingest.helper.IIngestLogProvider#returnMessage()
	 */
	@Override
	public StringBuffer returnConsolidatedMessage() {
		return mainStrBuffer.append("\n").append(footerStrBuffer);
	}

	@Override
	public StringBuffer returnConsolidateBulkProductLog() {
		return mainStrBuffer.append(successProductStrBuffer).append(failedProductStrBuffer).append(footerStrBuffer);
	}
	
	@Override
	public void appendFooterMessage(String message) {
		footerStrBuffer.append(message).append("\n");
		
	}

	@Override
	public void appendFailedFilesMessage(String message) {
		failedFilesStrBuffer.append(message).append("\n");
		
	}

	@Override
	public void appendFailedFilesMessageToMain() {
		this.mainStrBuffer.append(failedFilesStrBuffer);
		
	}
	
	@Override
	public void appendProductTempMessage(String message) {
		this.tempProductBuffer.append(message).append("\n");		
	}
	
	@Override
	public void appendSuccessProductMainMessage() {
		this.successProductStrBuffer.append(tempProductBuffer).append("\n").append(footerStrBuffer).append("\n");
	}
	
	@Override
	public void appendFailedProductMainMessage() {
		this.failedProductStrBuffer.append(tempProductBuffer).append("\n").append(footerStrBuffer).append("\n");
	}
	
	@Override
	public void appendFailedFilesMessageToTempBuffer()
	{
		this.tempProductBuffer.append(failedFilesStrBuffer);
	}
	
	@Override
	public void clearBuffers()
	{
		this.footerStrBuffer = footerStrBuffer.delete(0, footerStrBuffer.length());
		this.failedFilesStrBuffer = failedFilesStrBuffer.delete(0, failedFilesStrBuffer.length());
		this.tempProductBuffer = tempProductBuffer.delete(0, tempProductBuffer.length());
	}

}

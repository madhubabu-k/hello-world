/**
 * 
 */
package com.pearson.ps.ingest.provider;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.documentum.fc.client.DfQuery;
import com.documentum.fc.client.IDfCollection;
import com.documentum.fc.client.IDfQuery;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.common.DfException;
import com.pearson.ps.ingest.exception.IngestException;
import com.pearson.ps.ingest.main.IngestConstants;
import com.pearson.ps.ingest.utils.IngestUtility;

/**
 * @author Manav Leslie
 * 
 */
public class CachedDataProviderImpl implements CachedDataProvider {

	private static final Logger LOGGER = LoggerFactory.getLogger(CachedDataProviderImpl.class);

	private Map<String, String> objMapping;

	

	@Override
	public void populateExtensionMappingMap(String bu_id, IDfSession tempSession) throws DfException {
		objMapping = new HashMap<String, String>();
		IDfCollection coll = null;
		IDfQuery query = null;
		String dql = null;
		
		// Register Tables are having entries for Master BU only. So for BUs starts with Master BU, have to use getBUKey Method.
		String buid = IngestUtility.getBUKey(bu_id);
		
		try {
			dql = "select b.object_type , b.extension from dm_dbo.prsn_ps_type_mapping_details b , "
					+ "dm_dbo.prsn_ps_object_type_mapping a where a.mapping_name=b.mapping_name "
					+ "and a.bu_id=b.bu_name and a.bu_id = '" + StringEscapeUtils.escapeSql(buid) + "' ";

			query = new DfQuery();
			query.setDQL(dql);
			coll = query.execute(tempSession, 0);

			while (coll.next()) {
				this.objMapping.put(coll.getString("extension").toUpperCase(), coll.getString("object_type"));
			}
		} finally {
				try {
					if (coll != null) {
						coll.close();
					}
				} catch (DfException e) {
					LOGGER.error(e.getMessage());
				}
		}
	}

	@Override
	public String getObjTypeForExtension(String extension) throws IngestException {
		//LOGGER.info("extension  "+extension);
		if (objMapping == null )
		{
			throw new IngestException("Unable to instantiate the map for object type mapping ");
		}
		String objType = objMapping.get(extension.toUpperCase());
		if (objType == null) {
			objType = IngestConstants.OBJECT_TYPE_PRSN_PS_CONTENT;
		}
		return objType;
	}

	/*public Map<String, String> loadMetaFromProps(String strBuName) throws IOException, IngestException {
		LOGGER.info("Fetching for strBuName "+strBuName);
		Map<String, String> metaDataPropMap = new HashMap<String, String>();
		String strPropFile = getPropertyFileName(strBuName);
		ClassLoader loader = getClass().getClassLoader();

		if (strPropFile.equalsIgnoreCase("NO MATCH")) {
			throw new IngestException(
					"***** ERROR ***** Matching Properties File not found. The Attributes can not be mapped. The Ingest will not proceed.");
		}
		InputStream propStream = loader.getResourceAsStream(strPropFile);

		Properties MetaProps = new Properties();
		MetaProps.load(propStream);
		for (Entry<Object, Object> entry : MetaProps.entrySet()) {
			String key = (String) entry.getKey();
			String value = (String) entry.getValue();
			metaDataPropMap.put(key.trim(), value.trim());
		}

		return metaDataPropMap;
	}*/

	public String getPropertyFileName(String buName, boolean isBulkIngest) {
		String strName = "";

		if(isBulkIngest)
		{
			/*if(isProduct)
			{*/
				if (buName.toUpperCase().startsWith("DK".toUpperCase()) || buName.toUpperCase().startsWith("Penguin".toUpperCase())) {
					strName = "ProductMetaInfo_B3.properties";
				} else if (buName.toUpperCase().startsWith("PI".toUpperCase())) {
					strName = "ProductMetaInfo_EPM.properties";
				} else {
					strName = "NO MATCH";
				}
			/*}
			else
			{
				if (buName.toUpperCase().startsWith("DK".toUpperCase()) || buName.toUpperCase().startsWith("Penguin".toUpperCase())) {
					strName = "AssetsMetaInfo_B3.properties";
				} else if (buName.toUpperCase().startsWith("PI".toUpperCase())) {
					strName = "AssetsMetaInfo_EPM.properties";
				} else {
					strName = "NO MATCH";
				}
			}*/
		}
		else
		{
			if (buName.toUpperCase().startsWith("DK".toUpperCase())) {
				strName = "MetaDataInfo_DK.properties";
			} else if (buName.toUpperCase().startsWith("PI".toUpperCase())) {
				strName = "MetaDataInfo_PI.properties";
			} else if (buName.toUpperCase().startsWith("Penguin".toUpperCase())) {
				strName = "MetaDataInfo_Penguin.properties";
			} else {
				strName = "NO MATCH";
			}
		}

		return strName;
	}

}

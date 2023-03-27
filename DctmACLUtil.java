/*****************************************************************************
 *
 *    File Name   : DctmAcl.java  
 *    Purpose     :This type represents an ACL in Documentum. It's primary use is to connect the
  					data held in the ROWSecurityAcl POJO with an actual ACL object in Documentum.
 *    Created On  :     4th July,2011
 *    Created By  : HCL Technologies Ltd.
 *    Modified On :
 *    Modified By :     
 *    Changes Done:
 *    Remarks     :
 *
 *****************************************************************************/

package com.pearson.ps.ingest.utils;

import org.apache.commons.lang.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.documentum.com.DfClientX;
import com.documentum.com.IDfClientX;
import com.documentum.fc.client.IDfACL;
import com.documentum.fc.client.IDfCollection;
import com.documentum.fc.client.IDfPersistentObject;
import com.documentum.fc.client.IDfQuery;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.common.DfException;
import com.pearson.ps.ingest.exception.IngestException;
import com.pearson.ps.ingest.main.IngestConstants;

public final class DctmACLUtil {

	/**
	 * Private constructor to prevent creating instances
	 */
	private DctmACLUtil() {
	}

	private static final Logger LOGGER = LoggerFactory
			.getLogger(DctmACLUtil.class);

	/**
	 * The Documentum type name for ACL objects.
	 */

	private static final String ACL_NAME_PREFIX = "ps_product_";
	private static final String ACL_NAME_SUFFIX = "_acl";

	public static IDfACL getProductFolderAcl(String productId,
			IDfSession session, String businessUnit, String strSecurityProfile)
			throws DfException, IngestException {

		String aclName = ACL_NAME_PREFIX + productId + ACL_NAME_SUFFIX;

		IDfACL inProgressACL = createInDctm(session, aclName, businessUnit,
				strSecurityProfile);

		return inProgressACL;
	}

	/**
	 * Create a new ACL object in Documentum with permissions as specified in
	 * the ROWSecurityAcl object. If an ACL with this name already exists in the
	 * repository, then this method fails with an exception.
	 * 
	 * @param session
	 *            the session to use to create the new ACL.
	 * @param acl
	 *            the ROWSecurityAcl object containing permission information.
	 * @param businessUnit
	 *            the ROWSecurityAcl object containing permission information.
	 * @return an IDfACL object referencing the newly created ACL.
	 * @throws DfException
	 * @throws DfException
	 * @throws IngestException
	 */
	private static IDfACL createInDctm(IDfSession session, String aclName,
			String businessUnit, String strSecurityProfile) throws DfException,
			IngestException {

		IDfACL loidfACL = null;

		try {
			loidfACL = (IDfACL) session.newObject("dm_acl");
			loidfACL.setACLClass(0);
			loidfACL.setString("owner_name", "dm_dbo");
			loidfACL.setObjectName(aclName);
			loidfACL.setDescription("");
			loidfACL.save();

			String strgroup = "";
			String straccess = "";

			int iaccess = 0;
			IDfCollection loidfColl = null;

			String strBUName = IngestUtility.getBUKey(businessUnit);

			String strQueryForACLDetails = "SELECT a.ps_group, a.ps_access FROM dm_dbo.prsn_ps_security_profile a, dm_dbo.prsn_ps_profile_bu_mapping b WHERE b.bu_id = '"
					+ StringEscapeUtils.escapeSql(strBUName)
					+ "' and a.profile_mapping ='"
					+ StringEscapeUtils.escapeSql(strSecurityProfile)
					+ "' and a.profile_mapping = b.profile_mapping and a.lifecycle_state = "
					+ IngestConstants.IN_PROGRESS_STATE + "";

			LOGGER.info("strQueryForACLDetails : {}", strQueryForACLDetails);

			IDfClientX clientx = new DfClientX();
			IDfQuery idfQuery = clientx.getQuery();
			idfQuery.setDQL(strQueryForACLDetails);
			loidfColl = idfQuery.execute(session, IDfQuery.EXEC_QUERY);

			if (loidfColl != null) {
				while (loidfColl.next()) {
					strgroup = loidfColl.getValue("ps_group").toString();
					straccess = loidfColl.getValue("ps_access").toString();

					if (straccess.equalsIgnoreCase("NONE")) {
						iaccess = IDfACL.DF_PERMIT_NONE;
					} else if (straccess.equalsIgnoreCase("BROWSE")) {
						iaccess = IDfACL.DF_PERMIT_BROWSE;
					} else if (straccess.equalsIgnoreCase("READ")) {
						iaccess = IDfACL.DF_PERMIT_READ;
					} else if (straccess.equalsIgnoreCase("RELATE")) {
						iaccess = IDfACL.DF_PERMIT_RELATE;
					} else if (straccess.equalsIgnoreCase("VERSION")) {
						iaccess = IDfACL.DF_PERMIT_VERSION;
					} else if (straccess.equalsIgnoreCase("WRITE")) {
						iaccess = IDfACL.DF_PERMIT_WRITE;
					} else if (straccess.equalsIgnoreCase("DELETE")) {
						iaccess = IDfACL.DF_PERMIT_DELETE;
					}

					/** Code Modified for Bug DE394 */
					try {
						IDfPersistentObject accessorGroupObject = session
								.getObjectByQualification("dm_group where group_name = '"
										+ strgroup + "'");

						if (null != accessorGroupObject) {
							if (iaccess == IDfACL.DF_PERMIT_DELETE) {
								loidfACL.grant(
										strgroup,
										iaccess,
										"CHANGE_PERMIT,CHANGE_LOCATION,EXECUTE_PROC,CHANGE_STATE,CHANGE_OWNER,DELETE_OBJECT,CHANGE_FOLDER_LINKS");
							} else if (iaccess == IDfACL.DF_PERMIT_WRITE) {
								loidfACL.grant(strgroup, iaccess,
										"CHANGE_PERMIT,CHANGE_LOCATION,EXECUTE_PROC");
							} else {
								loidfACL.grant(strgroup, iaccess,
										"CHANGE_LOCATION,EXECUTE_PROC");
							}
							loidfACL.save();
						} else {
							LOGGER.error("Group doesn't exist : {} ", strgroup);
						}
					} catch (DfException dfe) {
						LOGGER.error(dfe.getMessage(), dfe);
					}
					/** Code Modified for Bug DE394 */
				}
				loidfColl.close();
				loidfColl = null;
			}

			// loidfACL.save();

		} catch (DfException dfe) {
			throw new IngestException(dfe.getMessage(), dfe);
		}
		return loidfACL;
	}

}

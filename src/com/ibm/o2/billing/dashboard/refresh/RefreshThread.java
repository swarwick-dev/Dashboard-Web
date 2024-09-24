package com.ibm.o2.billing.dashboard.refresh;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.ServletContext;

import oracle.jdbc.driver.OracleDriver;

//import com.ibm.icu.text.DateFormat;
//import com.ibm.icu.text.SimpleDateFormat;

//import oracle.jdbc.driver.OracleDriver;

public class RefreshThread implements Runnable {

	private ServletContext context;
	private String sContext = "";
	String sRefreshDTM = "";
	Timestamp tsLastRefreshDtm;
	String sWarnings = "";
	private Connection con;
	OracleDriver d = new oracle.jdbc.driver.OracleDriver();
	private String sOraSID = "GENPRO";
	String sDBStatus = "UNAVAILABLE";
	private String sHost = "gb02qds203o2px7";
	private String sUser = "geneva_admin";
	private String sPwd = "genpro";
	private String sPort = "1521";
//	private String sHost = "localhost";
//	private String sUser = "geneva_admin";
//	private String sPwd = "genpro";
//	private String sPort = "23400";
	
	private String buildTd(String sValue, String sAlignment, String sColour, int iSpan, int iWidth, int iFontSize,
			boolean bBold, boolean bRightBorder) {
		String sTdString = "<td style='font-size:";
		sTdString += String.valueOf(iFontSize);
		sTdString += "px; color: ";
		sTdString += sColour;
		sTdString += "; width =";
		sTdString += String.valueOf(iWidth);
		sTdString += "px; text-align: ";
		sTdString += sAlignment;
		sTdString += ";  padding-left: 2px;";
		/*
		 * if (bRightBorder) sTdString += "border: 2px solid #ddd; ";
		 */
		if (bBold)
			sTdString += "font-weight: bold;' colspan='";
		else
			sTdString += "' colspan='";
		sTdString += String.valueOf(iSpan);
		sTdString += "'>";
		sTdString += sValue;
		sTdString += " </td>";

		return sTdString;
	}

	private String buildProgress(float currVal, float maxVal) {
		String sResult = "";

		float fPct = currVal > 0 ? (currVal / maxVal) * 100 : 0;
		String sColour = "w3-light-grey";

		if (fPct == 100)
			sColour = "w3-green";
		else if (fPct > 0)
			sColour = "w3-orange ";

		sResult = "<div class='w3-light-grey w3-round'> " + "<div class='w3-container w3-center w3-round " + sColour
				+ "' style='width:" + fPct + "%'>" + fPct + "%</div></div>";

		return sResult;
	}

	private String buildTh(String sValue, String sAlignment, String sColour, int iSpan, int iWidth, int iFontSize,
			boolean bBold, boolean bRightBorder) {
		String sTdString = "<th style='font-size:";
		sTdString += String.valueOf(iFontSize);
		sTdString += "px; color: ";
		sTdString += sColour;
		sTdString += "; width =";
		sTdString += String.valueOf(iWidth);
		sTdString += "px; text-align: ";
		sTdString += sAlignment;
		sTdString += ";  padding-left: 2px;";
		/*
		 * if (bRightBorder) sTdString += "border: 2px solid #ddd; ";
		 */
		if (bBold)
			sTdString += "font-weight: bold;' colspan='";
		else
			sTdString += "' colspan='";
		sTdString += String.valueOf(iSpan);
		sTdString += "'>";
		sTdString += sValue;
		sTdString += " </th>";

		return sTdString;
	}

	String getDashString(String sComponent, String sName) {
		int iStatus = 0;
		String sSQL;
		Statement st = null;
		ResultSet rs = null;
		String sResult = "";

		sSQL = new String("select string from interfacedash.dashboard where component = '");
		sSQL += sComponent;
		sSQL += "' and name = '";
		sSQL += sName;
		sSQL += "'";

		try {
			st = con.createStatement();
			rs = st.executeQuery(sSQL);
			while (rs.next()) {
				sResult = rs.getString(1);
			}

			st.close();
		} catch (SQLException e1) {
			System.out.println("**** BILLD - ERROR : " + e1.getMessage());
			sResult = "";
		}

		return sResult;
	}

	int getDashInt(String sComponent, String sName) {
		int iStatus = 0;
		String sSQL;
		Statement st = null;
		ResultSet rs = null;
		String sResult = "0";

		sSQL = new String("select string from interfacedash.dashboard where component = '");
		sSQL += sComponent;
		sSQL += "' and name = '";
		sSQL += sName;
		sSQL += "'";

		try {
			st = con.createStatement();
			rs = st.executeQuery(sSQL);
			while (rs.next()) {
				sResult = rs.getString(1);
			}

			st.close();
		} catch (SQLException e1) {
			System.out.println("**** BILLD - ERROR : " + e1.getMessage());
			sResult = "0";
		}

		return Integer.parseInt(sResult);
	}

	String getRunningTasks() {
		String sSQL;
		Statement st = null;
		ResultSet rs = null;
		String sResult = "";

		sSQL = new String(
				"select distinct to_char(tl.start_dtm,'DD-MM-YYYY HH24:MI:SS') as start_dtm , t.task_name , pp.process_plan_name , nvl(tl.total_errors,0) as errors , round((sysdate - tl.start_dtm) * 24 * 60,2) as minutes from task t, tasklog tl, processlog pl, processplan pp\n"
						+ "where tl.task_status in (1,2) and tl.end_dtm is null and t.task_id = tl.task_id \n"
						+ "and pl.task_instance_id = tl.task_instance_id\n"
						+ "and pp.process_def_id = pl.process_def_id\n" + "and pl.plan_number = pp.plan_number\n"
						+ "and pl.end_dtm is null order by 1 asc");

		try {
			st = con.createStatement();
			rs = st.executeQuery(sSQL);
			while (rs.next()) {
				sResult = sResult + "<tr>";

				sResult = sResult + buildTd(rs.getString("start_dtm"), "left", "black", 1, 60, 12, false, false);
				sResult = sResult + buildTd(rs.getString("task_name"), "left", "black", 1, 120, 12, false, false);
				sResult = sResult
						+ buildTd(rs.getString("process_plan_name"), "left", "black", 1, 120, 12, false, false);
				sResult = sResult + buildTd(rs.getString("errors"), "left", "black", 1, 60, 12, false, false);
				sResult = sResult + buildTd(rs.getString("minutes"), "left", "black", 1, 60, 12, false, false);

				sResult = sResult + "</tr>";
			}

			st.close();
		} catch (SQLException e1) {
			System.out.println("**** BILLD - Running ERROR : " + e1.getMessage());
			e1.printStackTrace();
			sResult = "";
		}

		return sResult;
	}

	String getCompletedTasks() {
		String sSQL;
		Statement st = null;
		ResultSet rs = null;
		String sResult = "";
		String sDesc = "";
		String sTstmp = "";

		sSQL = new String("select to_char(TSTMP,'HH24:MI:SS YYYY-MM-DD') AS LOG_TSTMP, DESCRIPTION from (\n"
				+ "                                        SELECT pl.end_dtm AS TSTMP, CASE WHEN pl.total_errors > 0 THEN  ' PP-FAILURE: ' WHEN pl.processes_failed > 0 THEN ' PP-FAILURE: ' ELSE '      PP-OK: ' END || pp.process_plan_name || ' (of (' || pt.task_name || ') completed. (' || pl.processes_failed || ' failed processes / ' || pl.total_errors || ' errors)' AS DESCRIPTION\n"
				+ "                                        FROM       processlog pl, processplan pp, task pt, tasklog pr\n"
				+ "                                        WHERE      (((pl.processes_failed + pl.total_errors > 0) AND pl.end_dtm > sysdate - interval '2' hour)\n"
				+ "                                        OR       (      (pl.processes_failed + pl.total_errors >= 0)\n"
				+ "                                        AND pl.end_dtm > sysdate - INTERVAL '2' hour))\n"
				+ "                                        AND     pp.process_def_id = pl.process_def_id\n"
				+ "                                        AND     pp.start_dat = (SELECT max(pp3.start_dat) FROM processplan pp3\n"
				+ "                                        WHERE   pp3.plan_number = pl.plan_number AND     pp3.process_def_id = pl.process_def_id)\n"
				+ "                                        AND     pp.plan_number = pl.plan_number AND     pr.task_id = pt.task_id\n"
				+ "                                        AND     pl.task_instance_id = pr.task_instance_id union all\n"
				+ "                                        SELECT tl.end_dtm AS TSTMP, CASE WHEN tl.total_errors > 0 THEN  ' TA-FAILURE: ' ELSE '      TA-OK: ' END || ta.task_name || ' completed. (' || tl.total_errors || ' errors)'AS DESCRIPTION\n"
				+ "                                        FROM task ta, tasklog tl WHERE   tl.task_id = ta.task_id\n"
				+ "                                        AND     (((tl.end_dtm > sysdate - interval '2' hour) AND tl.total_errors > 0)\n"
				+ "                                        OR      ((tl.end_dtm > sysdate - interval '2' hour) AND tl.total_errors >= 0)))\n"
				+ "                                        ORDER BY TSTMP desc");

		try {
			st = con.createStatement();
			rs = st.executeQuery(sSQL);
			while (rs.next()) {
				sDesc = rs.getString("DESCRIPTION");
				sTstmp = rs.getString("LOG_TSTMP");

				sResult = sResult + "<tr>";

				if (sDesc.contains("FAILURE")) {
					sResult += buildTd(sTstmp + " " + sDesc, "left", "red", 1, 300, 12, true, false);

					if (sDesc.contains("PP")) {

						String sDTM = sTstmp.substring(9, 19) + " " + sTstmp.substring(0, 8);
						Timestamp ts = Timestamp.valueOf(sDTM);
						// String sUpdDTM = sRefreshDTM.substring(6, 10) + "-" +
						// sRefreshDTM.substring(3, 5) + "-"
						// + sRefreshDTM.substring(0, 2) + " " + sRefreshDTM.substring(11);

						tsLastRefreshDtm = Timestamp.valueOf(sRefreshDTM);
						if (((tsLastRefreshDtm.getTime() - ts.getTime()) / 60000) < 30) {
							List<String> lVals = Arrays.asList(sDesc.split("\\(", -1));
							String sFailure = lVals.get(0);
							sWarnings += "<li>" + sFailure + "</li>\n";
						}
					}
				} else
					sResult += buildTd(sTstmp + " " + sDesc, "left", "black", 1, 300, 12, false, false);

				sResult += "</tr>";
			}

			st.close();
		} catch (SQLException e1) {
			System.out.println("**** BILLD - Completed ERROR : " + e1.getMessage());
			sResult = "";
		}

		return sResult;
	}

	String getFS() {
		String sSQL;
		Statement st = null;
		ResultSet rs = null;
		String sResult = "";
		String sDesc = "";

		sSQL = new String(
				"select STRING FROM INTERFACEDASH.DASHBOARD WHERE COMPONENT = 'DASHBOARD' AND NAME = 'FS_USAGE'");

		try {
			st = con.createStatement();
			rs = st.executeQuery(sSQL);
			while (rs.next()) {
				sDesc = rs.getString("STRING");

				if (!sDesc.contains("null")) {

					String[] sTokens = sDesc.split("\n");
					sResult = sResult + "<tr>";

					for (int i = 0; i < sTokens.length; i++) {
						String[] sFS = sTokens[i].split(",");
						String sColour = "black";
						boolean bBold = false;

						if (sFS[0].equals("/export/home/geneva/live/log") && Integer.parseInt(sFS[1]) > 98) {
							sColour = "red";
							bBold = true;
						} else if (sFS[0].equals("/export/home/geneva/live/cdrpp") && Integer.parseInt(sFS[1]) > 98) {
							sColour = "red";
							bBold = true;
						} else if (sFS[0].equals("/export/home/geneva/live/SPD") && Integer.parseInt(sFS[1]) > 98) {
							sColour = "red";
							bBold = true;
						} else if (sFS[0].equals("/export/home/BDBR") && Integer.parseInt(sFS[1]) > 65) {
							sColour = "red";
							bBold = true;
						} else if (Integer.parseInt(sFS[1]) > 85) {
							sColour = "red";
							bBold = true;
						}

						sResult += "<tr>";
						sResult += buildTd(sFS[0] + " : ", "right", sColour, 1, 50, 12, bBold, false);
						sResult += buildTd(sFS[1], "left", sColour, 1, 60, 12, bBold, false);
						sResult += "</tr>";
					}
				}
			}

			st.close();
		} catch (SQLException e1) {
			System.out.println("**** BILLD - FS ERROR : " + e1.getMessage());
			sResult = "";
		}

		return sResult;
	}

	String getSQLString(String sSQL) {
		String sResult = "";

		Statement st = null;
		ResultSet rs = null;

		try {
			st = con.createStatement();
			rs = st.executeQuery(sSQL);
			while (rs.next()) {
				sResult = rs.getString(1);
				break;
			}

			st.close();
		} catch (SQLException e1) {
			System.out.println("**** BILLD - getSQLString ERROR : " + e1.getMessage());
			sResult = "";
		}

		return sResult;
	}

	String getBDBRDetail() {
		String sResult = "";
		File sBDBRFile = new File(sContext + "bdbr.html.tmp");
		PrintWriter pwBDBR = null;
		String sBDBRDetail = "";
		String sExtrTables = "";
		String sTrfrTables = "";
		String sBDBRHtml = "";

		String sLog = getSQLString("SELECT pl.process_instance_id || '.LOG' "
				+ "FROM PROCESSLOG p, PROCESSINSTANCELOG pl WHERE p.PROCESS_DEF_ID IN (SELECT PROCESS_DEF_ID FROM PROCESSDEFINITION WHERE IMAGE_NAME like 'bdbr%.pl') "
				+ "and pl.process_id = p.process_id ORDER BY 1 DESC");

		String sExtrStart = getDashString("BDBR", "EXTRACT_START");
		String sExtrEnd = getDashString("BDBR", "EXTRACT_END");
		String sTrfrStart = getDashString("BDBR", "TRANSFER_START");
		String sTrfrEnd = getDashString("BDBR", "TRANSFER_END");

		String sTrfrCnt = getDashString("BDBR", "TRANSFER_THREADS");
		String sExtrCnt = getDashString("BDBR", "EXTRACT_THREADS");

		int bdbrs = getProcessCountPL("bdbr");

		// String sExtrMbs = getDashString("BDBR", "EXTRACT_SIZE");
		String sExpectedMbs = getSQLString(
				"select nvl(sum(mbs),0) from (select table_name, avg(total_filesize)/1024/1024 as mbs from bdbr_transfer_set where table_name in "
						+ "(SELECT table_name FROM BDBR_MANAGER WHERE EXTRACT_LOCK_BOO = 'T') "
						+ "group by table_name)");
		// int iDonePct = Integer.parseInt(sExtrMbs) > 0 ?
		// (Integer.parseInt(sExtrMbs)/Integer.parseInt(sExpectedMbs))*100 : 100;
		// String sExtrInProgress = getSQLString("SELECT NVL(COUNT(*),0) as a FROM
		// BDBR_MANAGER WHERE EXTRACT_LOCK_BOO = 'T'");
		String sExtrRunId = getSQLString("SELECT MAX(RUN_ID) as a FROM BDBR_EXTRACT_SET");
		String sTrfrRunId = getSQLString("SELECT MAX(RUN_ID) as a FROM BDBR_TRANSFER_SET");

		int iExtrComp = 0;
		int iExtrFailed = 0;
		int iExtrRun = 0;
		int iExtrPend = 0;
		int iTrfrComp = 0;
		int iTrfrFailed = 0;
		int iTrfrRun = 0;
		int iTrfrPend = 0;
		int iTrfrFilesPend = 0;
		int iTrfrFilesSent = 0;
		int iTrfrFilesRun = 0;
		int iTrfrFilesFailed = 0;
		float fTrfrFilesSizePend = 0;
		float fTrfrFilesSizeSent = 0;
		float fTrfrFilesSizeRun = 0;
		float fTrfrFilesSizeFailed = 0;

		Statement st = null;
		ResultSet rs = null;
		String sSQL = "SELECT TABLE_NAME, STATUS, nvl(to_char(EXTRACT_START_DTM,'YYYY-MM-DD HH24:MI:SS'),' '), nvl(to_char(EXTRACT_END_DTM,'YYYY-MM-DD HH24:MI:SS'),' '), "
				+ "EXTRACT_TYPE, EXTRACT_SEQ FROM BDBR_EXTRACT_SET WHERE RUN_ID = " + sExtrRunId
				+ " order by EXTRACT_START_DTM DESC";
		try {
			st = con.createStatement();
			rs = st.executeQuery(sSQL);
			int iStatus = 0;
			while (rs.next()) {
				sExtrTables += "<tr>";

				iStatus = rs.getInt(2);
				String sColour = "black";
				String sStatus = "";

				switch (iStatus) {
				case 1:
					iExtrPend++;
					sStatus = "PENDING";
					break;
				case 2:
					iExtrRun++;
					sColour = "green";
					sStatus = "RUNNING";
					break;
				case 3:
					iExtrComp++;
					sStatus = "COMPLETE";
					break;
				case 4:
					iExtrFailed++;
					sColour = "red";
					sStatus = "FAILED";
					break;
				}

				sExtrTables += buildTd(rs.getString(1), "left", sColour, 1, 255, 12, false, true);
				sExtrTables += buildTd(sStatus, "left", sColour, 1, 60, 12, false, true);
				sExtrTables += buildTd(rs.getString(3), "left", sColour, 1, 60, 12, false, true);
				sExtrTables += buildTd(rs.getString(4), "left", sColour, 1, 60, 12, false, true);
				sExtrTables += buildTd(rs.getString(5), "left", sColour, 1, 60, 12, false, true);
				sExtrTables += buildTd(rs.getString(6), "left", sColour, 1, 60, 12, false, true);

				sExtrTables += "</tr>";
			}

			st.close();
		} catch (SQLException e1) {
			System.out.println("**** BILLD - BDBR ERROR : " + e1.getMessage());
			sResult = "";
		}

		sSQL = "SELECT TABLE_NAME, STATUS, nvl(to_char(TRANSFER_START_DTM,'YYYY-MM-DD HH24:MI:SS'),' '), nvl(to_char(TRANSFER_END_DTM,'YYYY-MM-DD HH24:MI:SS'),' '), "
				+ "EXTRACT_SEQ FROM BDBR_TRANSFER_SET WHERE (STATUS IN (1,2) OR CREATED_DTM >= to_date('" + sTrfrStart
				+ "','YYYY-MM-DD HH24:MI:SS')) order by TRANSFER_START_DTM DESC";
		try {
			st = con.createStatement();
			rs = st.executeQuery(sSQL);
			int iStatus = 0;
			while (rs.next()) {
				sTrfrTables += "<tr>";
				iStatus = rs.getInt(2);
				String sColour = "black";
				String sStatus = "";

				switch (iStatus) {
				case 1:
					iTrfrPend++;
					sStatus = "PENDING";
					break;
				case 2:
					iTrfrRun++;
					sColour = "green";
					sStatus = "RUNNING";
					break;
				case 3:
					iTrfrComp++;
					sStatus = "COMPLETE";
					break;
				case 4:
					iTrfrFailed++;
					sColour = "red";
					sStatus = "FAILED";
					break;
				}

				sTrfrTables += buildTd(rs.getString(1), "left", sColour, 1, 255, 12, false, true);
				sTrfrTables += buildTd(sStatus, "left", sColour, 1, 60, 12, false, true);
				sTrfrTables += buildTd(rs.getString(3), "left", sColour, 1, 60, 12, false, true);
				sTrfrTables += buildTd(rs.getString(4), "left", sColour, 1, 60, 12, false, true);
				sTrfrTables += buildTd(rs.getString(5), "left", sColour, 1, 60, 12, false, true);

				sTrfrTables += "</tr>";
			}

			st.close();
		} catch (SQLException e1) {
			System.out.println("**** BILLD - ERROR : " + e1.getMessage());
			sResult = "";
		}

		sSQL = "select ts.status, count(*), nvl(sum(filesize),0)/1024/1024 from bdbr_transfer_set ts,bdbr_transfer_set_files tsf where tsf.transfer_set_id = ts.transfer_set_id and (ts.STATUS IN (1,2) OR ts.created_dtm >= to_date('"
				+ sTrfrStart + "','YYYY-MM-DD HH24:MI:SS')) group by ts.status"; // Total files + size
		try {
			st = con.createStatement();
			rs = st.executeQuery(sSQL);
			int iStatus = 0;
			while (rs.next()) {
				iStatus = rs.getInt(1);

				switch (iStatus) {
				case 1:
					iTrfrFilesPend += rs.getInt(2);
					fTrfrFilesSizePend += rs.getFloat(3);
					break;
				case 2:
					iTrfrFilesRun += rs.getInt(2);
					fTrfrFilesSizeRun += rs.getFloat(3);
					break;
				case 3:
					iTrfrFilesSent += rs.getInt(2);
					fTrfrFilesSizeSent += rs.getFloat(3);
					break;
				case 4:
					iTrfrFilesFailed += rs.getInt(2);
					fTrfrFilesSizeFailed += rs.getFloat(3);
					break;
				}
			}

			st.close();
		} catch (SQLException e1) {
			System.out.println("**** BILLD - ERROR : " + e1.getMessage());
			sResult = "";
		}

		sResult += "<tr>" + buildTd("Status : ", "right", "black", 1, 50, 12, false, false);
		sResult += buildTd(bdbrs > 0 ? "RUNNING" : "NOT RUNNING", "left",
				(iTrfrFailed + iExtrFailed) == 0 ? "black" : "red", 1, 60, 12, false, false) + "</tr>";
		sResult += "<tr>" + buildTd("Process Count : ", "right", "black", 1, 50, 12, false, false);
		sResult += buildTd(Integer.toString(bdbrs), "left", "black", 1, 60, 12, false, false) + "</tr>";
		sResult += "<tr>" + buildTd("Extract Start : ", "right", "black", 1, 50, 12, false, false);
		sResult += buildTd(sExtrStart, "left", "black", 1, 60, 12, false, false);
		sResult += buildTd("Extract End : ", "right", "black", 1, 50, 12, false, false);
		sResult += buildTd(sExtrEnd, "left", "black", 1, 60, 12, false, false) + "</tr>";
		sResult += "<tr>" + buildTd("Transfer Start : ", "right", "black", 1, 50, 12, false, false);
		sResult += buildTd(sTrfrStart, "left", "black", 1, 60, 12, false, false);
		sResult += buildTd("Transfer End : ", "right", "black", 1, 50, 12, false, false);
		sResult += buildTd(sTrfrEnd, "left", "black", 1, 60, 12, false, false) + "</tr>";
		sResult += "<tr>" + buildTd("Log File : ", "right", "black", 1, 50, 12, false, false);
		sResult += buildTd(sLog, "left", "black", 1, 60, 12, false, false) + "</tr>";
		sResult += "<tr>" + buildTd("Errors : ", "right", "black", 1, 50, 12, false, false);
		sResult += buildTd(Integer.toString(iTrfrFailed + iExtrFailed), "left",
				iTrfrFailed + iExtrFailed == 0 ? "black" : "red", 1, 60, 12, false, false) + "</tr>";
		sResult += "<tr>" + buildTd("Extractors : ", "right", "black", 1, 50, 12, false, false);
		sResult += buildTd(sExtrCnt, "left", "black", 1, 60, 12, false, false) + "</tr>";
		sResult += "<tr>" + buildTd("Extracting : ", "right", "black", 1, 50, 12, false, false);
		sResult += buildTd(Integer.toString(iExtrRun), "left", "black", 1, 60, 12, false, false) + "</tr>";
		sResult += "<tr>" + buildTd("Extracted : ", "right", "black", 1, 50, 12, false, false);
		sResult += buildTd(Integer.toString(iExtrComp), "left", "black", 1, 60, 12, false, false) + "</tr>";
		sResult += "<tr>" + buildTd("Pending : ", "right", "black", 1, 50, 12, false, false);
		sResult += buildTd(Integer.toString(iExtrPend), "left", "black", 1, 60, 12, false, false) + "</tr>";

		sResult += "<tr>" + buildTd("Progress : ", "right", "black", 1, 50, 12, false, false);
		sResult += buildTd(buildProgress(iExtrComp, (iExtrComp + iExtrPend + iExtrFailed + iExtrRun)), "left", "black",
				1, 50, 12, false, true);
		sResult += "</tr>";

		sResult += "<tr>" + buildTd("SCP Threads : ", "right", "black", 1, 50, 12, false, false);
		sResult += buildTd(sTrfrCnt, "left", "black", 1, 60, 12, false, false) + "</tr>";
		sResult += "<tr>" + buildTd("Files to SCP : ", "right", "black", 1, 50, 12, false, false);
		sResult += buildTd(Integer.toString(iTrfrFilesPend + iTrfrFilesRun + iTrfrFilesFailed), "left", "black", 1, 60,
				12, false, false) + "</tr>";
		sResult += "<tr>" + buildTd("Data to SCP : ", "right", "black", 1, 50, 12, false, false);
		sResult += buildTd(Float.toString(fTrfrFilesSizePend + fTrfrFilesSizeFailed + fTrfrFilesSizeRun) + " MB",
				"left", "black", 1, 60, 12, false, false) + "</tr>";
		sResult += "<tr>" + buildTd("Files Sent : ", "right", "black", 1, 50, 12, false, false);
		sResult += buildTd(Integer.toString(iTrfrFilesSent), "left", "black", 1, 60, 12, false, false) + "</tr>";
		sResult += "<tr>" + buildTd("Data Sent : ", "right", "black", 1, 50, 12, false, false);
		sResult += buildTd(Float.toString(fTrfrFilesSizeSent) + " MB", "left", "black", 1, 60, 12, false, false)
				+ "</tr>";

		sResult += "<tr>" + buildTd("Progress : ", "right", "black", 1, 50, 12, false, false);
		sResult += buildTd(
				buildProgress(fTrfrFilesSizeSent,
						(fTrfrFilesSizeSent + fTrfrFilesSizePend + fTrfrFilesSizeFailed + fTrfrFilesSizeRun)),
				"left", "black", 1, 50, 12, false, true);

		sResult += "</tr>";

		sBDBRHtml += "	<div> <div class=col-lg-6>\n" + "		<rd-widget>\n"
				+ "		<rd-widget-header title=\"BDBR Extract Status\">\n" + "		</rd-widget-header>\n"
				+ "		<rd-widget-body classes=\"medium no-padding\">\n" + "		<div class=table-responsive>\n"
				+ "			<table>\n" + "				<tbody>\n";

		sBDBRHtml += "<tr>" + buildTd("Status : ", "right", "black", 1, 50, 12, false, false);
		sBDBRHtml += buildTd(Integer.parseInt(sExtrCnt) > 0 ? "RUNNING" : "NOT RUNNING", "left",
				(iExtrFailed) == 0 ? "black" : "red", 1, 60, 12, false, false) + "</tr>";
		sBDBRHtml += "<tr>" + buildTd("Process Count : ", "right", "black", 1, 50, 12, false, false);
		sBDBRHtml += buildTd(sExtrCnt, "left", "black", 1, 60, 12, false, false) + "</tr>";
		sBDBRHtml += "<tr>" + buildTd("Extract Start : ", "right", "black", 1, 50, 12, false, false);
		sBDBRHtml += buildTd(sExtrStart, "left", "black", 1, 60, 12, false, false);
		sBDBRHtml += buildTd("Extract End : ", "right", "black", 1, 50, 12, false, false);
		sBDBRHtml += buildTd(sExtrEnd, "left", "black", 1, 60, 12, false, false) + "</tr>";
		sBDBRHtml += "<tr>" + buildTd("Log File : ", "right", "black", 1, 50, 12, false, false);
		sBDBRHtml += buildTd(sLog, "left", "black", 1, 60, 12, false, false) + "</tr>";
		sBDBRHtml += "<tr>" + buildTd("Errors : ", "right", "black", 1, 50, 12, false, false);
		sBDBRHtml += buildTd(Integer.toString(iExtrFailed), "left", iExtrFailed == 0 ? "black" : "red", 1, 60, 12,
				false, false) + "</tr>";
		sBDBRHtml += "<tr>" + buildTd("Extract Complete : ", "right", "black", 1, 50, 12, false, false);
		sBDBRHtml += buildTd(Integer.toString(iExtrComp), "left", "black", 1, 60, 12, false, false) + "</tr>";
		sBDBRHtml += "<tr>" + buildTd("Extract Running : ", "right", "black", 1, 50, 12, false, false);
		sBDBRHtml += buildTd(Integer.toString(iExtrRun), "left", "black", 1, 60, 12, false, false) + "</tr>";
		sBDBRHtml += "<tr>" + buildTd("Extract Failed : ", "right", "black", 1, 50, 12, false, false);
		sBDBRHtml += buildTd(Integer.toString(iExtrFailed), "left", "black", 1, 60, 12, false, false) + "</tr>";
		sBDBRHtml += "<tr>" + buildTd("Extract Pending : ", "right", "black", 1, 50, 12, false, false);
		sBDBRHtml += buildTd(Integer.toString(iExtrPend), "left", "black", 1, 60, 12, false, false) + "</tr>";
		sBDBRHtml += "<tr>" + buildTd("Progress : ", "right", "black", 1, 50, 12, false, false);
		sBDBRHtml += buildTd(buildProgress(iExtrComp, (iExtrComp + iExtrPend + iExtrFailed + iExtrRun)), "left",
				"black", 1, 50, 12, false, true);
		sBDBRHtml += "</tr>";

		sBDBRHtml += "				</tbody>\n" + "			</table>\n" + " </div>\n"
				+ "		</rd-widget-body></rd-widget>\n" + "		<rd-widget>\n"
				+ "		<rd-widget-body classes=\"medium no-padding\">\n" + "		<div class=table-responsive>\n"
				+ "			<table>\n" + "<thead>" + buildTh("TABLE NAME", "left", "black", 1, 255, 12, true, true)
				+ buildTh("STATUS", "left", "black", 1, 60, 12, true, true)
				+ buildTh("START DTM", "left", "black", 1, 60, 12, true, true)
				+ buildTh("END DTM", "left", "black", 1, 60, 12, true, true)
				+ buildTh("TYPE", "left", "black", 1, 60, 12, true, true)
				+ buildTh("SEQ", "left", "black", 1, 60, 12, true, true) + "</thead>" + "				<tbody>\n"
				+ sExtrTables + "				</tbody>\n" + "			</table>\n" + "		</div>\n"
				+ "		</rd-widget-body></rd-widget>\n" + "	</div> </div>\n";

		sBDBRHtml += "	<div><div class=col-lg-6>\n" + "		<rd-widget>\n"
				+ "		<rd-widget-header title=\"BDBR Transfer Status\">\n" + "		</rd-widget-header>\n"
				+ "		<rd-widget-body classes=\"medium no-padding\">\n" + "		<div class=table-responsive>\n"
				+ "			<table>\n" + "				<tbody>\n";

		sBDBRHtml += "<tr>" + buildTd("Status : ", "right", "black", 1, 50, 12, false, false);
		sBDBRHtml += buildTd(Integer.parseInt(sTrfrCnt) > 0 ? "RUNNING" : "NOT RUNNING", "left",
				(iTrfrFailed) == 0 ? "black" : "red", 1, 60, 12, false, false) + "</tr>";
		sBDBRHtml += "<tr>" + buildTd("Process Count : ", "right", "black", 1, 50, 12, false, false);
		sBDBRHtml += buildTd(sTrfrCnt, "left", "black", 1, 60, 12, false, false) + "</tr>";
		sBDBRHtml += "<tr>" + buildTd("Transfer Start : ", "right", "black", 1, 50, 12, false, false);
		sBDBRHtml += buildTd(sTrfrStart, "left", "black", 1, 60, 12, false, false);
		sBDBRHtml += buildTd("Transfer End : ", "right", "black", 1, 50, 12, false, false);
		sBDBRHtml += buildTd(sTrfrEnd, "left", "black", 1, 60, 12, false, false) + "</tr>";
		sBDBRHtml += "<tr>" + buildTd("Log File : ", "right", "black", 1, 50, 12, false, false);
		sBDBRHtml += buildTd(sLog, "left", "black", 1, 60, 12, false, false) + "</tr>";
		sBDBRHtml += "<tr>" + buildTd("Errors : ", "right", "black", 1, 50, 12, false, false);
		sBDBRHtml += buildTd(Integer.toString(iTrfrFailed), "left", iTrfrFailed == 0 ? "black" : "red", 1, 60, 12,
				false, false) + "</tr>";
		sBDBRHtml += "<tr>" + buildTd("Transfer Complete : ", "right", "black", 1, 50, 12, false, false);
		sBDBRHtml += buildTd(Integer.toString(iTrfrComp), "left", "black", 1, 60, 12, false, false) + "</tr>";
		sBDBRHtml += "<tr>" + buildTd("Transfer Running : ", "right", "black", 1, 50, 12, false, false);
		sBDBRHtml += buildTd(Integer.toString(iTrfrRun), "left", "black", 1, 60, 12, false, false) + "</tr>";
		sBDBRHtml += "<tr>" + buildTd("Transfer Failed : ", "right", "black", 1, 50, 12, false, false);
		sBDBRHtml += buildTd(Integer.toString(iTrfrFailed), "left", "black", 1, 60, 12, false, false) + "</tr>";
		sBDBRHtml += "<tr>" + buildTd("Transfer Pending : ", "right", "black", 1, 50, 12, false, false);
		sBDBRHtml += buildTd(Integer.toString(iTrfrPend), "left", "black", 1, 60, 12, false, false) + "</tr>";
		sBDBRHtml += "<tr>" + buildTd("Progress : ", "right", "black", 1, 50, 12, false, false);
		sBDBRHtml += buildTd(
				buildProgress(fTrfrFilesSizeSent,
						(fTrfrFilesSizeSent + fTrfrFilesSizePend + fTrfrFilesSizeFailed + fTrfrFilesSizeRun)),
				"left", "black", 1, 50, 12, false, true);

		sBDBRHtml += "</tr>";

		sBDBRHtml += "				</tbody>\n" + "			</table>\n" + " </div>\n"
				+ "		</rd-widget-body></rd-widget>\n" + "		<rd-widget>\n"
				+ "		<rd-widget-body classes=\"medium no-padding\">\n" + "		<div class=table-responsive>\n"
				+ "		<div class=table-responsive>\n" + "			<table>\n" + "<thead>"
				+ buildTh("TABLE NAME", "left", "black", 1, 255, 12, true, true)
				+ buildTh("STATUS", "left", "black", 1, 60, 12, true, true)
				+ buildTh("START DTM", "left", "black", 1, 60, 12, true, true)
				+ buildTh("END DTM", "left", "black", 1, 60, 12, true, true)
				+ buildTh("SEQ", "left", "black", 1, 60, 12, true, true) + "</thead>" + "				<tbody>\n"
				+ sTrfrTables + "				</tbody>\n" + "			</table>\n" + "		</div>\n"
				+ "		</rd-widget-body></rd-widget>\n" + "	</div> </div>\n";

		sBDBRHtml += "	<div class=col-sm-12>\n" + "		<rd-widget>\n"
				+ "		<rd-widget-header title=\"Last Refreshed : " + sRefreshDTM + "\">\n"
				+ "</rd-widget-header>\n</rd-widget>\n	</div>\n";

		try {
			pwBDBR = new PrintWriter(new FileWriter(sBDBRFile));
		} catch (IOException e) {
			e.printStackTrace();
			sResult = "";
		}
		pwBDBR.println(sBDBRHtml);
		pwBDBR.close();
		sBDBRFile.renameTo(new File(sContext + "bdbr.html"));

		return sResult;
	}

	String getRBM() {
		String sResult = "";
		String sSQL;
		Statement st = null;
		ResultSet rs = null;

		int configAgent = getProcessCountSess("DConfigAgent");
		int tm = getProcessCountSess("TM");
		int biceps = getProcessCountSess("BiCEP");
		int cews = getProcessCountSess("CEW");
		int dums = getProcessCountSess("DUM");
		int cdrpps = getProcessCountSess("CDRPP");
		int sceps = getProcessCountSess("SCEP");
		int scbms = getProcessCountSess("SCBM");
		int cmsp = getProcessCountTask("CMS Action Performer");
		int cmse = getProcessCountTask("CMS Data Extract");
		int cmsl = getProcessCountTask("CMS Action Loader");

		int cmsp_upd = getDashInt("DASHBOARD", "CMS_AP_LOG_UPD");
		int cmse_upd = getDashInt("DASHBOARD", "CMS_DE_LOG_UPD");
		int cmsl_upd = getDashInt("DASHBOARD", "CMS_AL_LOG_UPD");

		sResult += "<tr>";
		sResult += buildTd("DB Status : ", "right", "black", 1, 60, 12, false, false);
		if (sDBStatus.contains("UNAVAILABLE"))
			sResult += buildTd(sDBStatus, "left", "red", 1, 50, 12, true, true);
		else
			sResult += buildTd(sDBStatus, "left", "green", 1, 50, 12, false, true);

		sResult += buildTd("Refreshed : ", "right", "black", 1, 60, 12, false, false);
		sResult += buildTd(sRefreshDTM, "left", "black", 1, 50, 12, false, true);
		sResult += "</tr>";

		sResult += "<tr>";
		sResult += buildTd("Config Agent :", "right", "black", 1, 50, 12, false, false);
		sResult += buildTd(configAgent > 0 ? "RUNNING" : "STOPPED", "left", configAgent > 0 ? "green" : "red", 1, 60,
				12, false, false);
		sResult += buildTd("Task Master :", "right", "black", 1, 50, 12, false, false);
		sResult += buildTd(tm > 0 ? "RUNNING" : "STOPPED", "left", tm > 0 ? "green" : "red", 1, 60, 12, false, false);
		sResult += "</tr>";

		sResult += "<tr>";
		sResult += buildTd("Oracle SID :", "right", "black", 1, 50, 12, false, false);
		sResult += buildTd(sOraSID, "left", "black", 3, 60, 12, false, false);
		sResult += "</tr>";

		sResult += "<tr>";
		sResult += buildTd("CEW :", "right", "black", 1, 50, 12, false, false);
		sResult += buildTd(cews > 0 ? "RUNNING" : "STOPPED", "left", cews > 0 ? "green" : "red", 1, 60, 12, false,
				false);
		sResult += buildTd("BiCEP :", "right", "black", 1, 50, 12, false, false);
		sResult += buildTd(biceps > 0 ? "RUNNING" : "STOPPED", "left", biceps > 0 ? "green" : "red", 1, 60, 12, false,
				false);
		sResult += "</tr>";
		sResult += "<tr>";
		sResult += buildTd("Process Count :", "right", "black", 1, 50, 12, false, false);
		sResult += buildTd(Integer.toString(cews), "left", "black", 1, 60, 12, false, false);
		sResult += buildTd("Process Count :", "right", "black", 1, 50, 12, false, false);
		sResult += buildTd(Integer.toString(biceps), "left", "black", 1, 60, 12, false, false);
		sResult += "</tr>";

		sResult += "<tr>";
		sResult += buildTd("CDRPP :", "right", "black", 1, 50, 12, false, false);
		sResult += buildTd(cdrpps > 0 ? "RUNNING" : "STOPPED", "left", cdrpps > 0 ? "green" : "red", 1, 60, 12, false,
				false);
		sResult += buildTd("DUM :", "right", "black", 1, 50, 12, false, false);
		sResult += buildTd(dums > 0 ? "RUNNING" : "STOPPED", "left", dums > 0 ? "green" : "red", 1, 60, 12, false,
				false);
		sResult += "</tr>";
		sResult += "<tr>";
		sResult += buildTd("Process Count :", "right", "black", 1, 50, 12, false, false);
		sResult += buildTd(Integer.toString(cdrpps), "left", "black", 1, 60, 12, false, false);
		sResult += buildTd("Process Count :", "right", "black", 1, 50, 12, false, false);
		sResult += buildTd(Integer.toString(dums), "left", "black", 1, 60, 12, false, false);
		sResult += "</tr>";

		sResult += "<tr>";
		sResult += buildTd("SCEP :", "right", "black", 1, 50, 12, false, false);
		sResult += buildTd(sceps > 0 ? "RUNNING" : "STOPPED", "left", sceps > 0 ? "green" : "red", 1, 60, 12, false,
				false);
		sResult += buildTd("SCBM :", "right", "black", 1, 50, 12, false, false);
		sResult += buildTd(scbms > 0 ? "RUNNING" : "STOPPED", "left", scbms > 0 ? "green" : "red", 1, 60, 12, false,
				false);
		sResult += "</tr>";
		sResult += "<tr>";
		sResult += buildTd("Process Count :", "right", "black", 1, 50, 12, false, false);
		sResult += buildTd(Integer.toString(sceps), "left", "black", 1, 60, 12, false, false);
		sResult += buildTd("Process Count :", "right", "black", 1, 50, 12, false, false);
		sResult += buildTd(Integer.toString(scbms), "left", "black", 1, 60, 12, false, false);
		sResult += "</tr>";

		try {
			st = con.createStatement();
			sSQL = new String(
					"SELECT system, to_number(trunc((sysdate-min(TIDEMARK_DTM))*24*60)) as minutes from geneva_adapter.O2TIDEMARKBILLING group by system order by 1");

			rs = st.executeQuery(sSQL);
			while (rs.next()) {

				sResult += "<tr>";
				sResult += buildTd(rs.getString("system") + " :", "right", "black", 1, 50, 12, false, false);
				sResult += buildTd(Integer.toString(rs.getInt("minutes")) + " minutes", "left",
						rs.getInt("minutes") < 15 ? "green" : "red", 3, 60, 12,
						rs.getInt("minutes") < 15 ? false : true, false);
				sResult += "</tr>";
			}

			st.close();
		} catch (SQLException e) {
			System.out.println("**** BILLD - RBM ERROR : " + e.getMessage());
			sResult = "";
		}

		String sCMSpText = "";
		if (cmsp_upd > 15) {
			sCMSpText = "ALERT : No Updates in log file for > 15 minutes - Call Out AMS";
		} else if (cmsp > 0) {
			sCMSpText = "RUNNING";
		} else {
			sCMSpText = "ALERT : Not running - Check and re-start it";
		}

		String sCMSeText = "";
		if (cmse_upd > 15) {
			sCMSeText = "ALERT : No Updates in log file for > 15 minutes - Call Out AMS";
		} else if (cmse > 0) {
			sCMSeText = "RUNNING";
		} else {
			sCMSeText = "ALERT : Not running - Check and re-start it";
		}

		String sCMSlText = "";
		if (cmsl_upd > 15) {
			sCMSlText = "ALERT : No Updates in log file for > 15 minutes - Call Out AMS";
		} else if (cmsl > 0) {
			sCMSlText = "RUNNING";
		} else {
			sCMSlText = "ALERT : Not running - Check and re-start it";
		}

		sResult += "<tr>";
		sResult += buildTd("CMS Performer :", "right", "black", 1, 50, 12, false, false);
		sResult += buildTd(sCMSpText, "left", (cmsp == 0 || sCMSpText.contains("ALERT")) ? "red" : "green", 3, 60, 12,
				(cmsp == 0 || sCMSpText.contains("ALERT")) ? true : false, false);
		sResult += "</tr>";
		sResult += "<tr>";
		sResult += buildTd("CMS Extractor :", "right", "black", 1, 50, 12, false, false);
		sResult += buildTd(sCMSeText, "left", (cmse == 0 || sCMSeText.contains("ALERT")) ? "red" : "green", 3, 60, 12,
				(cmse == 0 || sCMSeText.contains("ALERT")) ? true : false, false);
		sResult += "</tr>";
		sResult += "<tr>";
		sResult += buildTd("CMS Loader :", "right", "black", 1, 50, 12, false, false);
		sResult += buildTd(sCMSlText, "left", (cmsl == 0 || sCMSlText.contains("ALERT")) ? "red" : "green", 3, 60, 12,
				(cmsl == 0 || sCMSlText.contains("ALERT")) ? true : false, false);
		sResult += "</tr>";

		return sResult;
	}

	String getRating() {
		String sResult = "";
		String sSQL;
		Statement st = null;
		ResultSet rs = null;
		String sStartDTM = "";
		int iErrors = 0;
		int iProcesses = 0;
		String sEndDTM = "";
		int iWaiting = 0;
		int iRating = 0;
		int iRejected = 0;
		int iOnHold = 0;
		int iDisabled = 0;
		int iUnknown = 0;
		int iCEWExpired = 0;
		int iCEWQueue = 0;
		int iSCBMErrors = 0;
		int iSCBMQueue = 0;
		int iSCEPErrors = 0;
		int iSCEPQueue = 0;
		int iBiCEPQueue = 0;
		int iBiCEPErrors = 0;

		iProcesses = getProcessCountSess("RATE");

		try {
			st = con.createStatement();
		} catch (SQLException e) {
			System.out.println("**** BILLD - Rating ERROR : " + e.getMessage());
			sResult = "";
		}

		if (iProcesses != 0) {
			sSQL = new String(
					"SELECT to_char(min(p.start_dtm),'YYYY-MM-DD HH24:MI:SS') as start_dtm, nvl(sum(p.total_errors),0) as errors "
							+ "FROM PROCESSLOG p WHERE p.PROCESS_DEF_ID IN (SELECT PROCESS_DEF_ID FROM PROCESSDEFINITION WHERE IMAGE_NAME = 'RATE') and p.end_dtm is null");

			try {

				rs = st.executeQuery(sSQL);
				while (rs.next()) {
					sStartDTM = rs.getString("start_dtm");
					iErrors = rs.getInt("errors");
					break;
				}
			} catch (SQLException e1) {
				System.out.println("**** BILLD - Rating ERROR : " + e1.getMessage());
				sResult = "";
			}
		} else {
			// Not running
			sSQL = new String(
					"SELECT nvl(to_char(p.start_dtm,'YYYY-MM-DD HH24:MI:SS'),' ')as start_dtm, nvl(to_char(p.end_dtm,'YYYY-MM-DD HH24:MI:SS'),' ') as end_dtm, nvl(p.total_errors,0) as errors FROM PROCESSLOG p WHERE p.PROCESS_DEF_ID IN (SELECT PROCESS_DEF_ID FROM PROCESSDEFINITION WHERE IMAGE_NAME = 'RATE') and p.end_dtm is not null order by 1 desc");

			try {
				rs = st.executeQuery(sSQL);
				while (rs.next()) {
					sStartDTM = rs.getString("start_dtm");
					sEndDTM = rs.getString("end_dtm");
					iErrors = rs.getInt("errors");
					break;
				}

			} catch (SQLException e1) {
				System.out.println("**** BILLD - Rating ERROR : " + e1.getMessage());
				sResult = "";
			}

		}

		sSQL = new String(
				"select DECODE(TO_CHAR(j.JOB_STATUS), '2', 'Waiting', '3', 'Rating', '5', 'Rejected', '88', 'On hold', '98', 'Disabled', '99', 'Disabled', 'Unknown') as job_type, count(*) as jobs\n"
						+ "from job j, jobhasfile jhf, jobtype jt\n" + "where j.JOB_TYPE_ID IN (1,23,24,34)\n"
						+ "and j.JOB_STATUS not in (4,7,8)\n" + "and jhf.JOB_ID = j.JOB_ID\n"
						+ "and jt.job_type_id = j.job_type_id\n"
						+ "group by DECODE(TO_CHAR(j.JOB_STATUS), '2', 'Waiting', '3', 'Rating', '5', 'Rejected', '88', 'On hold', '98', 'Disabled', '99', 'Disabled', 'Unknown')");

		try {
			rs = st.executeQuery(sSQL);
			while (rs.next()) {
				String sType = rs.getString("job_type");
				if (sType.contains("Waiting")) {
					iWaiting = rs.getInt("jobs");
				} else if (sType.contains("Rating")) {
					iRating = rs.getInt("jobs");
				} else if (sType.contains("Rejected")) {
					iRejected = rs.getInt("jobs");
				} else if (sType.contains("On hold")) {
					iOnHold = rs.getInt("jobs");
				} else if (sType.contains("Disabled")) {
					iDisabled = rs.getInt("jobs");
				} else if (sType.contains("Unknown")) {
					iUnknown = rs.getInt("jobs");
				}
			}

		} catch (SQLException e1) {
			System.out.println("**** BILLD - Rating ERROR : " + e1.getMessage());
			sResult = "";
		}

		sSQL = new String(
				"select count(*) as count from aq$costedeventqueuetail where msg_state = 'READY' and consumer_name = 'CEW'");

		try {
			rs = st.executeQuery(sSQL);
			while (rs.next()) {
				iCEWQueue = rs.getInt("count");
				break;
			}

		} catch (SQLException e1) {
			System.out.println("**** BILLD - Rating ERROR : " + e1.getMessage());
			sResult = "";
		}

		sSQL = new String(
				"select count(*) as count from aq$costedeventqueuetail where msg_state = 'EXPIRED' and consumer_name = 'CEW'");

		try {
			rs = st.executeQuery(sSQL);
			while (rs.next()) {
				iCEWExpired = rs.getInt("count");
				break;
			}

		} catch (SQLException e1) {
			System.out.println("**** BILLD - Rating ERROR : " + e1.getMessage());
			sResult = "";
		}

		sSQL = new String(
				"select count(*) as count from aq$costedeventqueuetail where msg_state = 'READY' and consumer_name = 'BICEP'");

		try {
			rs = st.executeQuery(sSQL);
			while (rs.next()) {
				iBiCEPQueue = rs.getInt("count");
				break;
			}

		} catch (SQLException e1) {
			System.out.println("**** BILLD - Rating ERROR : " + e1.getMessage());
			sResult = "";
		}

		sSQL = new String(
				"select count(*) as count from INTERFACESC.aq$o2spendcapevent_qtab where msg_state = 'READY'");

		try {
			rs = st.executeQuery(sSQL);
			while (rs.next()) {
				iSCEPQueue = rs.getInt("count");
				break;
			}

		} catch (SQLException e1) {
			System.out.println("**** BILLD - Rating ERROR : " + e1.getMessage());
			sResult = "";
		}

		sSQL = new String(
				"select count(*) as count from INTERFACESC.aq$o2spendcapbarcheck_qtab where msg_state = 'READY'");

		try {
			rs = st.executeQuery(sSQL);
			while (rs.next()) {
				iSCBMQueue = rs.getInt("count");
				break;
			}

		} catch (SQLException e1) {
			System.out.println("**** BILLD - Rating ERROR : " + e1.getMessage());
			sResult = "";
		}

		sSQL = new String("select count(*) as count from INTERFACESC.SCEP_ERROR_TAB");

		try {
			rs = st.executeQuery(sSQL);
			while (rs.next()) {
				iSCEPErrors = rs.getInt("count");
				break;
			}

		} catch (SQLException e1) {
			System.out.println("**** BILLD - Rating ERROR : " + e1.getMessage());
			sResult = "";
		}

		sSQL = new String("select count(*) as count from INTERFACESC.SCBM_ERROR_TAB");

		try {
			rs = st.executeQuery(sSQL);
			while (rs.next()) {
				iSCBMErrors = rs.getInt("count");
				break;
			}

		} catch (SQLException e1) {
			System.out.println("**** BILLD - Rating ERROR : " + e1.getMessage());
			sResult = "";
		}

		sSQL = new String("select count(*) as count from INTERFACECDRPP.BICEP_ERROR_TAB");

		try {
			rs = st.executeQuery(sSQL);
			while (rs.next()) {
				iBiCEPErrors = rs.getInt("count");
				break;
			}

		} catch (SQLException e1) {
			System.out.println("**** BILLD - Rating ERROR : " + e1.getMessage());
			sResult = "";
		}

		sResult = "<tr>" + buildTd("Status :", "right", "black", 1, 50, 12, false, false)
				+ buildTd(iProcesses > 0 ? "RUNNING" : "NOT RUNNING", "left", iProcesses > 0 ? "green" : "black", 3, 60,
						12, false, false)
				+ "</tr>" + "<tr>" + buildTd("Process Count :", "right", "black", 1, 50, 12, false, false)
				+ buildTd(Integer.toString(iProcesses), "left", "black", 3, 60, 12, false, false) + "</tr>" + "<tr>"
				+ buildTd("Start Dtm :", "right", "black", 1, 50, 12, false, false)
				+ buildTd(sStartDTM, "left", "black", 3, 60, 12, false, false) + "</tr>" + "<tr>"
				+ buildTd("End Dtm :", "right", "black", 1, 50, 12, false, false)
				+ buildTd(sEndDTM, "left", "black", 3, 60, 12, false, false) + "</tr>" + "<tr>"
				+ buildTd("Errors :", "right", "black", 1, 50, 12, false, false)
				+ buildTd(Integer.toString(iErrors), "left", iErrors > 0 ? "red" : "black", 3, 60, 12, false, false)
				+ "</tr>" + "<tr>" + buildTd("Jobs Waiting :", "right", "black", 1, 50, 12, false, false)
				+ buildTd(Integer.toString(iWaiting), "left", "black", 3, 60, 12, false, false) + "</tr>" + "<tr>"
				+ buildTd("Jobs Rating :", "right", "black", 1, 50, 12, false, false)
				+ buildTd(Integer.toString(iRating), "left", "black", 3, 60, 12, false, false) + "</tr>" + "<tr>"
				+ buildTd("Jobs Rejected :", "right", "black", 1, 50, 12, false, false)
				+ buildTd(Integer.toString(iRejected), "left", "black", 3, 60, 12, false, false) + "</tr>" + "<tr>"
				+ buildTd("Jobs On Hold :", "right", "black", 1, 50, 12, false, false)
				+ buildTd(Integer.toString(iOnHold), "left", "black", 3, 60, 12, false, false) + "</tr>" + "<tr>"
				+ buildTd("Jobs Disabled :", "right", "black", 1, 50, 12, false, false)
				+ buildTd(Integer.toString(iDisabled), "left", "black", 3, 60, 12, false, false) + "</tr>" + "<tr>"
				+ buildTd("Jobs Unknown :", "right", "black", 1, 50, 12, false, false)
				+ buildTd(Integer.toString(iUnknown), "left", iUnknown > 0 ? "red" : "black", 3, 60, 12, false, false)
				+ "</tr>" + "<tr>" + buildTd("CEW Queue :", "right", "black", 1, 50, 12, false, false)
				+ buildTd(Integer.toString(iCEWQueue), "left", iCEWQueue > 15000 ? "red" : "black", 1, 60, 12, false,
						false)
				+ buildTd("Expired :", "right", "black", 1, 50, 12, false, false)
				+ buildTd(Integer.toString(iCEWExpired), "left", iCEWExpired > 0 ? "red" : "black", 1, 60, 12, false,
						false)
				+ "</tr>" + "<tr>" + buildTd("BiCEP Queue :", "right", "black", 1, 50, 12, false, false)
				+ buildTd(Integer.toString(iBiCEPQueue), "left", "black", 1, 60, 12, false, false)
				+ buildTd("Errors :", "right", "black", 1, 50, 12, false, false)
				+ buildTd(Integer.toString(iBiCEPErrors), "left", iBiCEPErrors > 0 ? "red" : "black", 1, 60, 12, false,
						false)
				+ "</tr>" + "<tr>" + buildTd("SCEP Queue :", "right", "black", 1, 50, 12, false, false)
				+ buildTd(Integer.toString(iSCEPQueue), "left", "black", 1, 60, 12, false, false)
				+ buildTd("Errors :", "right", "black", 1, 50, 12, false, false)
				+ buildTd(Integer.toString(iSCEPErrors), "left", iSCEPErrors > 0 ? "red" : "black", 1, 60, 12, false,
						false)
				+ "</tr>" + "<tr>" + buildTd("SCBM Queue :", "right", "black", 1, 50, 12, false, false)
				+ buildTd(Integer.toString(iSCBMQueue), "left", "black", 1, 60, 12, false, false)
				+ buildTd("Errors :", "right", "black", 1, 50, 12, false, false)
				+ buildTd(Integer.toString(iSCBMErrors), "left", iSCBMErrors > 0 ? "red" : "black", 1, 60, 12, false,
						false)
				+ "</tr>";

		try {
			st.close();
		} catch (SQLException e) {
			System.out.println("**** BILLD - Rating ERROR : " + e.getMessage());
			sResult = "";
		}

		return sResult;
	}

	int getProcessCountTask(String sProcessName) {
		int iCount = 0;
		String sSQL;
		Statement st = null;
		ResultSet rs = null;

		sSQL = new String(
				"select count(*) from tasklog tl, task t where tl.task_id = t.task_id and tl.end_dtm is null and t.task_name like '");
		sSQL += sProcessName;
		sSQL += "%'";

		try {
			st = con.createStatement();
			rs = st.executeQuery(sSQL);
			while (rs.next()) {
				iCount = rs.getInt(1);
			}
			st.close();
		} catch (SQLException e3) {
			return 0;
		}

		return iCount;
	}
	
	int getProcessCountPL(String sProcessName) {
		int iCount = 0;
		String sSQL;
		Statement st = null;
		ResultSet rs = null;

		sSQL = new String(
				"select count(*) from processlog pl, processdefinition pd where pl.process_def_id = pd.process_def_id and pl.end_dtm is null and pd.image_name like '");
		sSQL += sProcessName;
		sSQL += "%'";

		try {
			st = con.createStatement();
			rs = st.executeQuery(sSQL);
			while (rs.next()) {
				iCount = rs.getInt(1);
			}
			st.close();
		} catch (SQLException e3) {
			return 0;
		}

		return iCount;
	}

	int getProcessCountSess(String sProcessName) {
		int iCount = 0;
		String sSQL;
		Statement st = null;
		ResultSet rs = null;

		sSQL = new String("select count(*) from v$session where program like '");
		sSQL += sProcessName;
		sSQL += "%'";

		try {
			st = con.createStatement();
			rs = st.executeQuery(sSQL);
			while (rs.next()) {
				iCount = rs.getInt(1);
			}
			st.close();
		} catch (SQLException e3) {
			return 0;
		}

		return iCount;
	}

	public RefreshThread(ServletContext context) {
		this.context = context;
		sContext = context.getRealPath("/");
		try {
			System.out.println("******** REGISTERED");
			DriverManager.registerDriver(d);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {

//    	while (!shutdown) {
		try {
			this.createFile();
		} catch (Exception e) {
			e.printStackTrace();
		}
		// }

	}

	public void shutdown() {
		try {
			System.out.println("******** DEREGISTERED");
			DriverManager.deregisterDriver(d);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void createFile() throws IOException {
		String sDashHtml = "";
		String sTasksHtml = "";
		String sBDBR2 = "";
		String sTasks = "";
		String sRunningHtml = "";
		String sRating2 = "";
		String sRBM2 = "";
		String sFS2 = "";

		String connStr = "jdbc:oracle:thin:@"
				+ sHost
				+ ":"
				+ sPort
				+ ":"
				+ sOraSID;

		sWarnings = "";

		File sLogFile = new File(sContext + "dashboard.log");
		PrintWriter pwLog = new PrintWriter(new FileWriter(sLogFile, false));

		try {
			con = DriverManager.getConnection(connStr, sUser, sPwd);

			if (con != null) {
				sDBStatus = "CONNECTED";
				sRefreshDTM = // getDashString("DASHBOARD", "UPDATE_DTM");
						getSQLString("SELECT TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') FROM DUAL");
			} else {
				sWarnings += "<li>" + "Unable to connect to Oracle DB" + "</li>\n";
				pwLog.print("Unable to connect to Oracle DB");
				pwLog.close();

				File sAlertFile = new File(sContext + "alerts.html.tmp");
				PrintWriter pwAlerts = new PrintWriter(new FileWriter(sAlertFile, false));

				pwAlerts.print("              <div class=\"item dropdown\">\n"
						+ "               <a href=\"#\" class=\"dropdown-toggle\">\n"
						+ "                  <i class=\"glyphicon glyphicon-alert\" style=\"color: red\"></i>\n"
						+ "                </a>\n"
						+ "                <ul class=\"dropdown-menu dropdown-menu-right\">\n"
						+ "                  <li class=\"dropdown-header\">\n" + "Tasks failed in the last 30 minutes\n"
						+ "                  </li>\n" + "                  <li class=\"divider\"></li>\n" + sWarnings
						+ "                </ul>\n" + "            </div>\n");
				pwAlerts.close();
				sAlertFile.renameTo(new File(sContext + "alerts.html"));

				return;
			}
		} catch (SQLException e1) {
			sWarnings += "<li>" + e1.getMessage() + "</li>\n";
			pwLog.print("**** BILLD - ERROR : " + e1.getMessage());
			pwLog.close();
			File sAlertFile = new File(sContext + "alerts.html.tmp");
			PrintWriter pwAlerts = new PrintWriter(new FileWriter(sAlertFile, false));

			pwAlerts.print("              <div class=\"item dropdown\">\n"
					+ "               <a href=\"#\" class=\"dropdown-toggle\">\n"
					+ "                  <i class=\"glyphicon glyphicon-alert\" style=\"color: red\"></i>\n"
					+ "                </a>\n" + "                <ul class=\"dropdown-menu dropdown-menu-right\">\n"
					+ "                  <li class=\"dropdown-header\">\n" + "Tasks failed in the last 30 minutes\n"
					+ "                  </li>\n" + "                  <li class=\"divider\"></li>\n" + sWarnings
					+ "                </ul>\n" + "            </div>\n");
			pwAlerts.close();
			sAlertFile.renameTo(new File(sContext + "alerts.html"));
			return;
		}

		// System.out.println("**** BILLD - Conext : " + sContext);

		sRBM2 += "	<div class=col-sm-3>\n" + "		<rd-widget>\n" + "		<rd-widget-header title=RBM>\n"
				+ "		</rd-widget-header>\n" + "		<rd-widget-body classes=\"medium no-padding\">\n"
				+ "		<div class=table-responsive>\n" + "			<table>\n" + "				<tbody>\n" + getRBM();

		sRating2 += "<div class=col-sm-3>\n" + "		<rd-widget>\n" + "		<rd-widget-header title=Rating>\n"
				+ "		</rd-widget-header>\n" + "		<rd-widget-body classes=\"medium no-padding\">\n"
				+ "		<div class=table-responsive>\n" + "			<table>\n" + "				<tbody>\n"
				+ getRating();

		sBDBR2 += "	<div class=col-sm-3>\n" + "		<rd-widget>\n" + "		<rd-widget-header title=BDBR>\n"
				+ "		</rd-widget-header>\n" + "		<rd-widget-body classes=\"medium no-padding\">\n"
				+ "		<div class=table-responsive>\n" + "			<table>\n" + "				<tbody>\n"
				+ getBDBRDetail();

		sFS2 += "	<div class=col-sm-3>\n" + "		<rd-widget>\n" + "		<rd-widget-header title=Filesystem>\n"
				+ "		</rd-widget-header>\n" + "		<rd-widget-body classes=\"medium no-padding\">\n"
				+ "		<div class=table-responsive>\n" + "			<table>\n" + "				<tbody>\n" + getFS();

		sRBM2 += "				</tbody>\n" + "			</table>\n" + "		</div>\n"
				+ "		</rd-widget-body></rd-widget>\n" + "	</div>\n";

		sRating2 += "				</tbody>\n" + "			</table>\n" + "		</div>\n"
				+ "		</rd-widget-body></rd-widget>\n" + "	</div>\n";

		sBDBR2 += "				</tbody>\n" + "			</table>\n" + "		</div>\n"
				+ "		</rd-widget-body></rd-widget>\n" + "	</div>\n";

		sFS2 += "				</tbody>\n" + "			</table>\n" + "		</div>\n"
				+ "		</rd-widget-body></rd-widget>\n" + "	</div>\n";

		sTasks = getCompletedTasks();

		sTasks += "				</tbody>\n" + "			</table>\n" + "		</div>\n"
				+ "		</rd-widget-body></rd-widget>\n" + "	</div>\n" + "</div>" + "</div>" + "<div class=row>\n"
				+ "	<div class=col-sm-12>\n" + "		<rd-widget>\n"
				+ "		<rd-widget-header title=\"Last Refreshed : " + sRefreshDTM + "\">\n"
				+ "</rd-widget-header>\n</rd-widget>\n	</div>\n</div>";

		sDashHtml += "<div class=row>\n" + sRBM2 + sRating2 + sBDBR2 + sFS2 + "</div>\n" + "<div class=row>\n"
				+ "	<div class=col-lg-12>\n" + "		<rd-widget>\n"
				+ "		<rd-widget-header title=\"Task Status\">\n" + "		</rd-widget-header>\n"
				+ "		<rd-widget-body classes=\"large no-padding\">\n" + "		<div class=table-responsive>\n"
				+ "			<table>\n" + "				<tbody>\n" + sTasks;

		sTasksHtml += "<div class=row>\n" + "	<div class=col-lg-12>\n" + "		<rd-widget>\n"
				+ "		<rd-widget-header title=\"Task Status\">\n" + "		</rd-widget-header>\n"
				+ "		<rd-widget-body classes=\"xlarge no-padding\">\n" + "		<div class=table-responsive>\n"
				+ "			<table>\n" + "				<tbody>\n" + sTasks;

		File sAlertFile = new File(sContext + "alerts.html.tmp");
		PrintWriter pwAlerts = new PrintWriter(new FileWriter(sAlertFile, false));

		if (sWarnings.length() > 0) {
			pwAlerts.print("              <div class=\"item dropdown\">\n"
					+ "               <a href=\"#\" class=\"dropdown-toggle\">\n"
					+ "                  <i class=\"glyphicon glyphicon-alert\" style=\"color: red\"></i>\n"
					+ "                </a>\n" + "                <ul class=\"dropdown-menu dropdown-menu-right\">\n"
					+ "                  <li class=\"dropdown-header\">\n" + "Tasks failed in the last 30 minutes\n"
					+ "                  </li>\n" + "                  <li class=\"divider\"></li>\n" + sWarnings
					+ "                </ul>\n" + "            </div>\n");
		} else {
			pwAlerts.print("              <div class=\"item dropdown\">\n"
					+ "               <a href=\"#\" class=\"dropdown-toggle\">\n"
					+ "                  <i class=\"glyphicon glyphicon-alert\" style=\"color: black\"></i>\n"
					+ "                </a>\n" + "                <ul class=\"dropdown-menu dropdown-menu-right\">\n"
					+ "                  <li class=\"dropdown-header\">\n" + "No tasks failed in the last 30 minutes\n"
					+ "                  </li>\n" + "                  <li class=\"divider\"></li>\n"
					+ "                </ul>\n" + "            </div>\n");
		}

		pwAlerts.close();
		File sDashFile = new File(sContext + "dashboard.html.tmp");
		PrintWriter pwDash = new PrintWriter(new FileWriter(sDashFile, false));
		File sTaskFile = new File(sContext + "tasks.html.tmp");
		PrintWriter pwTask = new PrintWriter(new FileWriter(sTaskFile));
		pwDash.println(sDashHtml);
		pwDash.close();
		pwTask.println(sTasksHtml);
		pwTask.close();

		sAlertFile.renameTo(new File(sContext + "alerts.html"));
		sDashFile.renameTo(new File(sContext + "dashboard.html"));
		sTaskFile.renameTo(new File(sContext + "tasks.html"));

		sRunningHtml = "<div class=row>\n" + "	<div class=col-lg-12>\n" + "		<rd-widget>\n"
				+ "		<rd-widget-header title=\"Running Tasks\">\n" + "		</rd-widget-header>\n"
				+ "		<rd-widget-body classes=\"xlarge no-padding\">\n" + "		<div class=table-responsive>\n"
				+ "			<table>\n" + "				<tbody>\n"
				// tasks
				+ "<tr> " + buildTd("Start Time", "left", "black", 1, 60, 12, false, false)
				+ buildTd("Task", "left", "black", 1, 120, 12, false, false)
				+ buildTd("Process Name", "left", "black", 1, 120, 12, false, false)
				+ buildTd("Errors", "left", "black", 1, 60, 12, false, false)
				+ buildTd("Run Time (mins)", "left", "black", 1, 60, 12, false, false) + "</tr>" + getRunningTasks()
				+ "				</tbody>\n" + "			</table>\n" + "		</div>\n"
				+ "		</rd-widget-body></rd-widget>\n" + "	</div>\n" + "</div>" + "</div>" + "<div class=row>\n"
				+ "	<div class=col-sm-12>\n" + "		<rd-widget>\n"
				+ "		<rd-widget-header title=\"Last Refreshed : " + sRefreshDTM + "\">\n"
				+ "</rd-widget-header>\n</rd-widget>\n	</div>\n</div>";

		File sRunningFile = new File(sContext + "running.html.tmp");
		PrintWriter pwRunning = new PrintWriter(new FileWriter(sRunningFile));
		pwRunning.println(sRunningHtml);
		pwRunning.close();
		sRunningFile.renameTo(new File(sContext + "running.html"));

		try {
			if (con != null)
				con.close();
			// System.out.println("**** BILLD - Disconnected from db");

		} catch (SQLException e1) {
			sWarnings += "<li>" + e1.getMessage() + "</li>\n";
			pwLog.print("**** BILLD - ERROR : " + e1.getMessage());
		}

		pwLog.close();
	}

}

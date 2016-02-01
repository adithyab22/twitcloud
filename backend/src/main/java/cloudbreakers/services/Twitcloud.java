package cloudbreakers.services;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import cloudbreakers.pojos.DataSourceUtil;
import cloudbreakers.pojos.DataSourceUtil.DataSourceType;
import cloudbreakers.pojos.PDC_Encryption;
import cloudbreakers.pojos.ScoreCalculatorAndCensor;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

public class Twitcloud extends AbstractVerticle {

	public static Logger logger = Logger.getLogger(Twitcloud.class.getName());

	private static final String ACCOUNT_DETAIL = "CloudBreakers3,7716-4451-0934";
	private static final SimpleDateFormat MYSQL_DF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
	private static final SimpleDateFormat FULL_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	/**
	 * DB Settings
	 * 
	 */

	// private static final DataSourceType DATA_SOURCE =
	// DataSourceType.HBASE_DATA_SOURCE;
	// private static final String DS_HOST_IP =
	// "ec2-54-175-210-197.compute-1.amazonaws.com"; // hbase 54.175.210.197
	// private static final String DS_PORT = "2181";

	private static final DataSourceType DATA_SOURCE = DataSourceType.SQL_DATA_SOURCE;
	private static final String DS_HOST_IP = "127.0.0.1"; // sql
	private static final String DS_PORT = "3306";
	private static final String DS_USER = "appuser";
	private static final String DS_PASS = "pass123";
	private static final String DS_DB_NAME = "tweet_db";
	private static final int SQL_MIN_CONN = 50;
	private static final int SQL_MAX_CONN = 20000;
	private static final int SQL_INCREMENT_CONN = 10;

	/**
	 * SQL Queries
	 */
	private static final String Q2 = "SELECT tweet_id, content, score FROM tweets_q2 WHERE user_id_date=?;";

	private static final String Q3 = ""
			+ "select date, followers, tweet_id, content "
			+ "from tweets_q3 " + "where user_id=? " + "and date between ? and ?;";

	private static final String Q3_POSITIVE = ""
			+ "select date, score*(1+followers) as impact_score, tweet_id, content "
			+ "from tweets_q3 " + "where user_id=? " + "and date between ? and ? "
			+ "order by impact_score desc, tweet_id limit ?;";

	private static final String Q3_NEGATIVE = ""
			+ "select date, score*(1+followercount) as impact_score, tweet_id, content "
			+ "from tweets_q3 " + "where user_id=? " + "and date between ? and ? "
			+ "order by impact_score, tweet_id limit ?;";

	private static final byte[] TABLE_NAME = Bytes.toBytes("tweets");
	private static final byte[] COL_FAMILY_DATA = Bytes.toBytes("data");
	private static final byte[] COL_USER_ID = Bytes.toBytes("user_id");
	private static final byte[] COL_TIME_STAMP = Bytes.toBytes("time_stamp");
	private static final byte[] COL_TEXT = Bytes.toBytes("text");
	private static final byte[] COL_SCORE = Bytes.toBytes("score");
	private static final byte[] COL_FCOUNT = Bytes.toBytes("fcount");
	private static final byte[] COL_HASTTAGS = Bytes.toBytes("hashtags");

	private static final String SQL_COL_TWEET_ID = "tweet_id";
	private static final String SQL_COL_CONTENT = "content";
	private static final String SQL_COL_TWEET_TIME = "tweet_time";
	private static final String SQL_COL_FOLL_COUNT = "followercount";
	private static final String SQL_COL_SCORE = "score";
	private static final String SQL_COL_IMPACT_SCORE = "impact_score";

	/**
	 * Constants
	 */
	private static final char COLON = ':';
	private static final char COMMA = ',';
	private static final char UNDERSCORE = '_';
	private static final char NEWLINE = '\n';
	private static final Long ONE_DAY_MSEC = 86400000L;

	static {

		logger.setLevel(Level.INFO);
		MYSQL_DF.setTimeZone(TimeZone.getTimeZone("UTC"));
		DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
		FULL_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));

		FileHandler handler;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			handler = new FileHandler("server.log");
			handler.setFormatter(new SimpleFormatter());
			logger.addHandler(handler);
		} catch (Exception e) {
			logger.log(Level.SEVERE, null, e);
		}
	}

	@Override
	public void start(Future<Void> fut) {

		VertxOptions options = new VertxOptions();
		options.setMaxEventLoopExecuteTime(Long.MAX_VALUE);
		vertx = Vertx.vertx(options);

		Router router = Router.router(vertx);

		// Bind "/" to our hello message - so we are still compatible.
		router.route("/").handler(routingContext -> {
			HttpServerResponse response = routingContext.response();
			response.putHeader("content-type", "text/html").end("<h1>This server is run by CloudBreakers</h1>");
		});

		router.get("/q1").handler(this::executeQuery1);
		router.get("/q2").handler(this::executeQuery2);
		router.get("/q3").handler(this::executeQuery3);
//		router.get("/q4").handler(this::executeQuery4);

		// Create the HTTP server and pass the "accept" method to the request
		// handler.
		vertx.createHttpServer().requestHandler(router::accept).listen(config().getInteger("http.port", 80), result -> {
			if (result.succeeded()) {
				fut.complete();
			} else {
				fut.fail(result.cause());
			}
		});
	}

	private void executeQuery1(RoutingContext routingContext) {

		MultiMap map = routingContext.request().params();
		String key = map.get("key");
		String message = map.get("message");
		StringBuilder response = new StringBuilder();

		response.append(ACCOUNT_DETAIL).append(DATE_FORMAT.format(new Date())).append(NEWLINE)
				.append(PDC_Encryption.decrypt(key, message)).append(NEWLINE);

		routingContext.response().putHeader("content-type", "text/plain").end(response.toString());
	}

	private void executeQuery2(RoutingContext routingContext) {

		MultiMap map = routingContext.request().params();
		final String userID = map.get("userid");
		final String tweetTime = map.get("tweet_time");
		new Thread(new Runnable() {
			public void run() {
				String response = executeQuery2FromSQL(userID, tweetTime);
				routingContext.response().putHeader("content-type", "text/plain; charset=utf-8").end(response);
			}
		}).start();
	}

	private void executeQuery3(RoutingContext routingContext) {

		MultiMap map = routingContext.request().params();
		String userID = map.get("userid");
		String startDate = map.get("start_date");
		String endDate = map.get("end_date");
		String n = map.get("n");
		String response = "";
		response = executeQuery3FromSQL(userID, startDate, endDate, Integer.parseInt(n));
		routingContext.response().putHeader("content-type", "text/plain; charset=utf-8").end(response);
	}

	/**
	 * 
	 * @param userId
	 * @param tweetTime
	 * @return
	 */
	private String executeQuery2FromSQL(String userId, String tweetTime) {

		StringBuilder result = new StringBuilder();
		result.append(ACCOUNT_DETAIL).append(NEWLINE);

		Connection conn = null;

		try {
			conn = DataSourceUtil.getSQLConnectionPool(DATA_SOURCE, DS_HOST_IP, DS_PORT, DS_USER, DS_PASS, DS_DB_NAME,
					SQL_MIN_CONN, SQL_MAX_CONN, SQL_INCREMENT_CONN).getConnection();

			if (tweetTime.isEmpty() || userId.isEmpty()) {
				conn.close();
				return result.toString();
			}

			String date = MYSQL_DF.format(new Date(dateStringToTs(tweetTime)));

			StringBuilder sb = new StringBuilder();
			sb.append(userId).append(UNDERSCORE).append(date);

			PreparedStatement statement = conn.prepareStatement(Q2);
			statement.setString(1, sb.toString());
			
			ResultSet results = statement.executeQuery();

			while (results.next()) {

				String text = results.getString(SQL_COL_CONTENT);
				long tweetId = results.getLong(SQL_COL_TWEET_ID);
				int score = results.getInt(SQL_COL_SCORE);

				System.out.println(sb.toString());
				System.out.println(text);
				
				text = StringEscapeUtils.unescapeJava(text);
				text = StringEscapeUtils.unescapeJava(text);

				result.append(tweetId).append(COLON).append(score).append(COLON).append(text).append(NEWLINE);
			}
			statement.close();
			results.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {

			}
		}
		return result.toString();
	}

	private String executeQuery3FromSQL(String userID, String startDate, String endDate, int n) {

		StringBuilder result = new StringBuilder();
		result.append(ACCOUNT_DETAIL).append(NEWLINE);

		Connection conn = null;
		try {
			conn = DataSourceUtil.getSQLConnectionPool(DATA_SOURCE, DS_HOST_IP, DS_PORT, DS_USER, DS_PASS, DS_DB_NAME,
					SQL_MIN_CONN, SQL_MAX_CONN, SQL_INCREMENT_CONN).getConnection();

			result.append("Positive Tweets").append(NEWLINE);

			Long startTs = dateStringToTsDayLevel(startDate);
			Long endTs = dateStringToTsDayLevel(endDate) + ONE_DAY_MSEC;

			/*
			 * TreeSet<Query3Entity> entities = new TreeSet<>();
			 * PreparedStatement stmt = conn.prepareStatement(Q3);
			 * stmt.setLong(1, Long.parseLong(userID)); stmt.setString(2,
			 * startTs.toString()); stmt.setString(3, endTs.toString());
			 * 
			 * ResultSet rs = stmt.executeQuery(); while (rs.next()) {
			 * 
			 * String text = rs.getString(SQL_COL_CONTENT); text =
			 * StringEscapeUtils.unescapeJava(text); text =
			 * StringEscapeUtils.unescapeJava(text); text =
			 * StringEscapeUtils.unescapeJava(text);
			 * 
			 * int fCount = rs.getInt(SQL_COL_FOLL_COUNT); int score =
			 * ScoreCalculatorAndCensor.calculateSentimentScore(text); int
			 * impactScore = score*(1 + fCount);
			 * 
			 * Query3Entity entity = new
			 * Query3Entity(Long.parseLong(rs.getString(SQL_COL_TWEET_ID)),
			 * text, impactScore, rs.getString(SQL_COL_TWEET_TIME));
			 * 
			 * entities.add(entity); } stmt.close(); rs.close();
			 * TreeSet<Query3Entity> topPositive = new TreeSet<>();
			 * TreeSet<Query3Entity> topNegative = new TreeSet<>();
			 * 
			 * Iterator<Query3Entity> itr = entities.iterator();
			 * 
			 * int count = 0; while (itr.hasNext()) { Query3Entity current =
			 * itr.next(); if (count < n && current.impactScore > 0) {
			 * topPositive.add(current); } else { break; } count++; }
			 * 
			 * while(itr.hasNext() && itr.next().impactScore >= 0) { }
			 * 
			 * count = 0; while(itr.hasNext()) { if(count < n) {
			 * topNegative.add(itr.next()); } else { break; } count++; }
			 * 
			 * result.append("Positive Tweets").append(NEWLINE);
			 * 
			 * // date_pos1,impact_score_pos1,tweetid,censored_tweet_text\n
			 * 
			 * for (Query3Entity e : topPositive) {
			 * 
			 * result.append(e.date).append(COMMA).append(e.impactScore).append(
			 * COMMA).append(e.tweetId).append(COMMA)
			 * .append(e.text).append(NEWLINE); }
			 * 
			 * result.append("Negative Tweets").append(NEWLINE);
			 * 
			 * for (Query3Entity e : topNegative) {
			 * 
			 * result.append(e.date).append(COMMA).append(e.impactScore).append(
			 * COMMA).append(e.tweetId).append(COMMA)
			 * .append(e.text).append(NEWLINE); }
			 * 
			 * 
			 */
			PreparedStatement stmt = conn.prepareStatement(Q3_POSITIVE);
			stmt.setLong(1, Long.parseLong(userID));
			stmt.setString(2, startTs.toString());
			stmt.setString(3, endTs.toString());
			stmt.setInt(4, n);

			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {

				String text = rs.getString(SQL_COL_CONTENT);
				text = StringEscapeUtils.unescapeJava(text);
				text = StringEscapeUtils.unescapeJava(text);
				text = StringEscapeUtils.unescapeJava(text);

				// int fCount = rs.getInt(SQL_COL_FOLL_COUNT);
				// int score =
				// ScoreCalculatorAndCensor.calculateSentimentScore(text);
				int impactScore = rs.getInt("impact_score");

				result.append(rs.getString(SQL_COL_TWEET_TIME)).append(COMMA).append(impactScore).append(COMMA)
						.append(rs.getString(SQL_COL_TWEET_ID)).append(COMMA).append(text).append(NEWLINE);
			}

			stmt.close();
			rs.close();
			result.append("Negative Tweets").append(NEWLINE);

			PreparedStatement stmt2 = conn.prepareStatement(Q3_NEGATIVE);
			stmt2.setInt(1, Integer.parseInt(userID));
			stmt2.setString(2, startTs.toString());
			stmt2.setString(3, endTs.toString());
			stmt2.setInt(4, n);

			ResultSet rs_neg = stmt2.executeQuery();
			while (rs_neg.next()) {

				String text = rs_neg.getString(SQL_COL_CONTENT);
				text = StringEscapeUtils.unescapeJava(text);
				text = StringEscapeUtils.unescapeJava(text);
				text = StringEscapeUtils.unescapeJava(text);

				// t fCount = rs_neg.getInt(SQL_COL_FOLL_COUNT);
				// t score =
				// ScoreCalculatorAndCensor.calculateSentimentScore(text);
				int impactScore = rs_neg.getInt("impact_score");

				result.append(rs_neg.getString(SQL_COL_TWEET_TIME)).append(COMMA).append(impactScore).append(COMMA)
						.append(rs_neg.getString(SQL_COL_TWEET_ID)).append(COMMA).append(text).append(NEWLINE);
			}
			stmt2.close();
			rs_neg.close();

		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {

			}
		}

		return result.toString();
	}

	public static Long dateStringToTs(String date) throws ParseException {
		// 2014-05-31+01:29:04
		return FULL_DATE_FORMAT.parse(date).getTime();
	}

	public static Long dateStringToTsDayLevel(String date) throws ParseException {
		// 2014-05-31
		return DATE_FORMAT.parse(date).getTime();
	}

	public static String dateTsToString(Long ts) throws ParseException {
		// 2014-05-31+01:29:04
		return FULL_DATE_FORMAT.format(new Date(ts));
	}

	public static String dateTsToStringDayLevel(Long ts) throws ParseException {
		// 2014-05-31
		return DATE_FORMAT.format(new Date(ts));
	}

	private class Query3Entity implements Comparable<Query3Entity> {

		private Long tweetId;
		private String text;
		private int impactScore;
		private String date;

		public Query3Entity(Long tweetId, String text, int impactScore, String date) {
			this.tweetId = tweetId;
			this.text = text;
			this.impactScore = impactScore;
			this.date = date;
		}

		@Override
		public boolean equals(Object arg0) {

			Query3Entity that = (Query3Entity) arg0;
			if (that.impactScore == this.impactScore) {
				return this.tweetId.equals(that.tweetId);
			} else {
				return false;
			}
		}

		@Override
		public int compareTo(Query3Entity that) {

			if (that.impactScore == this.impactScore) {
				return -1 * that.tweetId.compareTo(this.tweetId);
			} else {
				if (that.impactScore < 0 && this.impactScore < 0) {
					return (that.impactScore > this.impactScore) ? -1 : 1;
				} else {
					return (that.impactScore > this.impactScore) ? 1 : -1;
				}
			}
		}
	}

}

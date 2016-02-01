package cloudbreakers.services;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import cloudbreakers.pojos.PDC_Encryption;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class Twitcloud1 extends AbstractVerticle {

	public static Logger logger = Logger.getLogger(Twitcloud1.class.getName());
	UnicodeUnescaper uu = new UnicodeUnescaper();

	private static final String ACCOUNT_DETAIL = "CloudBreakers3,7716-4451-0934";
	private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
	private static final SimpleDateFormat FULL_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static final SimpleDateFormat MYSQL_DF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	// /**
	// * DB Settings
	// *
	// */

	private static final String DS_HOST_IP = "127.0.0.1"; // sql
	private static final String DS_PORT = "3306";
	private static final String DS_USER = "appuser";
	private static final String DS_PASS = "pass123";
	private static final String DS_DB_NAME = "tweet_db";

	private static final String Q2 = "SELECT tweet_id, content, score FROM tweets_q2 WHERE user_id_date=?;";

	private static final String Q3 = "select tweet_time, score, followers, tweet_id, content "
			+ "from tweets_q3 " + "where user_id=? " + "and tweet_time between ? and ?;";

	private static final String Q4 = "select * from tweets_q4 where binary hastag=?;";

	private static final String Q5_1 = "select count_follower from tweets_q5 where user_id < ? order by user_id desc limit 1;";
	private static final String Q5_2 = "select count_follower from tweets_q5 where user_id <=? order by user_id desc limit 1;";

	private static final String SQL_COL_TWEET_ID = "tweet_id";
	private static final String SQL_COL_CONTENT = "content";
	private static final String SQL_COL_TWEET_TIME = "tweet_time";
	private static final String SQL_COL_FOLL_COUNT = "followercount";
	private static final String SQL_COL_SCORE = "score";
	private static final String SQL_COL_IMPACT_SCORE = "impact_score";

	private static final String COLON = ":";
	private static final String COMMA = ",";
	private static final String NEWLINE = "\n";
	private static final Long ONE_DAY_MSEC = 86400000L;
	private static final JsonObject config = new JsonObject()
			.put("url", "jdbc:mysql://" + DS_HOST_IP + ":" + DS_PORT + "/" + DS_DB_NAME)
			.put("driver_class", "com.mysql.jdbc.Driver").put("user", DS_USER).put("password", DS_PASS)
			.put("max_pool_size", 800);
	JedisPool pool = new JedisPool(new JedisPoolConfig(), "localhost");
	final Jedis jedis = pool.getResource();
	private JDBCClient client;

	long min_count = 0;
	long max_count = 0;

	static {

		logger.setLevel(Level.OFF);
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

		client = JDBCClient.createShared(vertx, config);
		Router router = Router.router(vertx);
		router.route("/").handler(routingContext -> {
			HttpServerResponse response = routingContext.response();
			response.putHeader("content-type", "text/html").end("<h1>This server is run by CloudBreakers</h1>");
		});

		router.get("/q1").handler(this::executeQuery1);
		router.get("/q2").handler(this::executeQuery2);
		router.get("/q3").handler(this::executeQuery3);
		router.get("/q4").handler(this::executeQuery4);
		router.get("/q5").handler(this::executeQuery5);
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

		response.append(ACCOUNT_DETAIL).append(DATE_FORMAT.format(new Date())).append("\n")
				.append(PDC_Encryption.decrypt(key, message)).append("\n");

		routingContext.response().putHeader("content-type", "text/plain").end(response.toString());
	}

	/**
	 * Query Two Handler
	 * 
	 * @param routingContext
	 */
	private void executeQuery2(RoutingContext routingContext) {
		final String user_id = routingContext.request().getParam("userid");
		String tt = routingContext.request().getParam("tweet_time");
		// Long timestamp = dateStringToTs(tt);
		String date = MYSQL_DF.format(new Date(dateStringToTs(tt)));
		final String key = user_id + "_" + date;
		if (jedis.exists(key)) {
			routingContext.response().setStatusCode(200).putHeader("content-type", "text/plain; charset=utf-8")
					.end(jedis.get(key).toString());
		} else {
			client.getConnection(ar -> {
				SQLConnection connection = ar.result();
				q2(key, jedis, connection, result -> {
					if (result.succeeded()) {
						routingContext.response().setStatusCode(200)
								.putHeader("content-type", "text/plain; charset=utf-8").end(result.result());
					} else {
						routingContext.response().setStatusCode(404).end();
					}
					connection.close();
				});
			});
		}
		// }
	}

	/**
	 * Query Two
	 * 
	 * @param user_id
	 * @param tweet_time
	 * @param jedis
	 * @param connection
	 * @param resultHandler
	 */
	private void q2(String user_id_date, Jedis jedis, SQLConnection connection,
			Handler<AsyncResult<String>> resultHandler) {
		connection.queryWithParams(Q2, new JsonArray().add(user_id_date), ar -> {
			if (ar.failed()) {
				resultHandler.handle(Future.failedFuture("Row not found"));
			} else {
				if (ar.result().getNumRows() >= 1) {

					StringBuilder stringBuilder = new StringBuilder();
					stringBuilder.append(ACCOUNT_DETAIL).append(NEWLINE);

					ResultSet resultSet = ar.result();
					List<JsonArray> results = resultSet.getResults();
					for (JsonArray next : results) {
						// String tt = next.getLong(0) + ":" + next.getLong(2) +
						// ":"+
						// StringEscapeUtils.unescapeJava(next.getString(1))+
						// "\n";
						String tt = next.getLong(0) + ":" + next.getLong(2) + ":"
								+ uu.unescapeJavaString(next.getString(1)) + "\n";
						// System.out.println(tt);
						stringBuilder.append(tt);

					}
					jedis.set(user_id_date, stringBuilder.toString());
					resultHandler.handle(Future.succeededFuture(stringBuilder.toString()));
				} else {
					resultHandler.handle(Future.failedFuture("Row not found"));
				}
			}
		});

	}

	/**
	 * 
	 * @param routingContext
	 */
	private void executeQuery3(RoutingContext routingContext) {

		MultiMap map = routingContext.request().params();
		String userID = map.get("userid");
		String startDate = map.get("start_date");
		String endDate = map.get("end_date");
		String n = map.get("n");
		String key = userID + "-" + startDate + "-" + endDate + "-" + n;
		if (jedis.exists(key)) {
			routingContext.response().setStatusCode(200).putHeader("content-type", "text/plain; charset=utf-8")
					.end(jedis.get(key).toString());
		} else {
			client.getConnection(ar -> {
				SQLConnection connection = ar.result();
				q3(userID, startDate, endDate, Integer.parseInt(n), connection, result -> {
					if (result.succeeded()) {
						routingContext.response().setStatusCode(200)
								.putHeader("content-type", "text/plain; charset=utf-8").end(result.result());
					} else {
						routingContext.response().setStatusCode(404).end();
					}
					connection.close();
				});
			});
		}
	}

	/**
	 * 
	 * @param user_id
	 * @param start_date
	 * @param end_date
	 * @param n
	 * @param connection
	 * @param resultHandler
	 */
	private void q3(String user_id, String start_date, String end_date, int n, SQLConnection connection,
			Handler<AsyncResult<String>> resultHandler) {

		String startDate = start_date + " 00:00:00";
		String endDate = getNextDay(end_date) + " 00:00:00";

		Long start = System.currentTimeMillis();
		connection.queryWithParams(Q3, new JsonArray().add(user_id).add(startDate).add(endDate), ar -> {
			if (ar.failed()) {
				resultHandler.handle(Future.failedFuture("Row not found"));
			} else {
				StringBuilder stringBuilder = new StringBuilder();
				stringBuilder.append(ACCOUNT_DETAIL).append(NEWLINE);
				if (ar.result().getNumRows() >= 1) {
					ResultSet resultSet = ar.result();
					List<JsonArray> results = resultSet.getResults();

					// System.out.println(System.currentTimeMillis()-start);

					TreeSet<Query3Entity> entities = new TreeSet<>();
					for (JsonArray next : results) {

						Query3Entity entity = new Query3Entity(next.getLong(3),
								uu.unescapeJavaString(next.getString(4)), next.getInteger(1) * (1 + next.getInteger(2)),
								next.getString(0).substring(0, 10));

						entities.add(entity);
					}

					TreeSet<Query3Entity> topPositive = new TreeSet<>();
					TreeSet<Query3Entity> topNegative = new TreeSet<>();

					Iterator<Query3Entity> itr = entities.iterator();

					Query3Entity current = null;
					int count = 0;
					while (itr.hasNext()) {
						current = itr.next();
						if (count < n && current.impactScore > 0) {
							topPositive.add(current);
						} else {
							break;
						}
						count++;
					}

					if (current != null && current.impactScore >= 0) {
						while (itr.hasNext()) {
							current = itr.next();
							if (current.impactScore < 0)
								break;
						}
					}

					count = 0;
					if (current != null && current.impactScore < 0) {
						topNegative.add(current);
						count++;
					}
					while (itr.hasNext()) {
						current = itr.next();
						if (count < n) {
							topNegative.add(current);
						} else {
							break;
						}
						count++;
					}

					stringBuilder.append("Positive Tweets").append(NEWLINE);

					// date_pos1,impact_score_pos1,tweetid,censored_tweet_text\n

					for (Query3Entity e : topPositive) {

						stringBuilder.append(e.date).append(COMMA).append(e.impactScore).append(COMMA).append(e.tweetId)
								.append(COMMA).append(e.text).append(NEWLINE);
					}

					stringBuilder.append(NEWLINE).append("Negative Tweets").append(NEWLINE);

					for (Query3Entity e : topNegative) {

						stringBuilder.append(e.date).append(COMMA).append(e.impactScore).append(COMMA).append(e.tweetId)
								.append(COMMA).append(e.text).append(NEWLINE);
					}

					jedis.set(user_id + "-" + start_date + "-" + end_date + "-" + n, stringBuilder.toString());
					resultHandler.handle(Future.succeededFuture(stringBuilder.toString()));
				} else {
					stringBuilder.append("Positive Tweets").append(NEWLINE);
					stringBuilder.append(NEWLINE).append("Negative Tweets").append(NEWLINE);
					resultHandler.handle(Future.succeededFuture(stringBuilder.toString()));
				}
			}
		});
	}

	private void executeQuery4(RoutingContext routingContext) {

		MultiMap map = routingContext.request().params();
		String hashTag = map.get("hashtag");
		String n = map.get("n");
		String key = hashTag + "-" + n;
		// if (jedis.exists(key)) {
		// routingContext.response().setStatusCode(200).putHeader("content-type",
		// "text/plain; charset=utf-8")
		// .end(jedis.get(key).toString());
		// } else {
		client.getConnection(ar -> {
			SQLConnection connection = ar.result();
			q4(hashTag, Integer.parseInt(n), connection, result -> {
				if (result.succeeded()) {
					routingContext.response().setStatusCode(200).putHeader("content-type", "text/plain; charset=utf-8")
							.end(result.result());
				} else {
					routingContext.response().setStatusCode(404).end();
				}
				connection.close();
			});
		});
		// }
	}

	private void q4(String hashtag, int n, SQLConnection connection, Handler<AsyncResult<String>> resultHandler) {

		Long start = System.currentTimeMillis();
		connection.queryWithParams(Q4, new JsonArray().add(hashtag), ar -> {
			if (ar.failed()) {
				resultHandler.handle(Future.failedFuture("Row not found"));
			} else {
				StringBuilder stringBuilder = new StringBuilder();
				stringBuilder.append(ACCOUNT_DETAIL).append(NEWLINE);

				if (ar.result().getNumRows() >= 1) {

					ResultSet resultSet = ar.result();
					List<JsonArray> results = resultSet.getResults();

					// System.out.println(System.currentTimeMillis()-start);

					Map<Date, List<Long>> dateToUsers = new HashMap<>(100);
					Map<Date, String> dateToText = new HashMap<>(100);
					Map<Date, Date> dateToMinDate = new HashMap<>();

					for (JsonArray next : results) {

						Date date = new Date(next.getLong(2));

						Date dayDate = date;
						dayDate.setHours(0);
						dayDate.setMinutes(0);
						dayDate.setSeconds(0);

						Date minDate = dateToMinDate.get(dayDate);
						if (minDate == null)
							minDate = date;

						if (date.before(minDate))
							dateToMinDate.put(dayDate, date);

						dateToText.put(date, next.getString(0));

						List<Long> userIds = dateToUsers.get(dayDate);
						if (userIds == null) {
							userIds = new LinkedList<Long>();
						}

						userIds.add(next.getLong(1));
						dateToUsers.put(dayDate, userIds);
					}

					for (Date key : dateToMinDate.keySet()) {
						System.out.println(key + ":" + dateToMinDate.get(key));
					}

					TreeSet<HashTagData> dateObjs = new TreeSet<>();
					for (Date key : dateToUsers.keySet()) {

						HashTagData obj = new HashTagData(key, dateToUsers.get(key).size(),
								dateToText.get(dateToMinDate.get(key)));
						dateObjs.add(obj);
					}

					int i = 0;
					for (HashTagData obj : dateObjs) {

						if (i++ == n) {
							break;
						}

						stringBuilder.append(DATE_FORMAT.format(obj.date)).append(COLON).append(obj.count)
								.append(COLON);

						List<Long> userList = dateToUsers.get(obj.date);
						Collections.sort(userList);
						LinkedHashSet<Long> users = new LinkedHashSet<Long>(userList);
						Iterator<Long> itr = users.iterator();
						while (itr.hasNext()) {
							stringBuilder.append(itr.next());
							if (itr.hasNext())
								stringBuilder.append(COMMA);
						}
						stringBuilder.append(COLON).append(obj.sourceTweet).append(NEWLINE);
					}

					jedis.set(hashtag + "-" + n, stringBuilder.toString());
					resultHandler.handle(Future.succeededFuture(stringBuilder.toString()));
				} else {
					System.out.println("no rows");
					resultHandler.handle(Future.succeededFuture(stringBuilder.toString()));
				}
			}
		});
	}

	private static class HashTagData implements Comparable<HashTagData> {

		Date date;
		int count;
		String sourceTweet;

		public HashTagData(Date date, int count, String sourceTweet) {
			this.date = date;
			this.count = count;
			this.sourceTweet = sourceTweet;
		}

		@Override
		public int compareTo(HashTagData that) {

			if (that.count == this.count) {
				return that.date.compareTo(this.date);
			} else {
				return new Integer(that.count).compareTo(this.count);
			}
		}
	}

	private void executeQuery5(RoutingContext routingContext) {

		MultiMap map = routingContext.request().params();
		String userid_min = map.get("userid_min");
		String userid_max = map.get("userid_max");

		String key = userid_min + "-" + userid_max;
		if (jedis.exists(key)) {
			
			routingContext.response().setStatusCode(200).putHeader("content-type", "text/plain; charset=utf-8")
					.end(jedis.get(key).toString());
		} else {
			client.getConnection(ar -> {
				SQLConnection connection = ar.result();
				q5_1(userid_min, connection, result -> {
					if (result.succeeded()) {
						min_count = Long.parseLong(result.result());
					} else {
						routingContext.response().setStatusCode(404).end();
					}
					connection.close();
				});
			});

			client.getConnection(ar -> {
				SQLConnection connection = ar.result();
				q5_2(userid_max, connection, result -> {
					if (result.succeeded()) {
						max_count = Long.parseLong(result.result());
					} else {
						routingContext.response().setStatusCode(404).end();
					}
					connection.close();
				});
			});

			StringBuilder stringBuilder = new StringBuilder();
			stringBuilder.append(ACCOUNT_DETAIL).append(NEWLINE).append(max_count - min_count).append(NEWLINE);

			routingContext.response().setStatusCode(200).putHeader("content-type", "text/plain; charset=utf-8")
					.end(stringBuilder.toString());
		}
	}

	/**
	 * 
	 * @param user_min
	 * @param user_max
	 * @param connection
	 * @param resultHandler
	 */
	private void q5_1(String user_min, SQLConnection connection, Handler<AsyncResult<String>> resultHandler) {

		connection.queryWithParams(Q5_1, new JsonArray().add(user_min), ar -> {
			if (ar.failed()) {
				resultHandler.handle(Future.failedFuture("Row not found"));
			} else {
				if (ar.result().getNumRows() >= 1) {
					ResultSet resultSet = ar.result();
					List<JsonArray> results = resultSet.getResults();
					long tt = 0;
					for (JsonArray next : results) {
						tt = next.getLong(0);
					}
					// jedis.set(min + "-" + max, stringBuilder.toString());
					resultHandler.handle(Future.succeededFuture(tt + ""));
				} else {
					resultHandler.handle(Future.succeededFuture(""));
				}
			}
		});
	}

	private void q5_2(String user_max, SQLConnection connection, Handler<AsyncResult<String>> resultHandler) {

		connection.queryWithParams(Q5_2, new JsonArray().add(user_max), ar -> {
			if (ar.failed()) {
				resultHandler.handle(Future.failedFuture("Row not found"));
			} else {
				if (ar.result().getNumRows() >= 1) {
					ResultSet resultSet = ar.result();
					List<JsonArray> results = resultSet.getResults();
					long tt = 0;
					for (JsonArray next : results) {
						tt = next.getLong(0);
					}
					// jedis.set(min + "-" + max, stringBuilder.toString());
					resultHandler.handle(Future.succeededFuture(tt + ""));
				} else {
					resultHandler.handle(Future.succeededFuture(""));
				}
			}
		});
	}

	public static Long dateStringToTs(String date) {
		// 2014-05-31+01:29:04
		try {
			return FULL_DATE_FORMAT.parse(date).getTime();
		} catch (ParseException e) {
			System.out.println("PArse Exception for " + date);
		}
		return null;
	}

	public static Long dateStringToTsDayLevel(String date) {
		// 2014-05-31
		try {
			return DATE_FORMAT.parse(date).getTime();
		} catch (ParseException e) {
			System.out.println("Parse Exception for " + date);
		}
		return null;
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

	public static String getNextDay(String date) {
		// 2014-05-31
		try {
			return DATE_FORMAT.format(new Date(DATE_FORMAT.parse(date).getTime() + ONE_DAY_MSEC));
		} catch (ParseException e) {
			System.out.println("Parse Exception for " + date);
		}
		return date;
	}

}

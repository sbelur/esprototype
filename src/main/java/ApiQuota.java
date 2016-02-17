import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by sbelur on 15/02/16.
 */
public class ApiQuota {

  public enum Policy {
    ALLOW_VIOLATION,
    DISCARD_REQUEST
  }

  private Policy policy;

  private int allowed = Integer.parseInt(System.getProperty("quota.allowed", "3"));

  private static final AtomicBoolean initiated = new AtomicBoolean(false);

  public ApiQuota(Policy policy) {
    this.policy = policy;
  }

  public static void init() {
    if (initiated.compareAndSet(false, true)) {
      boolean aggregate = Boolean.parseBoolean(System.getProperty("runaggreation", "false"));
      if (aggregate) {
        log.info("Running aggregation");
        scheduleGlobalStatAggregation();
      } else {
        log.info("Not Running aggregation");
      }
      syncGlobalStats();
    }
  }

  private static Log log = LogFactory.getLog(ApiQuota.class);

  private static final ScheduledExecutorService globalAggregator = Executors.newScheduledThreadPool(1);

  private static final AtomicBoolean globalAggScheduled = new AtomicBoolean(false);

  private static final ScheduledExecutorService globalSyncer = Executors.newScheduledThreadPool(1);

  private static final AtomicBoolean globalSyncScheduled = new AtomicBoolean(false);

  private static final ESQuery esQuery = new ESQuery();

  // For now only AS2 - ideally should be a map / protocol
  private static AtomicInteger globalCount = new AtomicInteger(0);

  //date truncated at minute to local stats in that minute
  private static Map<String, AtomicInteger> localCount = new HashMap<>();

  // Call it before API call.
  public boolean isInViolation() {
    int next = getCurrentTotalInWindow() + 1;
    log.info("next will be " + next + " allowed is " + allowed);
    return next > allowed;
  }

  public Optional<QuotaViolatedException> onViolation() {
    if (policy == Policy.ALLOW_VIOLATION) {
      log.warn("Allowing quota violation - Total no of requests " + getCurrentTotalInWindow()
          + " , whereas configured no is " + allowed + ". time now is " + Calendar
          .getInstance(TimeZone.getTimeZone("GMT")).getTime() + "...wait till start of 5 min boundary.");
      return Optional.empty();
    } else {
      return Optional.of(new QuotaViolatedException());
    }
  }

  public void onNormalFlow() {
    AtomicInteger local = getLocalForCurrentMin();
    local.incrementAndGet();
    log.info("Local map" + localCount);
  }

  private AtomicInteger getLocalForCurrentMin() {
    Calendar now = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
    now.set(Calendar.SECOND, 0);
    now.set(Calendar.MILLISECOND, 0);
    String key = now.getTime().toGMTString();
    localCount.putIfAbsent(key, new AtomicInteger(0));
    return localCount.get(key);
  }

  private static void scheduleGlobalStatAggregation() {
    if (globalAggScheduled.compareAndSet(false, true)) {
      globalAggregator.scheduleAtFixedRate(() -> {
        log.info("Aggregating");
        esQuery.searchResultWithAggregation();
      }, 1, 1, TimeUnit.MINUTES);
    }
  }

  private static void syncGlobalStats() {
    if (globalSyncScheduled.compareAndSet(false, true)) {
      globalSyncer.scheduleAtFixedRate(() -> {
        log.info("Syncing");
        ESQuery.SumHolder holder = esQuery.getGlobalStats();
        globalCount.set(holder.getTotal());
        log.info("Gloabal ->" + globalCount.get() + " at time " + Calendar.getInstance(TimeZone.getTimeZone("GMT"))
            .getTime());
      }, 5, 30, TimeUnit.SECONDS);
    }
  }

  private int getCurrentTotalInWindow() {
    AtomicInteger local = null;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    SimpleDateFormat localsdf = new SimpleDateFormat("\"d MMM yyyy HH:mm:ss 'GMT'\"");
    if (esQuery.lastAggTime.isPresent()) {

      try {
        Date last = sdf.parse(esQuery.lastAggTime.get());
        AtomicInteger allLocal = new AtomicInteger(0);
        localCount.forEach((k, v) -> {

          try {
            Date aLocalDt = localsdf.parse(k);
            if(aLocalDt.after(last) && aLocalDt.equals(last)){
              allLocal.addAndGet(v.get());
            }

          } catch (ParseException e) {
            throw new RuntimeException(e);
          }
        });
        local = allLocal;
      }
      catch (Exception e){
        e.printStackTrace();
        local = getLocalForCurrentMin();
      }
    } else {
      local = getLocalForCurrentMin();
    }
    log.info("Current local,global " + local.get() + " , " + globalCount.get() + " at time " + Calendar
        .getInstance(TimeZone.getTimeZone("GMT")).getTime());
    return local.get() + globalCount.get();
  }

}

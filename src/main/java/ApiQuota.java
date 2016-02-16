import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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

  public void init() {
    if (initiated.compareAndSet(false, true)) {
      scheduleGlobalStatAggregation();
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
  private AtomicInteger globalCount = new AtomicInteger(0);

  //date truncated at minute to local stats in that minute
  private Map<String, AtomicInteger> localCount = new HashMap<>();

  // Call it before API call.
  public boolean isInViolation() {
    int next = getCurrentTotalInWindow() + 1;
    log.info("next will be "+next + " allowed is "+allowed);
    return next > allowed;
  }

  public Optional<QuotaViolatedException> onViolation() {
    if (policy == Policy.ALLOW_VIOLATION) {
      log.warn("Allowing quota violation - Total no of requests " + getCurrentTotalInWindow()
          + " , whereas configured no is " + allowed + ". time now is "+Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTime() + "...wait till start of 5 min boundary.");
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

  private void scheduleGlobalStatAggregation() {
    if (globalAggScheduled.compareAndSet(false, true)) {
      globalAggregator.scheduleAtFixedRate(() -> {
        log.info("Aggregating");
        esQuery.searchResultWithAggregation();
      }, 1, 1, TimeUnit.MINUTES);
    }
  }

  private void syncGlobalStats() {
    if (globalSyncScheduled.compareAndSet(false, true)) {
      globalSyncer.scheduleAtFixedRate(() -> {
        log.info("Syncing");
        ESQuery.SumHolder holder = esQuery.getGlobalStats();
        globalCount.set(holder.getTotal());
        log.info("Gloabal ->" + globalCount.get() + " at time "+Calendar
            .getInstance(TimeZone.getTimeZone("GMT")).getTime());
      }, 5, 30, TimeUnit.SECONDS);
    }
  }

  private int getCurrentTotalInWindow() {
    AtomicInteger local = getLocalForCurrentMin();
    log.info("Current local,global " + local.get() + " , " + globalCount.get() + " at time " + Calendar
        .getInstance(TimeZone.getTimeZone("GMT")).getTime());
    return local.get() + globalCount.get();
  }

}

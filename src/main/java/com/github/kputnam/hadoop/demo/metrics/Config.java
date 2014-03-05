package com.github.kputnam.hadoop.demo.metrics;

import java.util.concurrent.TimeUnit;

/**
* Created by kputnam on 3/4/14.
*/
public class Config {
    private String configPath = null;
    private String metricPrefix = null;

    private int graphitePort = -1;
    private String graphiteHost = null;

    private int reportInterval = -1;
    private TimeUnit rateUnits = null;
    private TimeUnit durationUnits = null;

    public String getConfigPath() {
        return configPath;
    }

    public void setConfigPath(String configPath) {
        this.configPath = configPath;
    }

    public int getGraphitePort() {
        return graphitePort;
    }

    public void setGraphitePort(int graphitePort) {
        this.graphitePort = graphitePort;
    }

    public String getGraphiteHost() {
        return graphiteHost;
    }

    public void setGraphiteHost(String graphiteHost) {
        this.graphiteHost = graphiteHost;
    }

    public String getMetricPrefix() {
        return metricPrefix;
    }

    public void setMetricPrefix(String metricPrefix) {
        this.metricPrefix = metricPrefix;
    }

    public int getReportInterval() {
        return reportInterval;
    }

    public void setReportInterval(int reportInterval) {
        this.reportInterval = reportInterval;
    }

    public TimeUnit getRateUnits() {
        if (rateUnits == null)
            return TimeUnit.MILLISECONDS;

        return rateUnits;
    }

    public void setRateUnits(TimeUnit rateUnits) {
        this.rateUnits = rateUnits;
    }

    public TimeUnit getDurationUnits() {
        if (durationUnits == null)
            return TimeUnit.MILLISECONDS;

        return durationUnits;
    }

    public void setDurationUnits(TimeUnit durationUnits) {
        this.durationUnits = durationUnits;
    }

    public Config mergeInto(Config that) {
        if (this.configPath != null)
            that.setConfigPath(this.configPath);

        if (this.metricPrefix != null)
            that.setMetricPrefix(this.metricPrefix);

        if (this.graphitePort != -1)
            that.setGraphitePort(this.graphitePort);

        if (this.graphiteHost != null)
            that.setGraphiteHost(this.graphiteHost);

        if (this.reportInterval != -1)
            that.setReportInterval(this.reportInterval);

        if (this.rateUnits != null)
            that.setRateUnits(this.rateUnits);

        if (this.durationUnits != null)
            that.setDurationUnits(this.durationUnits);

        return that;
    }

    public Config loadFrom(Config that) {
        return that.mergeInto(this);
    }
}

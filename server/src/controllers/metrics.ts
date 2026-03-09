import { Request, Response } from 'express';

import { IngestData } from '../models/IngestData';
import { IngestResponse } from '../models/IngestResponse';
import {
  BaseMetric,
  BloodPressureMetric,
  BloodPressureModel,
  HeartRateMetric,
  HeartRateModel,
  Metric,
  SleepMetric,
  SleepModel,
  mapMetric,
  createMetricModel,
} from '../models/Metric';
import { MetricName } from '../models/MetricName';
import { filterFields, parseDate } from '../utils';

type MetricBucket = 'day' | 'week' | 'month';
type MetricRollup = 'sum' | 'avg' | 'min' | 'max';
type MetricTrend = 'rolling_avg';

interface AggregatedMetricPoint {
  date: Date;
  value: number;
  units: string;
}

const AGGREGATION_UNSUPPORTED_METRICS = new Set<MetricName>([
  MetricName.BLOOD_PRESSURE,
  MetricName.HEART_RATE,
  MetricName.SLEEP_ANALYSIS,
]);

const ADDITIVE_ROLLUP_METRICS = new Set<MetricName>([
  MetricName.ACTIVE_ENERGY,
  MetricName.RESTING_ENERGY,
  MetricName.APPLE_EXERCISE_TIME,
  MetricName.APPLE_MOVE_TIME,
  MetricName.APPLE_STAND_TIME,
  MetricName.CYCLING_DISTANCE,
  MetricName.SWIMMING_DISTANCE,
  MetricName.WALKING_RUNNING_DISTANCE,
  MetricName.WHEELCHAIR_DISTANCE,
  MetricName.FLIGHTS_CLIMBED,
  MetricName.WHEELCHAIR_PUSH_COUNT,
  MetricName.SWIM_STROKE_COUNT,
  MetricName.STEP_COUNT,
  MetricName.CYCLING_CADENCE,
  MetricName.CYCLING_POWER,
  MetricName.CYCLING_SPEED,
  MetricName.RUNNING_POWER,
  MetricName.RUNNING_SPEED,
  MetricName.DIETARY_ENERGY,
  MetricName.DIETARY_WATER,
]);

const VALID_BUCKETS = new Set<MetricBucket>(['day', 'week', 'month']);
const VALID_ROLLUPS = new Set<MetricRollup>(['sum', 'avg', 'min', 'max']);
const VALID_TRENDS = new Set<MetricTrend>(['rolling_avg']);

const isBaseMetric = (metric: Metric): metric is BaseMetric => 'qty' in metric;

const requiresAggregation = (query: Request['query']) =>
  query.aggregate != null ||
  query.bucket != null ||
  query.rollup != null ||
  query.trend != null ||
  query.window != null;

const getBucketDate = (input: Date, bucket: MetricBucket) => {
  const date = new Date(input);

  switch (bucket) {
    case 'day':
      return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate(), 12));
    case 'week': {
      const day = date.getUTCDay();
      const diffToMonday = (day + 6) % 7;
      return new Date(
        Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate() - diffToMonday, 12),
      );
    }
    case 'month':
      return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1, 12));
  }
};

const getRollingWindowStart = (date: Date, window: number) =>
  new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate() - (window - 1), 12));

const rollupValues = (values: number[], rollup: MetricRollup) => {
  switch (rollup) {
    case 'sum':
      return values.reduce((sum, value) => sum + value, 0);
    case 'avg':
      return values.reduce((sum, value) => sum + value, 0) / values.length;
    case 'min':
      return Math.min(...values);
    case 'max':
      return Math.max(...values);
  }
};

const aggregateBaseMetrics = (
  metrics: BaseMetric[],
  options: {
    bucket: MetricBucket;
    rollup: MetricRollup;
    trend?: MetricTrend;
    window?: number;
  },
): AggregatedMetricPoint[] => {
  const buckets = new Map<string, AggregatedMetricPoint & { values: number[] }>();

  for (const metric of metrics) {
    const bucketDate = getBucketDate(new Date(metric.date), options.bucket);
    const key = bucketDate.toISOString();
    const bucket = buckets.get(key);

    if (bucket) {
      bucket.values.push(metric.qty);
      continue;
    }

    buckets.set(key, {
      date: bucketDate,
      value: 0,
      units: metric.units,
      values: [metric.qty],
    });
  }

  const aggregated = Array.from(buckets.values())
    .map(({ date, units, values }) => ({
      date,
      value: rollupValues(values, options.rollup),
      units,
    }))
    .sort((left, right) => left.date.getTime() - right.date.getTime());

  if (options.trend !== 'rolling_avg' || !options.window) {
    return aggregated;
  }

  return aggregated.map((point, index, points) => {
    const windowStart = getRollingWindowStart(point.date, options.window!);
    const windowPoints = points.filter(
      (candidate) =>
        candidate.date.getTime() >= windowStart.getTime() &&
        candidate.date.getTime() <= point.date.getTime(),
    );

    return {
      date: point.date,
      value: rollupValues(
        windowPoints.map((candidate) => candidate.value),
        'avg',
      ),
      units: point.units,
    };
  });
};

const parseBucketParam = (req: Request) => {
  const rawBucket = (req.query.bucket ?? req.query.aggregate) as string | undefined;

  if (!rawBucket) {
    return null;
  }

  if (!VALID_BUCKETS.has(rawBucket as MetricBucket)) {
    throw new Error(`Invalid bucket: ${rawBucket}`);
  }

  return rawBucket as MetricBucket;
};

const parseRollupParam = (rawRollup: string | undefined, selectedMetric: MetricName) => {
  if (!rawRollup) {
    return ADDITIVE_ROLLUP_METRICS.has(selectedMetric) ? 'sum' : 'avg';
  }

  if (!VALID_ROLLUPS.has(rawRollup as MetricRollup)) {
    throw new Error(`Invalid rollup: ${rawRollup}`);
  }

  return rawRollup as MetricRollup;
};

const parseTrendParam = (rawTrend: string | undefined) => {
  if (!rawTrend) {
    return null;
  }

  if (!VALID_TRENDS.has(rawTrend as MetricTrend)) {
    throw new Error(`Invalid trend: ${rawTrend}`);
  }

  return rawTrend as MetricTrend;
};

const parseWindowParam = (rawWindow: string | undefined) => {
  if (!rawWindow) {
    return null;
  }

  const window = Number(rawWindow);

  if (!Number.isInteger(window) || window < 2 || window > 90) {
    throw new Error('Window must be an integer between 2 and 90');
  }

  return window;
};

export const getMetrics = async (req: Request, res: Response) => {
  try {
    const { from, to, include, exclude, rollup, trend, window } = req.query;
    const selectedMetric = req.params.selected_metric as MetricName;

    if (!selectedMetric) {
      throw new Error('No metric selected');
    }

    const fromDate = parseDate(from as string);
    const toDate = parseDate(to as string);

    let query = {};

    if (fromDate && toDate) {
      query = {
        date: {
          $gte: fromDate,
          $lte: toDate,
        },
      };
    }

    let metrics;
    const wantsAggregation = requiresAggregation(req.query);

    switch (selectedMetric) {
      case MetricName.BLOOD_PRESSURE:
        metrics = await BloodPressureModel.find(query).lean();
        break;
      case MetricName.HEART_RATE:
        metrics = await HeartRateModel.find(query).lean();
        break;
      case MetricName.SLEEP_ANALYSIS:
        metrics = await SleepModel.find(query).lean();
        break;
      default:
        metrics = await createMetricModel(selectedMetric).find(query).sort({ date: 1 }).lean();
    }

    if (wantsAggregation) {
      if (AGGREGATION_UNSUPPORTED_METRICS.has(selectedMetric)) {
        res.status(400).json({
          error: `Aggregation is not supported for ${selectedMetric}`,
        });
        return;
      }

      const bucket = parseBucketParam(req) ?? 'day';
      const parsedTrend = parseTrendParam(trend as string | undefined);
      const parsedWindow = parseWindowParam(window as string | undefined);

      if (parsedTrend && bucket !== 'day') {
        res.status(400).json({
          error: 'rolling_avg is only supported with bucket=day',
        });
        return;
      }

      if (parsedTrend && !parsedWindow) {
        res.status(400).json({
          error: 'window is required when trend=rolling_avg',
        });
        return;
      }

      const parsedRollup = parseRollupParam(rollup as string | undefined, selectedMetric);
      const baseMetrics = (metrics as Metric[]).filter((metric): metric is BaseMetric =>
        isBaseMetric(metric),
      );

      metrics = aggregateBaseMetrics(baseMetrics, {
        bucket,
        rollup: parsedRollup,
        trend: parsedTrend ?? undefined,
        window: parsedWindow ?? undefined,
      });
    }

    // Process include/exclude filters if provided
    if (include || exclude) {
      metrics = metrics.map((metric) => filterFields(metric, include, exclude));
    }

    console.log(metrics);
    res.json(metrics);
  } catch (error) {
    console.error('Error getting metrics:', error);
    res.status(400).json({ error: error instanceof Error ? error.message : 'Error getting metrics' });
  }
};

export const saveMetrics = async (ingestData: IngestData): Promise<IngestResponse> => {
  try {
    const response: IngestResponse = {};
    const metricsData = ingestData.data.metrics;

    if (!metricsData || metricsData.length === 0) {
      response.metrics = {
        success: true,
        error: 'No metrics data provided',
      };
      return response;
    }

    // Group metrics by type and map the data
    const metricsByType = metricsData.reduce(
      (acc, metric) => {
        const mappedMetrics = mapMetric(metric);
        const key = metric.name;
        acc[key] = acc[key] || [];
        acc[key].push(...mappedMetrics);
        return acc;
      },
      {} as {
        [key: string]: Metric[];
      },
    );

    const saveOperations = Object.entries(metricsByType).map(([key, metrics]) => {
      switch (key as MetricName) {
        case MetricName.BLOOD_PRESSURE:
          const bpMetrics = metrics as BloodPressureMetric[];
          return BloodPressureModel.bulkWrite(
            bpMetrics.map((metric) => ({
              updateOne: {
                filter: { source: metric.source, date: metric.date },
                update: { $set: metric },
                upsert: true,
              },
            })),
          );
        case MetricName.HEART_RATE:
          const hrMetrics = metrics as HeartRateMetric[];
          return HeartRateModel.bulkWrite(
            hrMetrics.map((metric) => ({
              updateOne: {
                filter: { source: metric.source, date: metric.date },
                update: { $set: metric },
                upsert: true,
              },
            })),
          );
        case MetricName.SLEEP_ANALYSIS:
          const sleepMetrics = metrics as SleepMetric[];
          return SleepModel.bulkWrite(
            sleepMetrics.map((metric) => ({
              updateOne: {
                filter: { source: metric.source, date: metric.date },
                update: { $set: metric },
                upsert: true,
              },
            })),
          );
        default:
          const baseMetrics = metrics as BaseMetric[];
          const model = createMetricModel(key as MetricName);
          return model.bulkWrite(
            baseMetrics.map((metric) => ({
              updateOne: {
                filter: { source: metric.source, date: metric.date },
                update: { $set: metric },
                upsert: true,
              },
            })),
          );
      }
    });

    await Promise.all(saveOperations);

    response.metrics = {
      success: true,
      message: `${metricsData.length} metrics saved successfully`,
    };

    return response;
  } catch (error) {
    console.error('Error saving metrics:', error);

    const errorResponse: IngestResponse = {};
    errorResponse.metrics = {
      success: false,
      error: error instanceof Error ? error.message : 'Error saving metrics',
    };

    return errorResponse;
  }
};

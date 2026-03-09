import { Request, Response } from 'express';

import { IngestData } from '../models/IngestData';
import { IngestResponse } from '../models/IngestResponse';
import { RouteModel, WorkoutModel, mapWorkoutData, mapRoute } from '../models/Workout';
import { filterFields, parseDate } from '../utils';

interface WorkoutSeriesPoint {
  timestamp: string;
  value: number;
}

interface WorkoutRoutePoint {
  latitude: number;
  longitude: number;
  time: string;
}

interface TimedQuantityEntry {
  date: Date;
  qty: number;
}

const roundTo = (value: number, decimals = 2) => {
  const factor = 10 ** decimals;
  return Math.round(value * factor) / factor;
};

const averageOf = (values: number[]) => {
  if (!values.length) {
    return null;
  }

  return roundTo(values.reduce((sum, value) => sum + value, 0) / values.length);
};

const maxOf = (values: number[]) => {
  if (!values.length) {
    return null;
  }

  return Math.max(...values);
};

const sumOf = (values: number[]) => {
  if (!values.length) {
    return null;
  }

  return roundTo(values.reduce((sum, value) => sum + value, 0));
};

const toIsoStringOrNull = (value: Date | string | number | undefined | null) => {
  if (value == null) {
    return null;
  }

  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    return null;
  }

  return date.toISOString();
};

const mapHeartRateSeries = (
  heartRateEntries:
    | {
        date: Date;
        Avg: number;
      }[]
    | undefined,
): WorkoutSeriesPoint[] =>
  heartRateEntries?.flatMap((entry) => {
    const timestamp = toIsoStringOrNull(entry.date);

    if (!timestamp) {
      return [];
    }

    return {
      timestamp,
      value: entry.Avg,
    };
  }) || [];

const mapQuantitySeries = (
  quantityEntries:
    | TimedQuantityEntry[]
    | undefined,
): WorkoutSeriesPoint[] =>
  quantityEntries?.flatMap((entry) => {
    const timestamp = toIsoStringOrNull(entry.date);

    if (!timestamp) {
      return [];
    }

    return {
      timestamp,
      value: entry.qty,
    };
  }) || [];

const mapSingleQuantitySeries = (entry: TimedQuantityEntry | undefined): WorkoutSeriesPoint[] => {
  if (entry?.qty == null) {
    return [];
  }

  const timestamp = toIsoStringOrNull(entry.date);

  if (!timestamp) {
    return [];
  }

  return [
    {
      timestamp,
      value: entry.qty,
    },
  ];
};

export const getWorkouts = async (req: Request, res: Response) => {
  try {
    const { startDate, endDate, include, exclude } = req.query;

    const fromDate = parseDate(startDate as string);
    const toDate = parseDate(endDate as string);

    console.log(fromDate, toDate);

    let query = {};

    if (fromDate && toDate) {
      query = {
        start: {
          $gte: fromDate,
          $lte: toDate,
        },
      };
    }

    const workouts = await WorkoutModel.find(query)
      .sort({ start: -1 })
      .lean()
      .then((workouts) => {
        const mappedWorkouts = workouts.map((workout) => {
          const startDate = new Date(workout.start);
          const endDate = new Date(workout.end);

          const result = {
            id: workout.workoutId,
            workout_type: workout.name,
            start_time: startDate.toISOString(),
            end_time: endDate.toISOString(),
            start_ms: startDate.getTime(),
            end_ms: endDate.getTime(),
            duration_minutes: workout.duration / 60,
            calories_burned: workout.activeEnergyBurned?.qty || null,
          };

          return result;
        });

        return mappedWorkouts;
      });

    // Process include/exclude filters if provided
    let processedWorkouts = workouts;
    if (include || exclude) {
      processedWorkouts = workouts.map(workout => filterFields(workout, include, exclude));
    }

    console.log(`${workouts.length} workouts fetched`);
    res.status(200).json(processedWorkouts);
  } catch (error) {
    console.error('Error fetching workouts:', error);
    res.status(500).json({ error: 'Error fetching workouts' });
  }
};

export const getWorkout = async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const { include, exclude } = req.query;

    console.log('Fetching workout details for ID:', id);
    const workout = await WorkoutModel.findOne({ workoutId: id }).lean();

    if (!workout) {
      return res.status(404).json({ error: 'Workout not found' });
    }

    const heartRateData = mapHeartRateSeries(workout.heartRateData);
    const heartRateRecovery = mapHeartRateSeries(workout.heartRateRecovery);
    const stepCount = mapQuantitySeries(workout.stepCount);
    const route = await RouteModel.findOne({ workoutId: id })
      .lean()
      .then((route) => {
        return route?.locations.flatMap((x) => {
          const time = toIsoStringOrNull(x.timestamp);

          if (!time) {
            return [];
          }

          return {
            latitude: x.latitude,
            longitude: x.longitude,
            time,
          };
        });
      });

    const routePoints: WorkoutRoutePoint[] = route || [];
    const distanceQty = workout.distance?.qty ?? null;
    const distanceUnits = workout.distance?.units ?? null;
    const activeEnergyQty = workout.activeEnergyBurned?.qty ?? workout.activeEnergy?.qty ?? null;
    const activeEnergyUnits = workout.activeEnergyBurned?.units ?? workout.activeEnergy?.units ?? null;
    const stepValues = stepCount.map((entry) => entry.value);
    const heartRateValues = heartRateData.map((entry) => entry.value);
    const heartRateRecoveryValues = heartRateRecovery.map((entry) => entry.value);
    const averagePace =
      distanceQty && distanceQty > 0
        ? roundTo((workout.duration / 60) / distanceQty)
        : null;

    let ret = {
      id: workout.workoutId,
      workout_type: workout.name,
      start_time: new Date(workout.start).toISOString(),
      end_time: new Date(workout.end).toISOString(),
      duration_minutes: roundTo(workout.duration / 60),
      distance: distanceQty,
      distance_units: distanceUnits,
      active_energy: activeEnergyQty,
      active_energy_units: activeEnergyUnits,
      summary: {
        distance: distanceQty,
        distance_units: distanceUnits,
        active_energy: activeEnergyQty,
        active_energy_units: activeEnergyUnits,
        total_steps: sumOf(stepValues),
        avg_heart_rate: averageOf(heartRateValues),
        max_heart_rate: maxOf(heartRateValues),
        avg_heart_rate_recovery: averageOf(heartRateRecoveryValues),
        max_heart_rate_recovery: maxOf(heartRateRecoveryValues),
        avg_pace_minutes_per_unit: averagePace,
        route_points: routePoints.length,
        temperature: workout.temperature?.qty ?? null,
        temperature_units: workout.temperature?.units ?? null,
        humidity: workout.humidity?.qty ?? null,
        humidity_units: workout.humidity?.units ?? null,
        intensity: workout.intensity?.qty ?? null,
        intensity_units: workout.intensity?.units ?? null,
      },
      series: {
        heart_rate: heartRateData,
        heart_rate_recovery: heartRateRecovery,
        step_count: stepCount,
        temperature: mapSingleQuantitySeries(workout.temperature),
        humidity: mapSingleQuantitySeries(workout.humidity),
        intensity: mapSingleQuantitySeries(workout.intensity),
      },
      heartRateData,
      heartRateRecovery,
      stepCount,
      route: routePoints,
    };

    // Process include/exclude filters if provided
    if (include || exclude) {
      ret = filterFields(ret, include, exclude);
    }

    console.log(`Workout ${id} fetched with ${route?.length ?? 0} locations`);
    res.status(200).json(ret);
  } catch (error) {
    console.error('Error fetching workout details:', error);
    res.status(500).json({ error: 'Error fetching workout details' });
  }
};

export const saveWorkouts = async (ingestData: IngestData): Promise<IngestResponse> => {
  try {
    const response: IngestResponse = {};
    const workouts = ingestData.data.workouts;

    if (!workouts || !workouts.length) {
      response.workouts = {
        success: true,
        message: 'No workout data provided',
      };
      return response;
    }

    const workoutOperations = workouts.map((workout) => {
      return {
        updateOne: {
          filter: { workoutId: workout.id },
          update: {
            $set: mapWorkoutData(workout),
          },
          upsert: true,
        },
      };
    });

    const routeOperations = workouts
      .filter((workout) => workout.route && workout.route.length > 0)
      .map(mapRoute)
      .map((route) => ({
        updateOne: {
          filter: { workoutId: route.workoutId },
          update: {
            $set: route,
          },
          upsert: true,
        },
      }));

    await Promise.all([
      WorkoutModel.bulkWrite(workoutOperations),
      routeOperations.length > 0 ? RouteModel.bulkWrite(routeOperations) : Promise.resolve(),
    ]);

    response.workouts = {
      success: true,
      message: `${workoutOperations.length} Workouts and ${routeOperations.length} Routes saved successfully`,
    };

    console.debug(`Processed ${workouts.length} workouts`);

    return response;
  } catch (error) {
    console.error('Error processing workouts:', error);

    const errorResponse: IngestResponse = {};
    errorResponse.workouts = {
      success: false,
      message: 'Workouts not saved',
      error: error instanceof Error ? error.message : 'An error occurred',
    };

    return errorResponse;
  }
};

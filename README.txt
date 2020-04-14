For Graders:

Once the tar file has been extracted, inside the root directory I used the command "gradlew clean build" to build the project. The built jar can be found in the folder build/libs/ and it will be called AirQuality-1.0-SNAPSHOT.jar
This jar can now be used for all execution/grading purposes.


Package Descriptions:

1) cs455.hadoop.mappers
This package consists of the 6 mapper classes for the 6 jobs that we have to execute in order to analyze the dataset.

	Class Descriptions in cs455.hadoop.mappers
	1a) cs455.hadoop.jobs.MaxSitesMapper
		The mapper class for finding the state with maximum number of sites.

	1b)  cs455.hadoop.jobs.MeanCoastSO2Mapper
		The mapper class for finding the coast with the with the higher mean SO2 level.

	1c)  cs455.hadoop.jobs.MaxHourSO2Mapper
		The mapper class for finding the time of day with highest mean SO2 levels.

	1d)  cs455.hadoop.jobs.YearSO2Mapper
		The mapper class for finding the mean SO2 levels per year to observe a trend in the level.

	1e)  cs455.hadoop.jobs.Top10HottestMapper
		The mapper class for finding the top 10 hottest states for the months June, July and August.

	1f)  cs455.hadoop.jobs.MeanSO2HottestMapper
		The mapper class for finding the mean yearly SO2 levels for the hottest states found by executing job 1e mentioned above.


2) cs455.hadoop.reducers
This package consists of the 6 reducer classes for the 6 jobs that we have to execute in order to analyze the dataset.

	Class Descriptions in cs455.hadoop.reducers
	2a) cs455.hadoop.jobs.MaxSitesReducer
		The reducer class for finding the state with maximum number of sites.

	2b)  cs455.hadoop.jobs.MeanCoastSO2Reducer
		The reducer class for finding the coast with the with the higher mean SO2 level.

	2c)  cs455.hadoop.jobs.MaxHourSO2Reducer
		The reducer class for finding the time of day with highest mean SO2 levels.

	2d)  cs455.hadoop.jobs.YearSO2Reducer
		The reducer class for finding the mean SO2 levels per year to observe a trend in the level.

	2e)  cs455.hadoop.jobs.Top10HottestReducer
		The reducer class for finding the top 10 hottest states for the months June, July and August.

	2f)  cs455.hadoop.jobs.MeanSO2HottestReducer
		The reducer class for finding the mean yearly SO2 levels for the hottest states found by executing job 2e mentioned above.


3) cs455.hadoop.jobs
This package consists of the 6 job handler classes for the 6 jobs that we have to execute in order to analyze the dataset.

	Class Descriptions in cs455.hadoop.jobs
	3a) cs455.hadoop.jobs.MaxSitesJob
		The job controller class for executing the task for finding the state with the maximum number of sites.

	3b)  cs455.hadoop.jobs.MeanCoastSO2Job
		The job controller class for executing the task for finding the coast with the with the higher mean SO2 level.

	3c)  cs455.hadoop.jobs.MaxHourSO2Job
		The job controller class for executing the task for finding the time of day with highest mean SO2 levels.

	3d)  cs455.hadoop.jobs.YearSO2Trend
		The job controller class for executing the task for finding the mean SO2 levels per year to observe a trend in the level.

	3e)  cs455.hadoop.jobs.Top10HottestJob
		The job controller class for executing the task for finding the top 10 hottest states for the months June, July and August.

	3f)  cs455.hadoop.jobs.MeanSO2HottestJob
		The job controller class for executing the task for finding the mean yearly SO2 levels for the hottest states found by executing job 3e mentioned above.


4) cs455.hadoop.helpers
This package consists of a lone class that is used as a wrapper over state and temperature (to enforce comparable) in order to find the hottest state

	Class Descriptions in cs455.hadoop.helpers
	4a) cs455.hadoop.helpers.StateTemp
		Forms a wrapper over a state name and it's mean temperature and ensures ordering over objects of this instance, facilitating easy usage via the Collections framework.

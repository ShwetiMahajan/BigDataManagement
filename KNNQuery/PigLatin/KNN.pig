point_data = load ‘Path_Of_pointsFile_in_HDFS’ USING PigStorage(',') AS (id:long ,x:double, y:double);
distances_from_query = FOREACH point_data GENERATE id,x,y, SQRT((((double)’#x_input’-x)*((double)’#x_input’-x))+(((double)’#y_input’-y)*((double)'y_input'-y))) as dist;
sorted_distances = ORDER distances_from_query BY dist;
kNearestNeighbors = limit sorted_distances (int)’#k_input’;
STORE kNearestNeighbors INTO '$/users/shwetimahajan/outputPig';


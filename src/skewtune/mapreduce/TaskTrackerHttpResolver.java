package skewtune.mapreduce;

public interface TaskTrackerHttpResolver {
    /**
     * @param tracker tracker host name
     * @param path    path in URI
     * @return URI construct from tracker and path information with appropriate port number
     */
    String getTaskTrackerURI(String tracker, String path);
}

package org.apache.flink.common.utils;

/**
 * @author Elias (siran0611@gmail.com)
 */
public class MissingSolutionException extends Exception{
    public MissingSolutionException() {
    }

    public static boolean ultimateCauseIsMissingSolution(Throwable e) {
        while (e != null) {
            if (e instanceof MissingSolutionException) {
             return true;
            }else {
                e = e.getCause();
            }
        }
        return false;
    }
}

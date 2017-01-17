package uom.msc.debs.event;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

public interface EventInfo {

    long DATA_START_TIME_PS = 10629342490369879L;
    long GAME_START_TIME_PS = 10753295594424116L;
    long FIRST_HALF_END_TIME_PS = 12557295594424116L;
    long SECOND_HALF_START_TIME_PS = 13086639146403495L;
    long GAME_END_TIME_PS = 14879639146403495L;
    long DATA_END_TIME_PS = 14893948418670216L;

    DecimalFormat DECIMAL_FORMAT = new DecimalFormat("###.##");
    SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

}

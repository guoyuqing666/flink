package flink;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import flink.dto.Behavior;

public class BehaviorCountWinFunc extends ProcessAllWindowFunction<Behavior, Object [], TimeWindow> {
    @Override
    public void process(Context ctx, Iterable<Behavior> iterable,
                        Collector<Object []> out) {
        int buy = 0;
        int fav = 0;
        int cart = 0;
        int pv = 0;
        for (Behavior be : iterable) {
            switch (be.getBehavior()) {
                case "buy":
                    ++buy;
                    break;
                case "fav":
                    ++fav;
                    break;
                case "cart":
                    ++cart;
                    break;
                case "pv":
                    ++pv;
                default:
                    break;
            }
        }
        Object statis[] = {buy, fav, cart, pv, ctx.window().getEnd()};
        //System.out.println("buy: "+statis[0].toString()+"fav: "+statis[1].toString()+"cart: "+statis[2].toString()+"pv: "+statis[3].toString()+"time: "+statis[4].toString());
        out.collect(statis);
    }
}
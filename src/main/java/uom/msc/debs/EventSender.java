package uom.msc.debs;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;

public class EventSender implements Runnable {
    private BlockingQueue<Event> queue = new LinkedBlockingQueue<Event>(200000);
    private InputHandler inputHandler;
    
    public EventSender(InputHandler inputHandler) {
        this.inputHandler = inputHandler;
    }

    public void run() {
        while (true) {
            try {
                Event event = queue.take();
                inputHandler.send(event);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        
    }
    
    public BlockingQueue getQueue() {
        return queue;
    }
}
